import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type StreamSkopeBackend,
} from "../../src/kafka/contracts";
import { createKafkaBackend, createBrowserKafkaProfileStore } from "../../src/main";
import { trustRecipeInput } from "../support/trust-recipe";
import { startDevelopmentHost, type RunningDevelopmentHost } from "../../src/platform/dev-host";

const rendererOrigin = "http://127.0.0.1:4173";
const token = "0123456789abcdef0123456789abcdef";
const runningHosts: RunningDevelopmentHost[] = [];

afterEach(async () => {
  for (const host of runningHosts.splice(0).reverse()) {
    await host.close();
  }
});

function headers(): Record<string, string> {
  return {
    "content-type": "application/json",
    origin: rendererOrigin,
    "x-streamskope-token": token,
  };
}

async function execute(
  host: RunningDevelopmentHost,
  command: HostCommand | Readonly<Record<string, unknown>>,
): Promise<{ readonly body: unknown; readonly status: number }> {
  const response = await fetch(`${host.origin}/commands`, {
    body: JSON.stringify(command),
    headers: headers(),
    method: "POST",
  });
  return {
    body: await response.json(),
    status: response.status,
  };
}

function profileEvents(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "profiles.changed" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "profiles.changed" }> =>
      event.event === "profiles.changed",
  );
}

describe("browser profile host composition", () => {
  it("dispatches safe session-only profile mutations and clears them with the host", async () => {
    const jksMaterial = (
      await readFile(join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks"))
    ).toString("base64");
    const store = createBrowserKafkaProfileStore();
    const backend = createKafkaBackend(store);
    const events: HostEvent[] = [];
    backend.subscribe((event) => {
      events.push(event);
    });
    const host = await startDevelopmentHost({
      backend,
      port: 0,
      rendererOrigin,
      token,
    });
    runningHosts.push(host);

    const recipeResponse = await execute(host, {
      command: "recipes.create",
      id: "create-binding-recipe",
      version: HOST_PROTOCOL_VERSION,
      payload: trustRecipeInput(),
    });
    expect(recipeResponse.body).toMatchObject({ ok: true });
    const recipe = events
      .filter((event) => event.event === "recipes.changed")
      .at(-1)
      ?.payload.recipes.find((entry) => entry.name === "Certificate file");
    if (!recipe) throw new Error("Expected created recipe");

    const create = await execute(host, {
      command: "profiles.create",
      id: "profile-create",
      payload: {
        profile: {
          brokers: ["broker.example.test:9093"],
          name: "Browser profile",
          binding: {
            mode: "replace",
            recipeId: recipe.id,
            recipeRevision: recipe.revision,
            overrides: { certificate_path: "/fixture/ca.pem" },
          },
          trust: {
            kind: "jks",
            label: "truststore.jks",
            material: { mode: "replace", value: jksMaterial },
            password: { mode: "replace", value: "password" },
          },
        },
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(create.status).toBe(200);
    expect(store.records()[0]).toMatchObject({
      binding: { recipe, overrides: { certificate_path: "/fixture/ca.pem" } },
    });
    expect(create.body).toMatchObject({
      command: "profiles.create",
      ok: true,
      version: HOST_PROTOCOL_VERSION,
    });

    const created = profileEvents(events).at(-1);
    expect(created?.payload).toMatchObject({
      profiles: [
        {
          active: false,
          brokers: ["broker.example.test:9093"],
          name: "Browser profile",
          trust: {
            kind: "jks",
            label: "truststore.jks",
            materialPresent: true,
            passwordPresent: true,
          },
        },
      ],
      store: {
        durability: "session",
        protection: "memory",
        state: "ready",
      },
    });
    const serializedCreated = JSON.stringify(created);
    expect(serializedCreated).not.toContain(jksMaterial);
    expect(serializedCreated).not.toContain('"value":"password"');
    expect(serializedCreated).not.toContain("BEGIN CERTIFICATE");
    const profileId = created?.payload.profiles[0]?.id;
    if (profileId === undefined) {
      throw new Error("Expected a safe created profile identifier.");
    }

    const update = await execute(host, {
      command: "profiles.update",
      id: "profile-update",
      payload: {
        profile: {
          brokers: ["broker.example.test:19093"],
          name: "Browser profile updated",
          expectedRevision: 1,
          trust: {
            kind: "jks",
            label: "truststore.jks",
            material: { mode: "retain" },
            password: { mode: "retain" },
          },
        },
        profileId,
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(update.body).toMatchObject({ command: "profiles.update", ok: true });
    expect(profileEvents(events).at(-1)?.payload.profiles[0]).toMatchObject({
      brokers: ["broker.example.test:19093"],
      id: profileId,
      name: "Browser profile updated",
      trust: { materialPresent: true, passwordPresent: true },
    });

    const listed = await execute(host, {
      command: "profiles.list",
      id: "profiles-list",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    expect(listed.body).toMatchObject({ command: "profiles.list", ok: true });
    expect(profileEvents(events).at(-1)?.payload.profiles).toHaveLength(1);

    const malformedCreate = await execute(host, {
      command: "profiles.create",
      id: "profile-invalid",
      payload: {
        profile: {
          brokers: ["broker.example.test:9093"],
          name: "Invalid retention",
          trust: {
            kind: "jks",
            label: "truststore.jks",
            material: { mode: "retain" },
            password: { mode: "retain" },
          },
        },
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(malformedCreate.status).toBe(400);
    expect(profileEvents(events).at(-1)?.payload.profiles).toHaveLength(1);

    const deletion = await execute(host, {
      command: "profiles.delete",
      id: "profile-delete",
      payload: { profileId },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(deletion.body).toMatchObject({ command: "profiles.delete", ok: true });
    expect(profileEvents(events).at(-1)?.payload.profiles).toEqual([]);

    await host.close();
    runningHosts.splice(runningHosts.indexOf(host), 1);

    const restartedBackend: StreamSkopeBackend = createKafkaBackend();
    const restartedEvents: HostEvent[] = [];
    restartedBackend.subscribe((event) => {
      restartedEvents.push(event);
    });
    const response = await restartedBackend.execute({
      command: "profiles.list",
      id: "profiles-after-restart",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    expect(response).toMatchObject({ command: "profiles.list", ok: true });
    expect(profileEvents(restartedEvents).at(-1)?.payload).toMatchObject({
      profiles: [],
      store: { durability: "session", protection: "memory", state: "ready" },
    });
  });
});
