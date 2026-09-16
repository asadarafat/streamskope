import { afterEach, describe, expect, it } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostCommand, type HostEvent } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/electron/main";
import { startDevelopmentHost, type RunningDevelopmentHost } from "../../src/platform/dev-host";
import { trustRecipeInput } from "../support/trust-recipe";

const rendererOrigin = "http://127.0.0.1:4173";
const token = "0123456789abcdef0123456789abcdef";
const runningHosts: RunningDevelopmentHost[] = [];

afterEach(async () => {
  for (const host of runningHosts.splice(0).reverse()) {
    await host.close();
  }
});

async function execute(
  host: RunningDevelopmentHost,
  command: HostCommand | Readonly<Record<string, unknown>>,
): Promise<{ readonly body: unknown; readonly status: number }> {
  const response = await fetch(`${host.origin}/commands`, {
    body: JSON.stringify(command),
    headers: {
      "content-type": "application/json",
      origin: rendererOrigin,
      "x-streamskope-token": token,
    },
    method: "POST",
  });
  return { body: await response.json(), status: response.status };
}

function templateEvents(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "templates.changed" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "templates.changed" }> =>
      event.event === "templates.changed",
  );
}

describe("browser connection-template backend composition", () => {
  it("validates recipe requests at the HTTP boundary without persisting a browser library", async () => {
    const backend = createKafkaBackend();
    const events: HostEvent[] = [];
    backend.subscribe((event) => {
      events.push(event);
    });
    const host = await startDevelopmentHost({ backend, port: 0, rendererOrigin, token });
    runningHosts.push(host);
    await expect(
      execute(host, {
        command: "recipes.create",
        id: "recipe-create",
        version: HOST_PROTOCOL_VERSION,
        payload: { ...trustRecipeInput(), name: "Session certificate" },
      }),
    ).resolves.toMatchObject({ body: { ok: true }, status: 200 });
    const snapshot = events.filter((event) => event.event === "recipes.changed").at(-1);
    expect(snapshot?.payload.store).toEqual({ durability: "session", state: "ready" });
    expect(snapshot?.payload.recipes.some((recipe) => recipe.name === "Session certificate")).toBe(
      true,
    );
    await expect(
      execute(host, {
        command: "recipes.create",
        id: "recipe-invalid",
        version: HOST_PROTOCOL_VERSION,
        payload: { ...trustRecipeInput(), password: "forbidden-inline-secret" },
      }),
    ).resolves.toMatchObject({ status: 400 });
    expect(events.filter((event) => event.event === "recipes.changed")).toHaveLength(1);
    expect(events.some((event) => event.event === "profiles.changed")).toBe(false);
    await host.close();
    runningHosts.splice(runningHosts.indexOf(host), 1);
    const restarted = createKafkaBackend();
    const restored: HostEvent[] = [];
    restarted.subscribe((event) => {
      restored.push(event);
    });
    try {
      await restarted.execute({
        command: "recipes.list",
        id: "recipe-restart",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      expect(
        restored
          .filter((event) => event.event === "recipes.changed")
          .at(-1)
          ?.payload.recipes.some((recipe) => recipe.name === "Session certificate"),
      ).toBe(false);
    } finally {
      await restarted.shutdown();
    }
  });

  it("dispatches bounded session-only mutations and clears them with the host", async () => {
    const backend = createKafkaBackend();
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

    await expect(
      execute(host, {
        command: "templates.list",
        id: "templates-list",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ body: { ok: true }, status: 200 });
    await expect(
      execute(host, {
        command: "templates.create",
        id: "templates-create",
        payload: {
          catalog: "oauth-endpoint",
          name: "Local aio",
          template: "http://{host}:15000/token",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ body: { ok: true }, status: 200 });
    await execute(host, {
      command: "templates.select",
      id: "templates-select",
      payload: { catalog: "oauth-endpoint", name: "Local aio" },
      version: HOST_PROTOCOL_VERSION,
    });

    expect(templateEvents(events).at(-1)).toMatchObject({
      payload: {
        catalogs: [
          { catalog: "truststore-fetch" },
          { catalog: "truststore-password" },
          {
            catalog: "oauth-endpoint",
            entries: [{ name: "nsp-25-4" }, { name: "Local aio" }],
            selectedName: "Local aio",
          },
        ],
        store: { durability: "session", state: "ready" },
      },
    });

    const malformed = await execute(host, {
      command: "templates.create",
      id: "templates-invalid",
      payload: {
        catalog: "truststore-fetch",
        name: "Invalid",
        template: "copy {unknown}",
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(malformed.status).toBe(400);
    expect(templateEvents(events).at(-1)?.payload.catalogs[0]?.entries).toHaveLength(2);

    await host.close();
    runningHosts.splice(runningHosts.indexOf(host), 1);

    const restarted = createKafkaBackend();
    const restartedEvents: HostEvent[] = [];
    restarted.subscribe((event) => {
      restartedEvents.push(event);
    });
    await restarted.execute({
      command: "templates.list",
      id: "templates-after-restart",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    expect(templateEvents(restartedEvents).at(-1)).toMatchObject({
      payload: {
        catalogs: [
          { entries: [{ name: "nsp-25-4" }, { name: "nsp-25-11" }] },
          { entries: [{ name: "nsp-25-11" }] },
          { entries: [{ name: "nsp-25-4" }], selectedName: "nsp-25-4" },
        ],
        store: { durability: "session", state: "ready" },
      },
    });
    await restarted.shutdown();
  });
});
