import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
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

describe("browser connection-template backend composition", () => {
  it("rejects retired commands and old protocol clients at the HTTP boundary", async () => {
    const backend = createKafkaBackend();
    const host = await startDevelopmentHost({ backend, port: 0, rendererOrigin, token });
    runningHosts.push(host);
    for (const command of [
      "templates.list",
      "templates.create",
      "templates.update",
      "templates.delete",
      "templates.select",
      "trustAcquisition.password.fetch",
    ]) {
      await expect(
        execute(host, { command, id: "retired", version: HOST_PROTOCOL_VERSION, payload: {} }),
      ).resolves.toMatchObject({ status: 400 });
    }
    await expect(
      execute(host, { command: "recipes.list", id: "old-client", version: 25, payload: {} }),
    ).resolves.toMatchObject({ status: 400 });
    await expect(
      execute(host, {
        command: "trustAcquisition.material.fetch",
        id: "missing-recipe",
        version: HOST_PROTOCOL_VERSION,
        payload: { kind: "pem", label: "ca", target: {} },
      }),
    ).resolves.toMatchObject({ status: 400 });
  });

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
});
