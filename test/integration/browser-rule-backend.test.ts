import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostEvent,
  type KafkaRuleDefinition,
} from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/electron/main";
import { startDevelopmentHost, type RunningDevelopmentHost } from "../../src/platform/dev-host";

const rendererOrigin = "http://127.0.0.1:4173";
const token = "0123456789abcdef0123456789abcdef";
const runningHosts: RunningDevelopmentHost[] = [];

const rule: KafkaRuleDefinition = {
  cooldownMs: 5_000,
  enabled: true,
  expression: '$.priority == "high"',
  level: "warn",
  name: "High priority",
  topic: "orders",
};

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

function ruleEvents(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "rules.changed" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "rules.changed" }> =>
      event.event === "rules.changed",
  );
}

describe("browser rule backend composition", () => {
  it("dispatches validated session-only rule workflows and clears them with the host", async () => {
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
        command: "rules.create",
        id: "rules-create",
        payload: { rule },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ body: { ok: true }, status: 200 });
    await expect(
      execute(host, {
        command: "rules.evaluate",
        id: "rules-evaluate",
        payload: {
          sample: '{"priority":"high"}',
          scope: "catalog",
          topic: "orders",
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ body: { ok: true }, status: 200 });

    expect(ruleEvents(events).at(-1)).toMatchObject({
      payload: {
        rules: [{ name: "High priority" }],
        store: { durability: "session", state: "ready" },
      },
    });
    expect(events.filter((event) => event.event === "rules.evaluation").at(-1)).toMatchObject({
      payload: {
        kind: "evaluation",
        requestId: "rules-evaluate",
        results: [{ name: "High priority", outcome: "matched" }],
      },
    });

    const malformed = await execute(host, {
      command: "rules.create",
      id: "rules-invalid",
      payload: { rule: { ...rule, hostFunction: "process.exit()" } },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(malformed.status).toBe(400);
    expect(ruleEvents(events).at(-1)?.payload.rules).toHaveLength(1);

    await host.close();
    runningHosts.splice(runningHosts.indexOf(host), 1);
    await expect(
      backend.execute({
        command: "rules.list",
        id: "rules-after-shutdown",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      error: { code: "BACKEND_UNAVAILABLE" },
      ok: false,
    });

    const restarted = createKafkaBackend();
    const restartedEvents: HostEvent[] = [];
    restarted.subscribe((event) => {
      restartedEvents.push(event);
    });
    await expect(
      restarted.execute({
        command: "rules.list",
        id: "rules-after-restart",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    expect(ruleEvents(restartedEvents).at(-1)).toMatchObject({
      payload: {
        rules: [],
        store: { durability: "session", state: "ready" },
      },
    });
    await restarted.shutdown();
  });
});
