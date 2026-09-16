import { describe, expect, it } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostEvent } from "../../src/features/kafka/contracts";
import type { KafkaApplicationSession, RedpandaTransformPort } from "../../src/features/kafka/application";
import { TransformFacadeController } from "../../src/features/kafka/facade/transform-facade";

function failure(name: string, status: number | null): Error & { readonly status: number | null } {
  return Object.assign(new Error("safe fixture failure"), { name, status });
}

function controllerFor(
  port: RedpandaTransformPort,
  events: HostEvent[],
): TransformFacadeController {
  const session = {
    clusterServiceContext: () => ({
      authorization: (): Promise<undefined> => Promise.resolve(undefined),
      baseUrl: "http://redpanda:9644",
      caPem: "fixture-ca",
    }),
    snapshot: () => ({ connectionName: "Local Redpanda", state: "connected" }),
  } as unknown as KafkaApplicationSession;
  let sequence = 0;
  return new TransformFacadeController({
    nextSequence: () => ++sequence,
    now: () => new Date("2026-08-12T12:00:00.000Z"),
    port,
    publish: (event) => events.push(event),
    recordActivity: () => undefined,
    session,
  });
}

describe("Redpanda transform facade", () => {
  it.each([
    [failure("RedpandaTransformResponseError", 404), "unsupported"],
    [failure("RedpandaTransformResponseError", null), "invalid-response"],
  ] as const)("publishes the truthful %s inventory state", async (upstream, state) => {
    const events: HostEvent[] = [];
    const controller = controllerFor(
      {
        delete: () => Promise.resolve(),
        list: () => Promise.reject(upstream),
      },
      events,
    );

    await expect(
      controller.execute(
        {
          command: "transforms.list",
          id: `transform-${state}`,
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        },
        `correlation-${state}`,
      ),
    ).resolves.toMatchObject({ ok: false });
    expect(events.at(-1)).toMatchObject({
      event: "transforms.changed",
      payload: { state, transforms: [] },
    });
  });
});
