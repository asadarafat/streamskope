import { expect, it, vi } from "vitest";

import type {
  KafkaApplicationSession,
  KafkaActiveConnection,
} from "../../src/features/kafka/application";
import {
  MemoryObservationStore,
  type ObservationStore,
} from "../../src/features/kafka/application/observation-store";
import { ObservationFacade } from "../../src/features/kafka/facade/observation-facade";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  type HostCommandResponse,
} from "../../src/features/kafka/contracts";
import type { ObservationCommand } from "../../src/features/kafka/contracts/observation-protocol";
import { KafkaEngineFailure } from "../../src/features/kafka/engine/failure";

const capture: ObservationCommand = {
  id: "capture",
  version: HOST_PROTOCOL_VERSION,
  command: "observations.capture",
  payload: { topic: "events", groupId: null, thresholds: { lag: null, requestMs: null } },
};
const health = {
  clusterId: "cluster",
  topicId: "topic",
  topic: "events",
  brokerCount: 1,
  controllerKnown: true,
  partitions: [{ partition: 0, leader: 1, replicas: 1, inSyncReplicas: 1, endOffset: "10" }],
};
function facade(
  observe: (() => Promise<typeof health>) | null,
  store: ObservationStore = new MemoryObservationStore(),
  openMessageStream?: KafkaActiveConnection["openMessageStream"],
): ObservationFacade {
  const context = observe
    ? {
        connection: {
          observeTopicHealth: observe,
          ...(openMessageStream === undefined ? {} : { openMessageStream }),
        },
        generation: 1,
      }
    : null;
  return new ObservationFacade(
    { writeContext: () => context } as unknown as KafkaApplicationSession,
    store,
    vi.fn(),
  );
}
function error(response: HostCommandResponse): Exclude<HostCommandResponse, { ok: true }>["error"] {
  expect(response.ok).toBe(false);
  if (response.ok) throw new Error("Expected failure");
  return response.error;
}

it("distinguishes disconnected, cooldown and corrupt history without returning private causes", async () => {
  expect(error(await facade(null).execute(capture, "disconnected")).code).toBe(
    "OBSERVATION_DISCONNECTED",
  );
  const active = facade(() => Promise.resolve(health));
  expect((await active.execute(capture, "first")).ok).toBe(true);
  const cooldown = await active.execute(capture, "cooldown");
  expect(error(cooldown)).toMatchObject({ code: "OBSERVATION_COOLDOWN", retryable: true });
  expect(error(cooldown).retryAfterMs).toBeGreaterThan(0);
  expect(error(cooldown).retryAfterMs).toBeLessThanOrEqual(10000);
  expect(parseHostCommandResponse(cooldown)).toEqual(cooldown);
  const history = facade(() => Promise.resolve(health), {
    durability: "durable",
    load: () => Promise.reject(new Error("password=private store detail")),
    commit: () => Promise.resolve(),
  });
  const failed = await history.execute(
    { id: "history", version: HOST_PROTOCOL_VERSION, command: "observations.history", payload: {} },
    "history",
  );
  expect(error(failed)).toMatchObject({
    code: "OBSERVATION_HISTORY_UNAVAILABLE",
    stage: "storage",
    retryable: false,
  });
  expect(JSON.stringify(failed)).not.toContain("password=private");
});

it.each(["AUTHORIZATION_DENIED", "TIMEOUT", "TOPIC_NOT_FOUND"] as const)(
  "preserves normalized %s and its recovery",
  async (code) => {
    const failed = await facade(() =>
      Promise.reject(
        new KafkaEngineFailure({
          code,
          stage: "kafka",
          retryable: code === "TIMEOUT",
          summary: "Safe selected-resource failure.",
          recovery: "Safe recovery instructions.",
          cause: new Error("password=private broker detail"),
        }),
      ),
    ).execute(capture, "failure");
    expect(error(failed)).toMatchObject({
      code,
      summary: "Safe selected-resource failure.",
      recovery: "Safe recovery instructions.",
      retryable: code === "TIMEOUT",
      correlationId: "failure",
    });
    expect(JSON.stringify(failed)).not.toContain("password=private");
  },
);

it.each(["AUTHORIZATION_DENIED", "TIMEOUT"] as const)(
  "retains sampling %s recovery with usable metadata and no private reader cause",
  async (code) => {
    const recovery =
      code === "AUTHORIZATION_DENIED"
        ? "Request READ permission for the selected topic."
        : "Check Kafka connectivity, then retry the bounded reader.";
    const open = vi.fn(() =>
      Promise.reject(
        new KafkaEngineFailure({
          code,
          stage: code === "AUTHORIZATION_DENIED" ? "authorization" : "kafka",
          retryable: code === "TIMEOUT",
          summary: "Record sampling could not read the selected topic.",
          recovery,
          cause: new Error("password=private reader detail"),
        }),
      ),
    );
    const response = await facade(
      () => Promise.resolve(health),
      new MemoryObservationStore(),
      open,
    ).execute(
      { ...capture, payload: { ...capture.payload, sampleRecords: true } },
      "sample-failure",
    );
    expect(response.ok).toBe(true);
    expect(parseHostCommandResponse(response)).toEqual(response);
    if (!response.ok || response.command !== "observations.capture")
      throw new Error("Expected partial observation capture");
    const sample = response.result.capture.series.samples[0]!;
    expect(sample).toMatchObject({
      state: "partial",
      partitions: [{ endOffset: "10", leader: 1 }],
      records: { state: "unavailable", count: 0, reason: "read-failed", analysisEligible: false },
      issues: [
        {
          measurement: "records",
          code,
          summary: "Record sampling could not read the selected topic.",
          recovery,
          retryable: code === "TIMEOUT",
        },
      ],
    });
    expect(sample.issues).toHaveLength(1);
    expect(open).toHaveBeenCalledOnce();
    expect(JSON.stringify(response)).not.toContain("password=private");
  },
);

it("preserves a late history read failure as storage error without overwriting retained evidence", async () => {
  const load = vi
    .fn<ObservationStore["load"]>()
    .mockResolvedValueOnce({ schemaVersion: 1, series: [] })
    .mockRejectedValueOnce(new Error("password=private history failure after collection"));
  const commit = vi.fn<ObservationStore["commit"]>(() => Promise.resolve());
  const observe = vi.fn(() => Promise.resolve(health));
  const response = await facade(observe, { durability: "durable", load, commit }).execute(
    capture,
    "late-history-failure",
  );
  expect(error(response)).toMatchObject({
    code: "OBSERVATION_HISTORY_UNAVAILABLE",
    stage: "storage",
    retryable: false,
    summary: "Observation history could not be loaded.",
  });
  expect(error(response).recovery).toContain("unreadable file has been preserved");
  expect(observe).toHaveBeenCalledOnce();
  expect(load).toHaveBeenCalledTimes(2);
  expect(commit).not.toHaveBeenCalled();
  expect(JSON.stringify(response)).not.toContain("password=private");
});

it("rejects the previous host protocol explicitly rather than accepting a mismatched renderer", () => {
  expect(() => parseHostCommand({ ...capture, version: HOST_PROTOCOL_VERSION - 1 })).toThrow();
});
