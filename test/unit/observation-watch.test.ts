import { afterEach, expect, it, vi } from "vitest";

import { KafkaConnectionScopes } from "../../src/features/kafka/application/connection-scope";
import { ObservationService } from "../../src/features/kafka/application/observation-service";
import {
  MemoryObservationStore,
  type ObservationStore,
} from "../../src/features/kafka/application/observation-store";
import { ObservationWatch } from "../../src/features/kafka/application/observation-watch";
import {
  emptyObservationWatch,
  parseObservationWatch,
  type ObservationWatchSnapshot,
} from "../../src/features/kafka/contracts/observation-watch";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
} from "../../src/features/kafka/contracts";
import { KAFKA_COMMAND_ACCESS } from "../../src/features/kafka/facade/command-protection";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";

const input = { topic: "events", groupId: null, thresholds: { lag: null, requestMs: null } };
const health = {
  clusterId: "cluster",
  topicId: "topic",
  topic: "events",
  brokerCount: 1,
  controllerKnown: true,
  partitions: [{ partition: 0, leader: 1, replicas: 1, inSyncReplicas: 1, endOffset: "10" }],
};
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done): void => {
    resolve = done;
  });
  return { promise, resolve };
}
function fixture(
  store: ObservationStore = new MemoryObservationStore(),
  changed?: (snapshot: ObservationWatchSnapshot) => void,
): {
  watch: ObservationWatch;
  service: ObservationService;
  store: ObservationStore;
  observe: ReturnType<typeof vi.fn<() => Promise<typeof health>>>;
  group: ReturnType<typeof vi.fn>;
  replace: () => void;
} {
  const observe = vi.fn((): Promise<typeof health> => Promise.resolve(health));
  const group = vi.fn(() => Promise.reject(new Error("Unexpected group read")));
  const connection = Object.assign(new RecordingActiveConnection(), {
    observeTopicHealth: observe,
    describeConsumerGroup: group,
  });
  let context = { connection, generation: 1, connectionName: "Same name" };
  const scopes = new KafkaConnectionScopes(() => context);
  const service = new ObservationService(() => scopes.observation(), store);
  const watch = new ObservationWatch(service, () => scopes.observation(), changed);
  return {
    watch,
    service,
    store,
    observe,
    group,
    replace: (): void => {
      context = { ...context, generation: context.generation + 1 };
    },
  };
}
afterEach((): void => {
  vi.useRealTimers();
});

it("does not manufacture changes when an empty watch is stopped or invalidated repeatedly", async () => {
  const changed = vi.fn();
  const f = fixture(undefined, changed);
  f.watch.invalidate();
  await f.watch.stop();
  f.watch.invalidate();
  expect(f.watch.snapshot()).toEqual(emptyObservationWatch());
  expect(changed).not.toHaveBeenCalled();
  expect(f.observe).not.toHaveBeenCalled();
});

it("publishes evidence revocation once after a completed capture without acquiring another scope", async () => {
  const changed = vi.fn();
  const f = fixture(undefined, changed);
  await f.watch.capture(input);
  expect(f.watch.snapshot().current).toBe(true);
  changed.mockClear();
  f.watch.invalidate();
  const revoked = f.watch.snapshot();
  expect(revoked).toMatchObject({ phase: "stopped", current: false, nextCaptureAt: null });
  expect(changed).toHaveBeenCalledOnce();
  f.watch.invalidate();
  await f.watch.stop();
  expect(f.watch.snapshot()).toEqual(revoked);
  expect(changed).toHaveBeenCalledOnce();
  expect(f.observe).toHaveBeenCalledOnce();
});

it("collects only after opt-in, keeps one completion-relative deadline and stops without another read", async () => {
  vi.useFakeTimers();
  const f = fixture();
  expect(f.watch.snapshot()).toEqual(emptyObservationWatch());
  await vi.advanceTimersByTimeAsync(60_000);
  expect(f.observe).not.toHaveBeenCalled();
  const started = await f.watch.start(input);
  expect(parseObservationWatch(started)).toMatchObject({
    phase: "waiting",
    current: true,
    repeated: true,
    connectionName: "Same name",
    clusterId: "cluster",
    topicId: "topic",
  });
  expect(vi.getTimerCount()).toBe(1);
  await vi.advanceTimersByTimeAsync(10_000);
  expect(f.observe).toHaveBeenCalledTimes(2);
  expect((await f.store.load()).series[0]!.samples).toHaveLength(2);
  const stopped = await f.watch.stop();
  expect(stopped).toMatchObject({ phase: "stopped", current: true, nextCaptureAt: null });
  expect(vi.getTimerCount()).toBe(0);
  await vi.advanceTimersByTimeAsync(60_000);
  expect(f.observe).toHaveBeenCalledTimes(2);
  await f.watch.stop();
  expect(vi.getTimerCount()).toBe(0);
});

it("retains original authority until a slow cancelled read settles and refuses replacement admission", async () => {
  vi.useFakeTimers();
  const f = fixture(),
    gate = deferred<typeof health>();
  f.observe.mockImplementation(() => gate.promise);
  const start = f.watch.start(input);
  await vi.advanceTimersByTimeAsync(0);
  expect(f.observe).toHaveBeenCalledOnce();
  let stopped = false;
  const stop = f.watch.stop().then((): void => {
    stopped = true;
  });
  f.replace();
  await vi.advanceTimersByTimeAsync(60_000);
  expect(stopped).toBe(false);
  expect(f.watch.snapshot().phase).toBe("stopping");
  await expect(f.watch.start(input)).rejects.toMatchObject({ code: "OBSERVATION_BUSY" });
  await expect(f.watch.capture(input)).rejects.toMatchObject({ code: "OBSERVATION_BUSY" });
  gate.resolve(health);
  await Promise.all([start, stop]);
  expect(f.watch.snapshot()).toMatchObject({ phase: "stopped", current: false });
  expect((await f.store.load()).series).toEqual([]);
  expect(f.observe).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});

it("cannot inherit a same-name connection replacement through its delayed timer", async () => {
  vi.useFakeTimers();
  const f = fixture();
  await f.watch.start(input);
  f.replace();
  expect(f.watch.snapshot().current).toBe(false);
  await vi.advanceTimersByTimeAsync(10_000);
  expect(f.observe).toHaveBeenCalledOnce();
  expect(f.watch.snapshot()).toMatchObject({ phase: "failed", current: false });
  expect(f.watch.snapshot().nextCaptureAt).toBe(Date.now() + 10_000);
  expect(vi.getTimerCount()).toBe(0);
  await vi.advanceTimersByTimeAsync(10_000);
  const replacement = await f.watch.start(input);
  expect(replacement.phase).toBe("waiting");
  const samples = (await f.store.load()).series[0]!.samples;
  expect(samples).toHaveLength(2);
  expect(samples[0]!.segmentId).not.toBe(samples[1]!.segmentId);
  await f.watch.stop();
});

it("stops a recreated topic before group or record reads and never appends it to old identity evidence", async () => {
  vi.useFakeTimers();
  const f = fixture();
  f.group.mockResolvedValue({
    id: "workers",
    state: "empty",
    protocol: "",
    protocolType: "",
    members: [],
    offsets: [],
    omittedOffsets: 0,
    omittedMembers: 0,
    omittedAssignments: 0,
  });
  await f.watch.start({ ...input, groupId: "workers" });
  expect(f.group).toHaveBeenCalledOnce();
  f.observe.mockResolvedValue({ ...health, topicId: "recreated-topic" });
  await vi.advanceTimersByTimeAsync(10_000);
  expect(f.group).toHaveBeenCalledOnce();
  expect(f.watch.snapshot()).toMatchObject({
    phase: "failed",
    error: {
      code: "OBSERVATION_INCOMPLETE",
      summary: "The watched Kafka resource identity changed.",
    },
  });
  expect((await f.store.load()).series).toHaveLength(1);
  expect((await f.store.load()).series[0]!.samples).toHaveLength(1);
  expect(vi.getTimerCount()).toBe(0);
});

it("never retries an unknown failure or exposes private causes to attachment status", async () => {
  vi.useFakeTimers();
  const events: ObservationWatchSnapshot[] = [];
  const f = fixture(undefined, (snapshot): void => {
    events.push(snapshot);
  });
  f.observe.mockRejectedValue(new Error("password=private-provider-secret"));
  expect(await f.watch.start(input)).toMatchObject({ phase: "failed", current: false });
  await vi.advanceTimersByTimeAsync(60_000);
  expect(f.observe).toHaveBeenCalledOnce();
  expect(JSON.stringify(events)).not.toContain("private-provider-secret");
  expect(vi.getTimerCount()).toBe(0);
});

it("keeps committed manual receipts historical when revocation arrives during a write", async () => {
  vi.useFakeTimers();
  const memory = new MemoryObservationStore(),
    gate = deferred<void>();
  let committing = false;
  const store: ObservationStore = {
    durability: "durable",
    load: () => memory.load(),
    commit: async (history): Promise<void> => {
      committing = true;
      await gate.promise;
      await memory.commit(history);
    },
  };
  const f = fixture(store);
  const capture = f.watch.capture(input);
  await vi.advanceTimersByTimeAsync(0);
  expect(committing).toBe(true);
  const stop = f.watch.stop();
  await expect(f.watch.start(input)).rejects.toMatchObject({ code: "OBSERVATION_BUSY" });
  gate.resolve();
  const receipt = await capture;
  await stop;
  expect(receipt.series.samples).toHaveLength(1);
  expect(f.watch.snapshot()).toMatchObject({
    phase: "stopped",
    current: false,
    lastSampleId: receipt.series.samples[0]!.id,
    nextCaptureAt: null,
  });
  expect((await memory.load()).series[0]!.samples).toEqual(receipt.series.samples);
  expect(vi.getTimerCount()).toBe(0);
});

it("observer failures cannot release an active watch, and reentrant stop cannot leave a timer", async () => {
  vi.useFakeTimers();
  const f = fixture(undefined, (): never => {
    throw new Error("detached renderer");
  });
  await f.watch.start(input);
  expect(f.watch.active).toBe(true);
  await expect(f.watch.capture(input)).rejects.toMatchObject({ code: "OBSERVATION_BUSY" });
  await f.watch.stop();
  const reentrant = fixture(undefined, (snapshot): void => {
    if (snapshot.phase === "capturing") void reentrant.watch.stop();
  });
  expect(await reentrant.watch.start(input)).toMatchObject({ phase: "stopped", current: false });
  expect(reentrant.observe).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
});

it("closes watch payloads/results/events and permits protected read-only collection without granting writes", () => {
  const base = { id: "watch", version: HOST_PROTOCOL_VERSION };
  for (const command of [
    "observations.watch.start",
    "observations.watch.status",
    "observations.watch.stop",
  ] as const) {
    const payload = command.endsWith("start") ? input : {};
    expect(parseHostCommand({ ...base, command, payload }).command).toBe(command);
    const response = {
      ...base,
      command,
      ok: true,
      result: { correlationId: "c", watch: emptyObservationWatch() },
    };
    expect(parseHostCommandResponse(response)).toEqual(response);
    expect(() =>
      parseHostCommandResponse({
        ...response,
        result: { ...response.result, watch: { ...emptyObservationWatch(), password: "private" } },
      }),
    ).toThrow();
  }
  expect(KAFKA_COMMAND_ACCESS["observations.watch.start"]).toBe("remote-read");
  const event = {
    event: "observations.watch.changed",
    payload: emptyObservationWatch(),
    version: HOST_PROTOCOL_VERSION,
    sequence: 1,
  };
  expect(parseHostEvent(event)).toEqual(event);
  expect(() => parseHostEvent({ ...event, version: HOST_PROTOCOL_VERSION - 1 })).toThrow();
  expect(() =>
    parseHostEvent({ ...event, payload: { ...event.payload, phase: "waiting" } }),
  ).toThrow();
});
