import { describe, expect, it, vi } from "vitest";

import {
  KafkaConnectionScopes,
  type ConnectionScopeContext,
} from "../../src/features/kafka/application/connection-scope";
import { sampleObservationRecords } from "../../src/features/kafka/application/observation-record-sample";
import type {
  KafkaConsumerGroupDetails,
  KafkaWriteInput,
  KafkaWriteOutcome,
} from "../../src/features/kafka/contracts";
import type { TopicHealth } from "../../src/features/kafka/contracts/observations";
import {
  ControlledMessageStream,
  RecordingActiveConnection,
} from "../support/kafka-backend-facade-fixture";

const write: KafkaWriteInput = {
  kind: "topic",
  topic: "scope.events",
  partitions: 1,
  replicationFactor: 1,
  configs: [],
};
const acknowledged: KafkaWriteOutcome = {
  state: "acknowledged",
  detail: "Broker accepted creation.",
  receipt: null,
  verification: "unavailable",
};
function deferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}
class Connection extends RecordingActiveConnection {
  readonly writes: KafkaWriteInput[] = [];
  readonly reviews: KafkaWriteInput[] = [];
  readonly observedTopics: string[] = [];
  readonly health: TopicHealth = {
    clusterId: "cluster-a",
    topicId: "topic-a",
    topic: "scope.events",
    brokerCount: 1,
    controllerKnown: true,
    partitions: [{ partition: 0, leader: 1, replicas: 1, inSyncReplicas: 1, endOffset: "0" }],
  };
  observeTopicHealth(topic: string): Promise<TopicHealth> {
    this.observedTopics.push(topic);
    return Promise.resolve(this.health);
  }
  override describeConsumerGroup(groupId: string): Promise<KafkaConsumerGroupDetails> {
    return Promise.resolve({
      id: groupId,
      state: "stable",
      protocol: "range",
      protocolType: "consumer",
      members: [],
      omittedAssignments: 0,
      omittedMembers: 0,
      omittedOffsets: 0,
      offsets: [
        { topic: this.health.topic, partition: 0, committedOffset: "0", endOffset: "0", lag: "0" },
      ],
    });
  }
  reviewWrite(input: KafkaWriteInput): Promise<void> {
    this.reviews.push(input);
    return Promise.resolve();
  }
  applyWrite(input: KafkaWriteInput): Promise<KafkaWriteOutcome> {
    this.writes.push(input);
    return Promise.resolve(acknowledged);
  }
}
function fixture(): {
  connection: Connection;
  scopes: KafkaConnectionScopes;
  disconnect(): void;
  reconnect(): void;
  replace(): Connection;
} {
  const connection = new Connection();
  let context: ConnectionScopeContext | null = {
    connection,
    generation: 1,
    connectionName: "Cluster A",
  };
  return {
    connection,
    scopes: new KafkaConnectionScopes(() => context),
    disconnect: (): void => {
      context = null;
    },
    reconnect: (): void => {
      context = { connection, generation: 2, connectionName: "Cluster A" };
    },
    replace: (): Connection => {
      const replacement = new Connection();
      context = { connection: replacement, generation: 1, connectionName: "Cluster A" };
      return replacement;
    },
  };
}

describe("connection-scoped authorities", () => {
  it.each(["disconnect", "reconnect", "replace"] as const)(
    "revokes old read and write authorities on %s, even when the profile name is unchanged",
    async (invalidate) => {
      const f = fixture();
      const observation = f.scopes.observation()!;
      const reviewed = f.scopes.reviewedWrite()!;
      f[invalidate]();
      expect(reviewed.isCurrent()).toBe(false);
      expect(reviewed.tryDispatchWrite!(write)).toEqual({ started: false });
      await expect(
        observation.observeTopicHealth!(write.topic, new AbortController().signal),
      ).rejects.toMatchObject({ code: "OBSERVATION_DISCONNECTED" });
      expect(f.connection.writes).toEqual([]);
      expect(f.connection.observedTopics).toEqual([]);
    },
  );

  it("rejects a pending read after same-connection reconnection rather than publishing it as current", async () => {
    const f = fixture();
    const reading = deferred<TopicHealth>();
    vi.spyOn(f.connection, "observeTopicHealth").mockReturnValue(reading.promise);
    const pending = f.scopes.observation()!.observeTopicHealth!(
      write.topic,
      new AbortController().signal,
    );
    const rejected = expect(pending).rejects.toMatchObject({ code: "OBSERVATION_DISCONNECTED" });
    f.reconnect();
    reading.resolve(f.connection.health);
    await rejected;
  });

  it("keeps adapter receivers bound for detached read, review, dispatch and record-reader capabilities", async () => {
    const f = fixture();
    const observation = f.scopes.observation()!;
    const reviewed = f.scopes.reviewedWrite()!;
    const read = observation.observeTopicHealth!;
    const group = observation.describeConsumerGroup!;
    const review = reviewed.reviewWrite!;
    const dispatch = reviewed.tryDispatchWrite!;
    const signal = new AbortController().signal;
    expect(await read(write.topic, signal)).toEqual(f.connection.health);
    expect((await group("workers", signal)).offsets[0]?.topic).toBe(write.topic);
    await review(write);
    const result = dispatch(write);
    expect(result.started).toBe(true);
    if (result.started) expect(await result.result).toEqual(acknowledged);
    const stream = new ControlledMessageStream();
    stream.end();
    f.connection.messageStreamOperations.push(() => Promise.resolve(stream));
    await observation.withRecordReader(signal, (reader) =>
      sampleObservationRecords(reader, write.topic, Date.now(), signal),
    );
    expect(f.connection.observedTopics).toEqual([write.topic]);
    expect(f.connection.reviews).toEqual([write]);
    expect(f.connection.writes).toEqual([write]);
    expect(stream.closeCalls).toBe(1);
    expect(f.connection.closeCalls).toBe(0);
  });

  it("does not open a record reader for a pre-aborted observation", async () => {
    const f = fixture();
    const open = vi.spyOn(f.connection, "openMessageStream");
    const signal = AbortSignal.abort();
    await expect(
      f.scopes
        .observation()!
        .withRecordReader(signal, (reader) =>
          sampleObservationRecords(reader, write.topic, Date.now(), signal),
        ),
    ).rejects.toMatchObject({ code: "CANCELLED" });
    expect(open).not.toHaveBeenCalled();
    expect(f.connection.closeCalls).toBe(0);
  });

  it("lets the sampler close a reader opened after revocation before refusing its stale result", async () => {
    const f = fixture();
    const opening = deferred<ControlledMessageStream>();
    const started = deferred<void>();
    f.connection.messageStreamOperations.push(() => {
      started.resolve();
      return opening.promise;
    });
    const signal = new AbortController().signal;
    const pending = f.scopes
      .observation()!
      .withRecordReader(signal, (reader) =>
        sampleObservationRecords(reader, write.topic, Date.now(), signal),
      );
    const rejected = expect(pending).rejects.toMatchObject({ code: "OBSERVATION_DISCONNECTED" });
    await started.promise;
    f.reconnect();
    const stream = new ControlledMessageStream();
    stream.end();
    opening.resolve(stream);
    await rejected;
    expect(stream.closeCalls).toBe(1);
    expect(f.connection.closeCalls).toBe(0);
  });

  it("preserves the broker receipt of an already dispatched write after authority revocation", async () => {
    const f = fixture();
    const writing = deferred<KafkaWriteOutcome>();
    const apply = vi.spyOn(f.connection, "applyWrite").mockReturnValue(writing.promise);
    const reviewed = f.scopes.reviewedWrite()!;
    const dispatched = reviewed.tryDispatchWrite!(write);
    expect(dispatched.started).toBe(true);
    f.replace();
    writing.resolve(acknowledged);
    if (dispatched.started) expect(await dispatched.result).toEqual(acknowledged);
    expect(reviewed.tryDispatchWrite!(write)).toEqual({ started: false });
    expect(apply).toHaveBeenCalledTimes(1);
  });

  it("keeps one cooldown identity for the same raw connection and distinguishes replacements", () => {
    const f = fixture();
    const identity = f.scopes.observation()!.connectionIdentity;
    expect(f.scopes.observation()!.connectionIdentity).toBe(identity);
    f.reconnect();
    expect(f.scopes.observation()!.connectionIdentity).toBe(identity);
    f.replace();
    expect(f.scopes.observation()!.connectionIdentity).not.toBe(identity);
  });

  it("pins a generation even when the session supplier mutates its context object in place", async () => {
    const connection = new Connection();
    const context = { connection, generation: 1, connectionName: "Cluster A" };
    const scopes = new KafkaConnectionScopes(() => context);
    const observation = scopes.observation()!;
    const reviewed = scopes.reviewedWrite()!;
    context.generation += 1;
    expect(reviewed.isCurrent()).toBe(false);
    expect(reviewed.tryDispatchWrite!(write)).toEqual({ started: false });
    await expect(
      observation.observeTopicHealth!(write.topic, new AbortController().signal),
    ).rejects.toMatchObject({ code: "OBSERVATION_DISCONNECTED" });
    expect(scopes.observation()!.connectionIdentity).toBe(observation.connectionIdentity);
    expect(connection.observedTopics).toEqual([]);
    expect(connection.writes).toEqual([]);
  });
});
