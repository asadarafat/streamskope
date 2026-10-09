import { describe, expect, it, vi, type Mock } from "vitest";

import {
  KafkaConnectionScopes,
  type ConnectionScopeContext,
  type AclReviewScope,
  type MutationDispatch,
  type OffsetResetScope,
  type ReviewedWriteScope,
} from "../../src/features/kafka/application/connection-scope";
import type {
  KafkaActiveConnection,
  KafkaClusterMetadata,
} from "../../src/features/kafka/application/types";
import { sampleObservationRecords } from "../../src/features/kafka/application/observation-record-sample";
import type {
  KafkaConsumerGroupDetails,
  KafkaAclBinding,
  KafkaConfigurationEntry,
  KafkaWriteInput,
  KafkaWriteOutcome,
} from "../../src/features/kafka/contracts";
import type { TopicHealth } from "../../src/features/kafka/contracts/observations";
import type {
  OffsetResetInput,
  OffsetResetResult,
  OffsetResetReview,
  OffsetResetSnapshot,
  OffsetResetTarget,
} from "../../src/features/kafka/contracts/offset-reset";
import type { KafkaWriteDestination } from "../../src/features/kafka/contracts/reviewed-writes";
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
  it("exposes only connection-fenced finite reader authority and forwards the private checkpoint", async () => {
    const f = fixture();
    const scope = f.scopes.recordRead()!;
    const request = { topic: "scope.events", mode: "earliest", maxMessages: 10 } as const;
    const stream = new ControlledMessageStream();
    const open = vi.spyOn(f.connection, "openMessageStream").mockResolvedValue(stream);
    const checkpoint = {
      clusterId: "cluster",
      topicId: "topic",
      partitionCount: 0,
      coverage: {
        reason: "range-complete",
        scannedRecords: 0,
        scannedBytes: 0,
        matchedRecords: 0,
        unavailableRecords: 0,
        partitions: [],
      },
    } as const;
    const signal = new AbortController().signal;
    expect(Object.keys(scope).sort()).toEqual(["connectionName", "isCurrent", "openMessageStream"]);
    expect(await scope.openMessageStream(request, signal, checkpoint)).toBe(stream);
    expect(open).toHaveBeenCalledWith(request, signal, checkpoint, undefined);
    f.reconnect();
    await expect(scope.openMessageStream(request, signal)).rejects.toThrow("connection changed");
    expect(open).toHaveBeenCalledOnce();
    await expect(
      f.scopes.recordRead()!.openMessageStream(request, AbortSignal.abort()),
    ).rejects.toThrow();
    expect(open).toHaveBeenCalledOnce();
    f.disconnect();
    expect(f.scopes.recordRead()).toBeNull();
  });

  it("forwards the exact record locator only through its original connection scope", async () => {
    const f = fixture();
    const scope = f.scopes.recordRead()!;
    const request = { topic: "scope.events", mode: "earliest", maxMessages: 1 } as const;
    const stream = new ControlledMessageStream();
    const open = vi.spyOn(f.connection, "openMessageStream").mockResolvedValue(stream);
    const locator = {
      schemaVersion: 1,
      clusterId: "cluster-a",
      topicId: "12345678-1234-1234-1234-123456789abc",
      topic: request.topic,
      partition: 0,
      offset: "9007199254740993",
      leaderEpoch: 3,
    } as const;
    const signal = new AbortController().signal;
    expect(await scope.openMessageStream(request, signal, undefined, locator)).toBe(stream);
    expect(open).toHaveBeenCalledWith(request, signal, undefined, locator);
    f.replace();
    await expect(scope.openMessageStream(request, signal, undefined, locator)).rejects.toThrow(
      "connection changed",
    );
    expect(open).toHaveBeenCalledOnce();
  });

  it("returns a late-opened export reader to its original owner so revocation cannot leak it", async () => {
    const f = fixture();
    const opened = deferred<ControlledMessageStream>();
    vi.spyOn(f.connection, "openMessageStream").mockReturnValue(opened.promise);
    const scope = f.scopes.recordRead()!;
    const pending = scope.openMessageStream(
      { topic: "events", mode: "earliest", maxMessages: 1 },
      new AbortController().signal,
    );
    f.reconnect();
    const stream = new ControlledMessageStream();
    opened.resolve(stream);
    expect(await pending).toBe(stream);
    expect(scope.isCurrent()).toBe(false);
    await stream.close();
    expect(stream.closeCalls).toBe(1);
  });

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
    const acl = scopes.aclReview()!;
    const reset = scopes.offsetReset()!;
    context.generation += 1;
    expect(reviewed.isCurrent()).toBe(false);
    expect(acl.isCurrent()).toBe(false);
    expect(reset.isCurrent()).toBe(false);
    expect(reviewed.tryDispatchWrite!(write)).toEqual({ started: false });
    await expect(
      observation.observeTopicHealth!(write.topic, new AbortController().signal),
    ).rejects.toMatchObject({ code: "OBSERVATION_DISCONNECTED" });
    expect(scopes.observation()!.connectionIdentity).toBe(observation.connectionIdentity);
    expect(connection.observedTopics).toEqual([]);
    expect(connection.writes).toEqual([]);
  });
});

const acl: KafkaAclBinding = {
  resourceType: "TOPIC",
  resourceName: "scope.events",
  patternType: "LITERAL",
  principal: "User:reader",
  host: "*",
  operation: "READ",
  permission: "ALLOW",
};
const resetTarget: OffsetResetTarget = { topic: "scope.events", partition: 0, offset: "10" };
const resetInput: OffsetResetInput = { groupId: "workers", targets: [resetTarget] };
const resetResult: OffsetResetResult = {
  ...resetTarget,
  state: "acknowledged",
  observed: "10",
  verified: true,
};
const destination: KafkaWriteDestination = {
  clusterId: "cluster-a",
  topicId: "topic-a",
  partitions: 1,
};

type MutationMethods = Pick<
  Required<KafkaActiveConnection>,
  | "describeClusterMetadata"
  | "describeBrokerConfiguration"
  | "listAcls"
  | "offsetResetSnapshot"
  | "offsetResetExamples"
  | "reviewWrite"
  | "createAcl"
  | "deleteAcl"
  | "resetGroupOffset"
  | "applyWrite"
>;
interface MutationFixture {
  readonly connection: KafkaActiveConnection;
  readonly methods: { readonly [K in keyof MutationMethods]: Mock<MutationMethods[K]> };
  readonly scopes: KafkaConnectionScopes;
  readonly acl: AclReviewScope;
  readonly reset: OffsetResetScope;
  readonly write: ReviewedWriteScope;
  delayReads(): () => void;
  delayMutations(): () => void;
  disconnect(): void;
  reconnect(): void;
  replace(): void;
}

function mutationFixture(): MutationFixture {
  let reads: Promise<void> = Promise.resolve();
  let mutations: Promise<void> = Promise.resolve();
  const methods = {
    describeClusterMetadata: vi.fn(async function (
      this: Connection,
      _signal?: AbortSignal,
    ): Promise<KafkaClusterMetadata> {
      expect(this).toBe(connection);
      await reads;
      return { clusterId: "cluster-a", controllerId: 1, brokers: [] };
    }),
    describeBrokerConfiguration: vi.fn(async function (
      this: Connection,
      _brokerId: number,
      _signal?: AbortSignal,
    ): Promise<readonly KafkaConfigurationEntry[]> {
      expect(this).toBe(connection);
      await reads;
      return [];
    }),
    listAcls: vi.fn(async function (
      this: Connection,
      _signal?: AbortSignal,
    ): Promise<readonly KafkaAclBinding[]> {
      expect(this).toBe(connection);
      await reads;
      return [acl];
    }),
    offsetResetSnapshot: vi.fn(async function (
      this: Connection,
      _input: OffsetResetInput,
    ): Promise<OffsetResetSnapshot> {
      expect(this).toBe(connection);
      await reads;
      return { inactive: true, state: "Empty", groupRead: "allowed", partitions: [] };
    }),
    offsetResetExamples: vi.fn(async function (
      this: Connection,
      _input: OffsetResetInput,
    ): Promise<Pick<OffsetResetReview, "examples" | "exampleStatus">> {
      expect(this).toBe(connection);
      await reads;
      return { examples: [], exampleStatus: "empty" };
    }),
    reviewWrite: vi.fn(async function (
      this: Connection,
      _input: KafkaWriteInput,
    ): Promise<KafkaWriteDestination> {
      expect(this).toBe(connection);
      await reads;
      return destination;
    }),
    createAcl: vi.fn(function (this: Connection, _binding: KafkaAclBinding): Promise<void> {
      expect(this).toBe(connection);
      return mutations;
    }),
    deleteAcl: vi.fn(function (this: Connection, _binding: KafkaAclBinding): Promise<void> {
      expect(this).toBe(connection);
      return mutations;
    }),
    resetGroupOffset: vi.fn(function (
      this: Connection,
      _groupId: string,
      _target: OffsetResetTarget,
    ): Promise<OffsetResetResult> {
      expect(this).toBe(connection);
      return mutations.then(() => resetResult);
    }),
    applyWrite: vi.fn(function (
      this: Connection,
      _input: KafkaWriteInput,
    ): Promise<KafkaWriteOutcome> {
      expect(this).toBe(connection);
      return mutations.then(() => acknowledged);
    }),
  };
  const connection = Object.assign(new Connection(), methods);
  let context: ConnectionScopeContext | null = {
    connection,
    generation: 1,
    connectionName: "Cluster A",
  };
  const scopes = new KafkaConnectionScopes(() => context);
  return {
    connection,
    methods,
    scopes,
    acl: scopes.aclReview()!,
    reset: scopes.offsetReset()!,
    write: scopes.reviewedWrite()!,
    delayReads: (): (() => void) => {
      const gate = deferred<void>();
      reads = gate.promise;
      return (): void => gate.resolve();
    },
    delayMutations: (): (() => void) => {
      const gate = deferred<void>();
      mutations = gate.promise;
      return (): void => gate.resolve();
    },
    disconnect: (): void => {
      context = null;
    },
    reconnect: (): void => {
      context = { connection, generation: 2, connectionName: "Cluster A" };
    },
    replace: (): void => {
      context = { connection: new Connection(), generation: 1, connectionName: "Cluster A" };
    },
  };
}

describe("narrow mutation scopes", () => {
  it("exposes only each operation's capabilities and omits unsupported operations", () => {
    const f = mutationFixture();
    expect(Object.keys(f.acl).sort()).toEqual([
      "connectionName",
      "describeBrokerConfiguration",
      "describeClusterMetadata",
      "isCurrent",
      "listAcls",
      "tryCreateAcl",
      "tryDeleteAcl",
    ]);
    expect(Object.keys(f.reset).sort()).toEqual([
      "connectionName",
      "isCurrent",
      "offsetResetExamples",
      "offsetResetSnapshot",
      "tryResetGroupOffset",
    ]);
    expect(Object.keys(f.write).sort()).toEqual([
      "connectionName",
      "isCurrent",
      "reviewWrite",
      "tryDispatchWrite",
    ]);
    const unavailable = fixture().scopes;
    expect(Object.keys(unavailable.aclReview()!).sort()).toEqual([
      "connectionName",
      "describeBrokerConfiguration",
      "describeClusterMetadata",
      "isCurrent",
    ]);
    expect(Object.keys(unavailable.offsetReset()!).sort()).toEqual(["connectionName", "isCurrent"]);
    f.disconnect();
    expect(f.scopes.aclReview()).toBeNull();
    expect(f.scopes.offsetReset()).toBeNull();
  });

  it.each(["disconnect", "reconnect", "replace"] as const)(
    "revokes every retained mutation scope on %s without touching the adapter",
    async (invalidate) => {
      const f = mutationFixture();
      f[invalidate]();
      expect(f.acl.isCurrent()).toBe(false);
      expect(f.reset.isCurrent()).toBe(false);
      expect(f.write.isCurrent()).toBe(false);
      expect(f.acl.tryCreateAcl!(acl)).toEqual({ started: false });
      expect(f.acl.tryDeleteAcl!(acl)).toEqual({ started: false });
      expect(f.reset.tryResetGroupOffset!(resetInput.groupId, resetTarget)).toEqual({
        started: false,
      });
      expect(f.write.tryDispatchWrite!(write)).toEqual({ started: false });
      const reads = [
        f.acl.describeClusterMetadata(),
        f.acl.describeBrokerConfiguration(1),
        f.acl.listAcls!(),
        f.reset.offsetResetSnapshot!(resetInput),
        f.reset.offsetResetExamples!(resetInput),
        f.write.reviewWrite!(write),
      ];
      await Promise.all(reads.map((read) => expect(read).rejects.toThrow("connection changed")));
      for (const method of Object.values(f.methods)) expect(method).not.toHaveBeenCalled();
    },
  );

  it("rejects all late review reads after same-adapter reconnection", async () => {
    const f = mutationFixture();
    const complete = f.delayReads();
    const reads = [
      f.acl.describeClusterMetadata(),
      f.acl.describeBrokerConfiguration(1),
      f.acl.listAcls!(),
      f.reset.offsetResetSnapshot!(resetInput),
      f.reset.offsetResetExamples!(resetInput),
      f.write.reviewWrite!(write),
    ];
    const rejections = reads.map((read) => expect(read).rejects.toThrow("connection changed"));
    f.reconnect();
    complete();
    await Promise.all(rejections);
  });

  it("binds detached reads and forwards their inputs, signals and reviewed destination", async () => {
    const f = mutationFixture();
    const signal = new AbortController().signal;
    const metadata = f.acl.describeClusterMetadata;
    const config = f.acl.describeBrokerConfiguration;
    const list = f.acl.listAcls!;
    const snapshot = f.reset.offsetResetSnapshot!;
    const examples = f.reset.offsetResetExamples!;
    const review = f.write.reviewWrite!;
    await metadata(signal);
    await config(1, signal);
    expect(await list(signal)).toEqual([acl]);
    await snapshot(resetInput);
    await examples(resetInput);
    expect(await review(write)).toEqual(destination);
    expect(f.methods.describeClusterMetadata).toHaveBeenCalledWith(signal);
    expect(f.methods.describeBrokerConfiguration).toHaveBeenCalledWith(1, signal);
    expect(f.methods.listAcls).toHaveBeenCalledWith(signal);
    expect(f.methods.offsetResetSnapshot).toHaveBeenCalledWith(resetInput);
    expect(f.methods.offsetResetExamples).toHaveBeenCalledWith(resetInput);
  });

  const dispatches = [
    {
      name: "create ACL",
      dispatch: (f: ReturnType<typeof mutationFixture>): (() => MutationDispatch<void>) => {
        const send = f.acl.tryCreateAcl!;
        return () => send(acl);
      },
      method: "createAcl",
      result: undefined,
    },
    {
      name: "delete ACL",
      dispatch: (f: ReturnType<typeof mutationFixture>): (() => MutationDispatch<void>) => {
        const send = f.acl.tryDeleteAcl!;
        return () => send(acl);
      },
      method: "deleteAcl",
      result: undefined,
    },
    {
      name: "reset offset",
      dispatch: (
        f: ReturnType<typeof mutationFixture>,
      ): (() => MutationDispatch<OffsetResetResult>) => {
        const send = f.reset.tryResetGroupOffset!;
        return () => send(resetInput.groupId, resetTarget);
      },
      method: "resetGroupOffset",
      result: resetResult,
    },
    {
      name: "write record",
      dispatch: (
        f: ReturnType<typeof mutationFixture>,
      ): (() => MutationDispatch<KafkaWriteOutcome>) => {
        const send = f.write.tryDispatchWrite!;
        return () => send(write);
      },
      method: "applyWrite",
      result: acknowledged,
    },
  ] as const;

  it.each(dispatches)(
    "dispatches $name immediately and preserves its acknowledgement after revocation",
    async ({ dispatch, method, result }) => {
      const f = mutationFixture();
      const complete = f.delayMutations();
      const send = dispatch(f);
      const admitted = send();
      expect(f.methods[method]).toHaveBeenCalledTimes(1);
      expect(admitted.started).toBe(true);
      f.reconnect();
      complete();
      if (admitted.started) expect(await admitted.result).toEqual(result);
      expect(send()).toEqual({ started: false });
      expect(f.methods[method]).toHaveBeenCalledTimes(1);
    },
  );

  it.each(dispatches)(
    "retains admission when $name throws synchronously",
    async ({ dispatch, method }) => {
      const f = mutationFixture();
      const failure = new Error("Transport failed after admission.");
      f.methods[method].mockImplementation(() => {
        throw failure;
      });
      const result = dispatch(f)();
      expect(result.started).toBe(true);
      if (result.started) await expect(result.result).rejects.toBe(failure);
      expect(f.methods[method]).toHaveBeenCalledTimes(1);
    },
  );
});
