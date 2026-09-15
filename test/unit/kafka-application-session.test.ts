import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  KafkaConsumerGroupDetails,
  KafkaFetchRequest,
  KafkaMessage,
  KafkaTopicConfigurationChange,
  SecureConnectionInput,
} from "../../src/kafka/contracts";
import {
  ConnectionAttemptSupersededError,
  KafkaApplicationSession,
  type KafkaActiveConnection,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaConsumerGroupInventory,
} from "../../src/kafka/application";

const firstConnection: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "First cluster",
  oauth: {
    clientId: "admin",
    clientSecret: "first-secret",
    scope: "kafka",
    tokenEndpoint: "http://localhost:15000/token",
  },
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfirst\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

const secondConnection: SecureConnectionInput = {
  ...firstConnection,
  name: "Second cluster",
  oauth: {
    ...firstConnection.oauth!,
    clientSecret: "second-secret",
  },
};

interface Deferred<Value> {
  readonly promise: Promise<Value>;
  reject(reason: Error): void;
  resolve(value: Value): void;
}

function deferred<Value>(): Deferred<Value> {
  let resolvePromise: ((value: Value) => void) | undefined;
  let rejectPromise: ((reason: Error) => void) | undefined;
  const promise = new Promise<Value>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return {
    promise,
    reject: (reason): void => {
      rejectPromise?.(reason);
    },
    resolve: (value): void => {
      resolvePromise?.(value);
    },
  };
}

class RecordingConnection implements KafkaActiveConnection {
  readonly alterCalls: Array<AbortSignal | undefined> = [];
  readonly alterOperations: Array<(signal: AbortSignal) => Promise<void>> = [];
  closeCalls = 0;
  readonly closeResult = deferred<void>();
  readonly listCalls: Array<AbortSignal | undefined> = [];
  readonly listOperations: Array<(signal: AbortSignal) => Promise<readonly string[]>> = [];
  readonly consumerGroupListCalls: Array<AbortSignal | undefined> = [];
  readonly consumerGroupListOperations: Array<
    (signal: AbortSignal) => Promise<KafkaConsumerGroupInventory>
  > = [];
  readonly consumerGroupDetailCalls: Array<{
    readonly groupId: string;
    readonly signal: AbortSignal | undefined;
  }> = [];
  readonly consumerGroupDetailOperations: Array<
    (groupId: string, signal: AbortSignal) => Promise<KafkaConsumerGroupDetails>
  > = [];
  readonly messageStreamCalls: Array<{
    readonly request: KafkaFetchRequest;
    readonly signal: AbortSignal;
  }> = [];
  readonly messageStreamOperations: Array<
    (request: KafkaFetchRequest, signal: AbortSignal) => Promise<ControlledMessageStream>
  > = [];

  constructor(private readonly closeImmediately = true) {}

  alterTopicConfiguration(
    _topic: string,
    _changes: readonly KafkaTopicConfigurationChange[],
    _validateOnly: boolean,
    signal?: AbortSignal,
  ): Promise<void> {
    this.alterCalls.push(signal);
    const operation = this.alterOperations.shift();
    return operation === undefined
      ? Promise.reject(new Error("No topic-configuration operation was configured."))
      : operation(signal ?? new AbortController().signal);
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    if (this.closeImmediately) {
      return Promise.resolve();
    }
    return this.closeResult.promise;
  }

  describeBrokerConfiguration(): never {
    throw new Error("No broker-configuration operation was configured.");
  }

  describeClusterMetadata(): never {
    throw new Error("No cluster-metadata operation was configured.");
  }

  describeConsumerGroup(groupId: string, signal?: AbortSignal): Promise<KafkaConsumerGroupDetails> {
    this.consumerGroupDetailCalls.push({ groupId, signal });
    const operation = this.consumerGroupDetailOperations.shift();
    return operation === undefined
      ? Promise.reject(new Error("No consumer-group detail operation was configured."))
      : operation(groupId, signal ?? new AbortController().signal);
  }

  listConsumerGroups(signal?: AbortSignal): Promise<KafkaConsumerGroupInventory> {
    this.consumerGroupListCalls.push(signal);
    const operation = this.consumerGroupListOperations.shift();
    return operation === undefined
      ? Promise.resolve({ groups: [], omittedGroups: 0 })
      : operation(signal ?? new AbortController().signal);
  }

  listTopics(signal?: AbortSignal): Promise<readonly string[]> {
    this.listCalls.push(signal);
    const operation = this.listOperations.shift();
    return operation === undefined
      ? Promise.resolve([])
      : operation(signal ?? new AbortController().signal);
  }

  describeTopicConfiguration(): Promise<readonly never[]> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }

  openMessageStream(
    request: KafkaFetchRequest,
    signal: AbortSignal,
  ): Promise<ControlledMessageStream> {
    this.messageStreamCalls.push({ request, signal });
    const operation = this.messageStreamOperations.shift();
    if (operation === undefined) {
      throw new Error("No message-stream operation was configured.");
    }
    return operation(request, signal);
  }
}

type StreamResult =
  | { readonly kind: "message"; readonly message: KafkaMessage }
  | { readonly error: Error; readonly kind: "error" }
  | { readonly kind: "end" };

class ControlledMessageStream implements AsyncIterable<KafkaMessage> {
  closeCalls = 0;
  private closed = false;
  private readonly queued: StreamResult[] = [];
  private readonly waiting: Array<(result: StreamResult) => void> = [];

  close(): Promise<void> {
    this.closeCalls += 1;
    if (!this.closed) {
      this.closed = true;
      this.deliver({ kind: "end" });
    }
    return Promise.resolve();
  }

  end(): void {
    if (!this.closed) {
      this.closed = true;
      this.deliver({ kind: "end" });
    }
  }

  fail(error: Error): void {
    if (!this.closed) {
      this.closed = true;
      this.deliver({ error, kind: "error" });
    }
  }

  push(message: KafkaMessage): void {
    if (!this.closed) {
      this.deliver({ kind: "message", message });
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    while (true) {
      const result = await this.nextResult();
      if (result.kind === "end") {
        return;
      }
      if (result.kind === "error") {
        throw result.error;
      }
      yield result.message;
    }
  }

  private deliver(result: StreamResult): void {
    const waiter = this.waiting.shift();
    if (waiter === undefined) {
      this.queued.push(result);
    } else {
      waiter(result);
    }
  }

  private nextResult(): Promise<StreamResult> {
    const result = this.queued.shift();
    return result === undefined
      ? new Promise((resolve) => {
          this.waiting.push(resolve);
        })
      : Promise.resolve(result);
  }
}

function message(id: string, topic = "test"): KafkaMessage {
  return {
    headers: {},
    id,
    key: "key",
    offset: id,
    originalByteSize: 7,
    partition: 0,
    payload: "value",
    preview: "value",
    timestamp: "2026-07-25T15:00:00.000Z",
    topic,
    truncated: false,
  };
}

function tailRequest(topic = "test"): KafkaFetchRequest {
  return {
    maxMessages: 1_000,
    mode: "tail",
    topic,
  };
}

function newestRequest(topic = "test"): KafkaFetchRequest {
  return {
    maxMessages: 10,
    mode: "newest",
    topic,
  };
}

async function settleAsyncIteration(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

interface PortCall {
  readonly connection: SecureConnectionInput;
  readonly signal: AbortSignal;
}

type OpenOperation = (
  connection: SecureConnectionInput,
  signal: AbortSignal,
) => Promise<KafkaActiveConnection>;

type TestOperation = (
  connection: SecureConnectionInput,
  signal: AbortSignal,
) => Promise<KafkaConnectionTestResult>;

class RecordingConnectionPort implements KafkaConnectionPort {
  readonly openCalls: PortCall[] = [];
  readonly testCalls: PortCall[] = [];
  readonly openOperations: OpenOperation[] = [];
  readonly testOperations: TestOperation[] = [];

  openConnection(
    connection: SecureConnectionInput,
    signal: AbortSignal,
  ): Promise<KafkaActiveConnection> {
    this.openCalls.push({ connection, signal });
    const operation = this.openOperations.shift();
    if (operation === undefined) {
      throw new Error("No open-connection operation was configured.");
    }
    return operation(connection, signal);
  }

  testConnection(
    connection: SecureConnectionInput,
    signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    this.testCalls.push({ connection, signal });
    const operation = this.testOperations.shift();
    if (operation === undefined) {
      throw new Error("No test-connection operation was configured.");
    }
    return operation(connection, signal);
  }
}

const successfulTest: KafkaConnectionTestResult = {
  checks: ["oauth", "tls", "kafka-authentication", "metadata"],
  topicCount: 1,
};

describe("Kafka application connection lifecycle", () => {
  it("does not report Connected until the connection port confirms metadata", async () => {
    const pendingOpen = deferred<KafkaActiveConnection>();
    const activeConnection = new RecordingConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => pendingOpen.promise);
    const session = new KafkaApplicationSession(port);

    const connecting = session.connect(firstConnection);

    expect(session.snapshot()).toEqual({
      connectionName: "First cluster",
      state: "connecting",
    });
    pendingOpen.resolve(activeConnection);
    await connecting;
    expect(session.snapshot()).toEqual({
      connectionName: "First cluster",
      state: "connected",
    });
    expect(activeConnection.closeCalls).toBe(0);
  });

  it("tests a temporary connection without replacing or closing the active connection", async () => {
    const activeConnection = new RecordingConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    port.testOperations.push(() => Promise.resolve(successfulTest));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const snapshots: unknown[] = [];
    session.subscribe((snapshot) => {
      snapshots.push(snapshot);
    });

    await expect(session.testConnection(secondConnection)).resolves.toEqual(successfulTest);

    expect(session.snapshot()).toEqual({
      connectionName: "First cluster",
      state: "connected",
    });
    expect(activeConnection.closeCalls).toBe(0);
    expect(snapshots).toEqual([]);
    expect(port.testCalls[0]?.connection.name).toBe("Second cluster");
  });

  it("closes and ignores a late connection result after a newer connection wins", async () => {
    const firstOpen = deferred<KafkaActiveConnection>();
    const secondOpen = deferred<KafkaActiveConnection>();
    const staleConnection = new RecordingConnection();
    const currentConnection = new RecordingConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(
      () => firstOpen.promise,
      () => secondOpen.promise,
    );
    const session = new KafkaApplicationSession(port);

    const obsoleteAttempt = session.connect(firstConnection);
    const currentAttempt = session.connect(secondConnection);
    expect(port.openCalls[0]?.signal.aborted).toBe(true);

    secondOpen.resolve(currentConnection);
    await currentAttempt;
    firstOpen.resolve(staleConnection);

    await expect(obsoleteAttempt).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    expect(staleConnection.closeCalls).toBe(1);
    expect(currentConnection.closeCalls).toBe(0);
    expect(session.snapshot()).toEqual({
      connectionName: "Second cluster",
      state: "connected",
    });
  });

  it("reports a current connection failure without retaining an active connection", async () => {
    const failure = new Error("metadata failed");
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.reject(failure));
    const session = new KafkaApplicationSession(port);
    const snapshots: unknown[] = [];
    session.subscribe((snapshot) => {
      snapshots.push(snapshot);
    });

    await expect(session.connect(firstConnection)).rejects.toBe(failure);

    expect(session.snapshot()).toEqual({
      connectionName: "First cluster",
      failure,
      state: "failed",
    });
    expect(snapshots).toEqual([
      { connectionName: "First cluster", state: "connecting" },
      { connectionName: "First cluster", failure, state: "failed" },
    ]);
  });

  it("publishes Disconnecting until the active Kafka resource closes", async () => {
    const activeConnection = new RecordingConnection(false);
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    const disconnecting = session.disconnect();
    expect(session.snapshot()).toEqual({
      connectionName: "First cluster",
      state: "disconnecting",
    });
    expect(activeConnection.closeCalls).toBe(1);

    activeConnection.closeResult.resolve();
    await disconnecting;
    expect(session.snapshot()).toEqual({
      connectionName: null,
      state: "disconnected",
    });
  });

  it("cancels an in-flight connection when the user disconnects without publishing a late failure", async () => {
    const port = new RecordingConnectionPort();
    port.openOperations.push(
      (_connection, signal) =>
        new Promise<KafkaActiveConnection>((_resolve, reject) => {
          signal.addEventListener(
            "abort",
            () => {
              reject(new Error("port operation aborted"));
            },
            { once: true },
          );
        }),
    );
    const session = new KafkaApplicationSession(port);
    const snapshots: unknown[] = [];
    session.subscribe((snapshot) => {
      snapshots.push(snapshot);
    });

    const connecting = session.connect(firstConnection);
    await session.disconnect();

    await expect(connecting).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    expect(session.snapshot()).toEqual({
      connectionName: null,
      state: "disconnected",
    });
    expect(snapshots).toEqual([
      { connectionName: "First cluster", state: "connecting" },
      { connectionName: "First cluster", state: "disconnecting" },
      { connectionName: null, state: "disconnected" },
    ]);
  });

  it("shutdown cancels temporary tests, closes the active connection, and rejects new work", async () => {
    const activeConnection = new RecordingConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    port.testOperations.push(
      (_connection, signal) =>
        new Promise<KafkaConnectionTestResult>((_resolve, reject) => {
          signal.addEventListener(
            "abort",
            () => {
              reject(new Error("test operation aborted"));
            },
            { once: true },
          );
        }),
    );
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const temporaryTest = session.testConnection(secondConnection);

    await session.shutdown();

    await expect(temporaryTest).rejects.toThrow("test operation aborted");
    expect(port.testCalls[0]?.signal.aborted).toBe(true);
    expect(activeConnection.closeCalls).toBe(1);
    expect(session.snapshot()).toEqual({
      connectionName: null,
      state: "disconnected",
    });
    await expect(session.connect(secondConnection)).rejects.toThrow(
      "Kafka application session is shut down.",
    );
  });
});

describe("Kafka application topic discovery", () => {
  it("lists topics only through the confirmed active connection", async () => {
    const activeConnection = new RecordingConnection();
    activeConnection.listOperations.push(() => Promise.resolve(["test", "audit.events"]));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    await expect(session.listTopics()).resolves.toEqual(["test", "audit.events"]);

    expect(activeConnection.listCalls).toHaveLength(1);
    expect(activeConnection.listCalls[0]?.aborted).toBe(false);
  });

  it("rejects topic discovery when no connection is active", async () => {
    const session = new KafkaApplicationSession(new RecordingConnectionPort());

    await expect(session.listTopics()).rejects.toThrow(
      "Connect to a Kafka cluster before listing topics.",
    );
  });

  it("propagates an authorization failure from the active connection", async () => {
    const denied = new Error("Kafka denied metadata access.");
    const activeConnection = new RecordingConnection();
    activeConnection.listOperations.push(() => Promise.reject(denied));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    await expect(session.listTopics()).rejects.toBe(denied);
  });

  it("aborts and rejects a topic response superseded by a newer connection", async () => {
    const obsoleteTopics = deferred<readonly string[]>();
    const firstActiveConnection = new RecordingConnection();
    firstActiveConnection.listOperations.push(() => obsoleteTopics.promise);
    const secondActiveConnection = new RecordingConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(
      () => Promise.resolve(firstActiveConnection),
      () => Promise.resolve(secondActiveConnection),
    );
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    const obsoleteRequest = session.listTopics();
    const replacement = session.connect(secondConnection);

    expect(firstActiveConnection.listCalls[0]?.aborted).toBe(true);
    obsoleteTopics.resolve(["obsolete.topic"]);
    await expect(obsoleteRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await replacement;
    expect(session.snapshot()).toEqual({
      connectionName: "Second cluster",
      state: "connected",
    });
  });

  it("aborts an incomplete topic refresh when a newer refresh starts", async () => {
    const obsoleteTopics = deferred<readonly string[]>();
    const currentTopics = deferred<readonly string[]>();
    const activeConnection = new RecordingConnection();
    activeConnection.listOperations.push(
      () => obsoleteTopics.promise,
      () => currentTopics.promise,
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    const obsoleteRequest = session.listTopics();
    const currentRequest = session.listTopics();

    expect(activeConnection.listCalls[0]?.aborted).toBe(true);
    expect(activeConnection.listCalls[1]?.aborted).toBe(false);
    obsoleteTopics.resolve(["obsolete.topic"]);
    currentTopics.resolve(["current.topic"]);
    await expect(obsoleteRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(currentRequest).resolves.toEqual(["current.topic"]);
  });
});

const consumerGroupInventory: KafkaConsumerGroupInventory = {
  groups: [
    {
      groupType: "classic",
      id: "orders-worker",
      protocolType: "consumer",
      state: "stable",
    },
  ],
  omittedGroups: 0,
};

const consumerGroupDetails: KafkaConsumerGroupDetails = {
  id: "orders-worker",
  members: [],
  offsets: [
    {
      committedOffset: "8",
      endOffset: "10",
      lag: "2",
      partition: 0,
      topic: "orders.events",
    },
  ],
  omittedAssignments: 0,
  omittedMembers: 0,
  omittedOffsets: 0,
  protocol: "range",
  protocolType: "consumer",
  state: "stable",
};

describe("Kafka application consumer-group discovery", () => {
  it("loads inventory and selected detail through the confirmed active connection", async () => {
    const activeConnection = new RecordingConnection();
    activeConnection.consumerGroupListOperations.push(() =>
      Promise.resolve(consumerGroupInventory),
    );
    activeConnection.consumerGroupDetailOperations.push(() =>
      Promise.resolve(consumerGroupDetails),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    await expect(session.listConsumerGroups()).resolves.toEqual(consumerGroupInventory);
    await expect(session.describeConsumerGroup("orders-worker")).resolves.toEqual(
      consumerGroupDetails,
    );
    expect(activeConnection.consumerGroupDetailCalls[0]?.groupId).toBe("orders-worker");
  });

  it("aborts and rejects inventory superseded by a newer inventory request", async () => {
    const obsolete = deferred<KafkaConsumerGroupInventory>();
    const activeConnection = new RecordingConnection();
    activeConnection.consumerGroupListOperations.push(
      () => obsolete.promise,
      () => Promise.resolve(consumerGroupInventory),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    const obsoleteRequest = session.listConsumerGroups();
    const currentRequest = session.listConsumerGroups();

    expect(activeConnection.consumerGroupListCalls[0]?.aborted).toBe(true);
    obsolete.resolve({ groups: [], omittedGroups: 0 });
    await expect(obsoleteRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(currentRequest).resolves.toEqual(consumerGroupInventory);
  });

  it("aborts and rejects detail superseded by a newly selected group", async () => {
    const obsolete = deferred<KafkaConsumerGroupDetails>();
    const nextDetails = { ...consumerGroupDetails, id: "payments-worker" };
    const activeConnection = new RecordingConnection();
    activeConnection.consumerGroupDetailOperations.push(
      () => obsolete.promise,
      () => Promise.resolve(nextDetails),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    const obsoleteRequest = session.describeConsumerGroup("orders-worker");
    const currentRequest = session.describeConsumerGroup("payments-worker");

    expect(activeConnection.consumerGroupDetailCalls[0]?.signal?.aborted).toBe(true);
    obsolete.resolve(consumerGroupDetails);
    await expect(obsoleteRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(currentRequest).resolves.toEqual(nextDetails);
  });

  it("cancels both consumer-group request classes when the connection changes", async () => {
    const inventory = deferred<KafkaConsumerGroupInventory>();
    const detail = deferred<KafkaConsumerGroupDetails>();
    const firstActiveConnection = new RecordingConnection();
    firstActiveConnection.consumerGroupListOperations.push(() => inventory.promise);
    firstActiveConnection.consumerGroupDetailOperations.push(() => detail.promise);
    const secondActiveConnection = new RecordingConnection();
    const port = new RecordingConnectionPort();
    port.openOperations.push(
      () => Promise.resolve(firstActiveConnection),
      () => Promise.resolve(secondActiveConnection),
    );
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    const inventoryRequest = session.listConsumerGroups();
    const detailRequest = session.describeConsumerGroup("orders-worker");
    const replacement = session.connect(secondConnection);

    expect(firstActiveConnection.consumerGroupListCalls[0]?.aborted).toBe(true);
    expect(firstActiveConnection.consumerGroupDetailCalls[0]?.signal?.aborted).toBe(true);
    inventory.resolve(consumerGroupInventory);
    detail.resolve(consumerGroupDetails);
    await expect(inventoryRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(detailRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await replacement;
  });

  it("cancels and awaits both consumer-group request classes during disconnect", async () => {
    const inventory = deferred<KafkaConsumerGroupInventory>();
    const detail = deferred<KafkaConsumerGroupDetails>();
    const activeConnection = new RecordingConnection();
    activeConnection.consumerGroupListOperations.push(() => inventory.promise);
    activeConnection.consumerGroupDetailOperations.push(() => detail.promise);
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const inventoryRequest = session.listConsumerGroups();
    const detailRequest = session.describeConsumerGroup("orders-worker");

    const disconnect = session.disconnect();

    expect(activeConnection.consumerGroupListCalls[0]?.aborted).toBe(true);
    expect(activeConnection.consumerGroupDetailCalls[0]?.signal?.aborted).toBe(true);
    inventory.resolve(consumerGroupInventory);
    detail.resolve(consumerGroupDetails);
    await expect(inventoryRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(detailRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await disconnect;
  });

  it("cancels and awaits both consumer-group request classes during shutdown", async () => {
    const inventory = deferred<KafkaConsumerGroupInventory>();
    const detail = deferred<KafkaConsumerGroupDetails>();
    const activeConnection = new RecordingConnection();
    activeConnection.consumerGroupListOperations.push(() => inventory.promise);
    activeConnection.consumerGroupDetailOperations.push(() => detail.promise);
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    const inventoryRequest = session.listConsumerGroups();
    const detailRequest = session.describeConsumerGroup("orders-worker");

    const shutdown = session.shutdown();

    expect(activeConnection.consumerGroupListCalls[0]?.aborted).toBe(true);
    expect(activeConnection.consumerGroupDetailCalls[0]?.signal?.aborted).toBe(true);
    inventory.resolve(consumerGroupInventory);
    detail.resolve(consumerGroupDetails);
    await expect(inventoryRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await expect(detailRequest).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await shutdown;
  });

  it("rejects consumer-group reads without an active connection", async () => {
    const session = new KafkaApplicationSession(new RecordingConnectionPort());

    await expect(session.listConsumerGroups()).rejects.toThrow(
      "Connect to a Kafka cluster before listing consumer groups.",
    );
    await expect(session.describeConsumerGroup("orders-worker")).rejects.toThrow(
      "Connect to a Kafka cluster before reading a consumer group.",
    );
  });
});

afterEach(() => {
  vi.useRealTimers();
});

describe("Kafka application message consumption", () => {
  it("delivers records from one confirmed active connection", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    const received: KafkaMessage[] = [];
    await session.connect(firstConnection);

    const request = tailRequest();
    await session.startConsumption(request, {
      onComplete: () => undefined,
      onEmpty: () => undefined,
      onFailure: () => undefined,
      onMessage: (record) => {
        received.push(record);
      },
    });
    stream.push(message("1"));
    await settleAsyncIteration();

    expect(received).toEqual([message("1")]);
    expect(activeConnection.messageStreamCalls[0]).toMatchObject({ request });
  });

  it("acknowledges stop only after closing the stream and suppresses later records", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    const received: KafkaMessage[] = [];
    await session.connect(firstConnection);
    await session.startConsumption(tailRequest(), {
      onComplete: () => undefined,
      onEmpty: () => undefined,
      onFailure: () => undefined,
      onMessage: (record) => {
        received.push(record);
      },
    });

    await session.stopConsumption();
    stream.push(message("late"));
    await settleAsyncIteration();

    expect(stream.closeCalls).toBe(1);
    expect(received).toEqual([]);
  });

  it("closes and supersedes the prior stream when the selected topic changes", async () => {
    const firstStream = new ControlledMessageStream();
    const secondStream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(
      () => Promise.resolve(firstStream),
      () => Promise.resolve(secondStream),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    const received: KafkaMessage[] = [];
    const observer = {
      onComplete: (): undefined => undefined,
      onEmpty: (): undefined => undefined,
      onFailure: (): undefined => undefined,
      onMessage: (record: KafkaMessage): void => {
        received.push(record);
      },
    };
    await session.connect(firstConnection);
    await session.startConsumption(tailRequest("first.topic"), observer);

    await session.startConsumption(tailRequest("second.topic"), observer);
    firstStream.push(message("obsolete", "first.topic"));
    secondStream.push(message("current", "second.topic"));
    await settleAsyncIteration();

    expect(firstStream.closeCalls).toBe(1);
    expect(received).toEqual([message("current", "second.topic")]);
  });

  it("reports an empty observation without treating it as a failure", async () => {
    vi.useFakeTimers();
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    const observations: string[] = [];
    await session.connect(firstConnection);
    await session.startConsumption(tailRequest(), {
      onComplete: () => {
        observations.push("complete");
      },
      onEmpty: () => {
        observations.push("empty");
      },
      onFailure: () => {
        observations.push("failed");
      },
      onMessage: () => {
        observations.push("message");
      },
    });

    await vi.advanceTimersByTimeAsync(1_000);

    expect(observations).toEqual(["empty"]);
    expect(stream.closeCalls).toBe(0);
  });

  it("reports an active stream failure and closes its resources", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    const failures: unknown[] = [];
    await session.connect(firstConnection);
    await session.startConsumption(tailRequest(), {
      onComplete: () => undefined,
      onEmpty: () => undefined,
      onFailure: (error) => {
        failures.push(error);
      },
      onMessage: () => undefined,
    });
    const failure = new Error("consumer failed");

    stream.fail(failure);
    await settleAsyncIteration();

    expect(failures).toEqual([failure]);
    expect(stream.closeCalls).toBe(1);
  });

  it("cancels and closes consumption before disconnect completes", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);
    await session.startConsumption(tailRequest(), {
      onComplete: () => undefined,
      onEmpty: () => undefined,
      onFailure: () => undefined,
      onMessage: () => undefined,
    });

    await session.disconnect();

    expect(activeConnection.messageStreamCalls[0]?.signal.aborted).toBe(true);
    expect(stream.closeCalls).toBe(1);
    expect(activeConnection.closeCalls).toBe(1);
  });

  it("reports finite completion after delivering a Newest N snapshot", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    const observations: string[] = [];
    await session.connect(firstConnection);

    await session.startConsumption(newestRequest(), {
      onComplete: () => {
        observations.push("complete");
      },
      onEmpty: () => {
        observations.push("empty");
      },
      onFailure: () => {
        observations.push("failed");
      },
      onMessage: () => {
        observations.push("message");
      },
    });
    stream.push(message("1"));
    stream.end();
    await vi.waitFor(() => {
      expect(observations).toEqual(["message", "complete"]);
    });

    expect(stream.closeCalls).toBe(1);
  });

  it("does not use the live empty timer for a finite empty snapshot", async () => {
    vi.useFakeTimers();
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    const observations: string[] = [];
    await session.connect(firstConnection);

    await session.startConsumption(newestRequest(), {
      onComplete: () => {
        observations.push("complete");
      },
      onEmpty: () => {
        observations.push("empty");
      },
      onFailure: () => {
        observations.push("failed");
      },
      onMessage: () => {
        observations.push("message");
      },
    });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(observations).toEqual([]);

    stream.end();
    await settleAsyncIteration();
    expect(observations).toEqual(["complete"]);
  });
});

describe("Kafka application transform log reads", () => {
  it("filters other transform records before returning a bounded result and closes the stream", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    const reading = session.loadTransformLogs("mask-orders");
    stream.push({
      ...message("1", "_redpanda.transform_logs"),
      key: "other-transform",
      payload: '{"level":"error","message":"must not cross the host boundary"}',
      preview: '{"level":"error","message":"must not cross the host boundary"}',
    });
    stream.push({
      ...message("2", "_redpanda.transform_logs"),
      key: "mask-orders",
      payload:
        '{"body":{"stringValue":"processed order"},"severityNumber":9,"timeUnixNano":"1784991600123000000"}',
      preview:
        '{"body":{"stringValue":"processed order"},"severityNumber":9,"timeUnixNano":"1784991600123000000"}',
    });
    stream.end();

    await expect(reading).resolves.toEqual({
      logs: [
        {
          level: "info",
          message: "processed order",
          offset: "2",
          partition: 0,
          timestamp: "2026-07-25T15:00:00.123Z",
        },
      ],
      omittedLogs: 0,
    });
    expect(activeConnection.messageStreamCalls[0]?.request).toMatchObject({
      mode: "newest",
      topic: "_redpanda.transform_logs",
    });
    expect(stream.closeCalls).toBe(1);
  });

  it("does not cancel an unrelated topic-configuration operation", async () => {
    const configuration = deferred<void>();
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingConnection();
    activeConnection.alterOperations.push(() => configuration.promise);
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const session = new KafkaApplicationSession(port);
    await session.connect(firstConnection);

    const changing = session.alterTopicConfiguration(
      "orders.events",
      [{ isSensitive: false, name: "retention.ms", value: "60000" }],
      true,
    );
    const reading = session.loadTransformLogs("mask-orders");
    stream.end();

    await expect(reading).resolves.toEqual({ logs: [], omittedLogs: 0 });
    expect(activeConnection.alterCalls[0]?.aborted).toBe(false);
    configuration.resolve();
    await expect(changing).resolves.toBeUndefined();
  });
});
