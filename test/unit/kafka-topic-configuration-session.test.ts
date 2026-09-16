import { describe, expect, it } from "vitest";

import type {
  KafkaFetchRequest,
  KafkaMessage,
  KafkaTopicConfigurationChange,
  KafkaTopicConfigurationEntry,
  SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import {
  ConnectionAttemptSupersededError,
  KafkaApplicationSession,
  type KafkaActiveConnection,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaMessageStream,
} from "../../src/features/kafka/application";

const connectionInput: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "Local aio",
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

const configurationEntry: KafkaTopicConfigurationEntry = {
  documentation: null,
  isDefault: false,
  isSensitive: false,
  name: "retention.ms",
  readOnly: false,
  source: "topic",
  synonyms: [],
  type: "long",
  value: "86400000",
};

const changes: readonly KafkaTopicConfigurationChange[] = [
  {
    isSensitive: false,
    name: "retention.ms",
    value: "604800000",
  },
];

class EmptyMessageStream implements KafkaMessageStream {
  close(): Promise<void> {
    return Promise.resolve();
  }

  [Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    return {
      next: (): Promise<IteratorResult<KafkaMessage>> =>
        Promise.resolve({ done: true, value: undefined }),
    };
  }
}

class RecordingConnection implements KafkaActiveConnection {
  readonly alterCalls: Array<{
    readonly changes: readonly KafkaTopicConfigurationChange[];
    readonly signal: AbortSignal | undefined;
    readonly topic: string;
    readonly validateOnly: boolean;
  }> = [];
  readonly describeCalls: Array<{
    readonly signal: AbortSignal | undefined;
    readonly topic: string;
  }> = [];
  describeOperation: (
    topic: string,
    signal: AbortSignal | undefined,
  ) => Promise<readonly KafkaTopicConfigurationEntry[]> = () =>
    Promise.resolve([configurationEntry]);

  alterTopicConfiguration(
    topic: string,
    submittedChanges: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
    signal?: AbortSignal,
  ): Promise<void> {
    this.alterCalls.push({ changes: submittedChanges, signal, topic, validateOnly });
    return Promise.resolve();
  }

  close(): Promise<void> {
    return Promise.resolve();
  }

  describeBrokerConfiguration(): never {
    throw new Error("No broker-configuration operation was requested.");
  }

  describeClusterMetadata(): never {
    throw new Error("No cluster-metadata operation was requested.");
  }

  describeTopicConfiguration(
    topic: string,
    signal?: AbortSignal,
  ): Promise<readonly KafkaTopicConfigurationEntry[]> {
    this.describeCalls.push({ signal, topic });
    return this.describeOperation(topic, signal);
  }

  listTopics(): Promise<readonly string[]> {
    return Promise.resolve(["orders.events"]);
  }

  openMessageStream(
    _request: KafkaFetchRequest,
    _signal: AbortSignal,
  ): Promise<KafkaMessageStream> {
    return Promise.resolve(new EmptyMessageStream());
  }
}

class ImmediateConnectionPort implements KafkaConnectionPort {
  constructor(private readonly connection: RecordingConnection) {}

  openConnection(
    _connection: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaActiveConnection> {
    return Promise.resolve(this.connection);
  }

  testConnection(
    _connection: SecureConnectionInput,
    _signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    return Promise.resolve({ checks: ["metadata"], topicCount: 1 });
  }
}

async function connectedSession(connection: RecordingConnection): Promise<KafkaApplicationSession> {
  const session = new KafkaApplicationSession(new ImmediateConnectionPort(connection));
  await session.connect(connectionInput);
  return session;
}

describe("Kafka application topic-configuration lifecycle", () => {
  it("forwards describe and validate/apply through the active connection", async () => {
    const connection = new RecordingConnection();
    const session = await connectedSession(connection);

    await expect(session.describeTopicConfiguration("orders.events")).resolves.toEqual([
      configurationEntry,
    ]);
    await session.alterTopicConfiguration("orders.events", changes, true);
    await session.alterTopicConfiguration("orders.events", changes, false);

    expect(connection.describeCalls).toHaveLength(1);
    expect(
      connection.alterCalls.map(({ topic, validateOnly }) => ({ topic, validateOnly })),
    ).toEqual([
      { topic: "orders.events", validateOnly: true },
      { topic: "orders.events", validateOnly: false },
    ]);
    expect(connection.alterCalls.every((call) => call.changes === changes)).toBe(true);
  });

  it("cancels an obsolete describe and prevents its late result after disconnect", async () => {
    const connection = new RecordingConnection();
    let resolveDescribe: ((entries: readonly KafkaTopicConfigurationEntry[]) => void) | undefined;
    connection.describeOperation = (
      _topic,
      signal,
    ): Promise<readonly KafkaTopicConfigurationEntry[]> =>
      new Promise((resolve, reject) => {
        resolveDescribe = resolve;
        signal?.addEventListener(
          "abort",
          () => {
            reject(new DOMException("Aborted", "AbortError"));
          },
          { once: true },
        );
      });
    const session = await connectedSession(connection);

    const pending = session.describeTopicConfiguration("orders.events");
    const disconnect = session.disconnect();

    await expect(pending).rejects.toBeInstanceOf(ConnectionAttemptSupersededError);
    await disconnect;
    expect(connection.describeCalls[0]?.signal?.aborted).toBe(true);
    resolveDescribe?.([configurationEntry]);
    expect(session.snapshot()).toMatchObject({ state: "disconnected" });
  });

  it("rejects configuration access without an active connection", async () => {
    const session = new KafkaApplicationSession(
      new ImmediateConnectionPort(new RecordingConnection()),
    );

    await expect(session.describeTopicConfiguration("orders.events")).rejects.toThrow(
      "Connect to a Kafka cluster before reading topic configuration.",
    );
    await expect(session.alterTopicConfiguration("orders.events", changes, true)).rejects.toThrow(
      "Connect to a Kafka cluster before changing topic configuration.",
    );
  });
});
