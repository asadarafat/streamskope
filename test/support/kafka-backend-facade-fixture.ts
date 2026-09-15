import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  type HostCommand,
  type KafkaConsumerGroupDetails,
  type KafkaFetchRequest,
  type KafkaMessage,
  type SecureConnectionInput,
} from "../../src/kafka/contracts";
import {
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaRuleStore,
  InMemoryKafkaTopicConfigurationHistoryStore,
  KafkaApplicationSession,
  KafkaConnectionTemplateService,
  KafkaLiveRuleRuntime,
  KafkaOperationalPreferenceService,
  KafkaProfileService,
  KafkaRuleService,
  KafkaTopicConfigurationService,
  type KafkaActiveConnection,
  type KafkaConnectionPort,
  type KafkaConnectionTestResult,
  type KafkaClusterServiceContext,
  type KafkaConsumerGroupInventory,
  type KafkaMessageStream,
  type SchemaRegistryPort,
  type KafkaTrustAcquisitionServicePort,
} from "../../src/kafka/application";
import { KafkaBackendFacade } from "../../src/kafka/facade";
import { StreamSkopeKafkaRuleEvaluator } from "../../src/kafka/engine";

const connection: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "Local aio",
  oauth: {
    clientId: "admin",
    clientSecret: "fixture-secret",
    scope: "kafka",
    tokenEndpoint: "http://localhost:15000/token",
  },
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

export class RecordingActiveConnection implements KafkaActiveConnection {
  closeCalls = 0;
  readonly clusterServiceContexts: Partial<
    Record<"redpandaAdmin" | "schemaRegistry", KafkaClusterServiceContext>
  > = {};
  readonly listOperations: Array<(signal: AbortSignal) => Promise<readonly string[]>> = [];
  readonly consumerGroupListOperations: Array<
    (signal: AbortSignal) => Promise<KafkaConsumerGroupInventory>
  > = [];
  readonly consumerGroupDetailOperations: Array<
    (groupId: string, signal: AbortSignal) => Promise<KafkaConsumerGroupDetails>
  > = [];
  readonly messageStreamOperations: Array<
    (request: KafkaFetchRequest, signal: AbortSignal) => Promise<KafkaMessageStream>
  > = [];

  alterTopicConfiguration(): Promise<void> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  clusterServiceContext(
    service: "redpandaAdmin" | "schemaRegistry",
  ): KafkaClusterServiceContext | null {
    return this.clusterServiceContexts[service] ?? null;
  }

  describeBrokerConfiguration(): never {
    throw new Error("No broker-configuration operation was configured.");
  }

  describeClusterMetadata(): never {
    throw new Error("No cluster-metadata operation was configured.");
  }

  describeConsumerGroup(groupId: string, signal?: AbortSignal): Promise<KafkaConsumerGroupDetails> {
    const operation = this.consumerGroupDetailOperations.shift();
    return operation === undefined
      ? Promise.reject(new Error("No consumer-group detail operation was configured."))
      : operation(groupId, signal ?? new AbortController().signal);
  }

  listConsumerGroups(signal?: AbortSignal): Promise<KafkaConsumerGroupInventory> {
    const operation = this.consumerGroupListOperations.shift();
    return operation === undefined
      ? Promise.resolve({ groups: [], omittedGroups: 0 })
      : operation(signal ?? new AbortController().signal);
  }

  listTopics(signal?: AbortSignal): Promise<readonly string[]> {
    const operation = this.listOperations.shift();
    return operation === undefined
      ? Promise.resolve([])
      : operation(signal ?? new AbortController().signal);
  }

  describeTopicConfiguration(): Promise<readonly never[]> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }

  openMessageStream(request: KafkaFetchRequest, signal: AbortSignal): Promise<KafkaMessageStream> {
    const operation = this.messageStreamOperations.shift();
    return operation === undefined
      ? Promise.reject(new Error("No message-stream operation was configured."))
      : operation(request, signal);
  }
}

type StreamResult =
  | { readonly kind: "message"; readonly message: KafkaMessage }
  | { readonly error: Error; readonly kind: "error" }
  | { readonly kind: "end" };

export class ControlledMessageStream implements KafkaMessageStream {
  closeCalls = 0;
  deliveredMessages = 0;
  private closed = false;
  private readonly queued: StreamResult[] = [];
  private readonly waiting: Array<(result: StreamResult) => void> = [];

  close(): Promise<void> {
    this.closeCalls += 1;
    this.end();
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
      this.deliveredMessages += 1;
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

type OpenOperation = (
  input: SecureConnectionInput,
  signal: AbortSignal,
) => Promise<KafkaActiveConnection>;

type TestOperation = (
  input: SecureConnectionInput,
  signal: AbortSignal,
) => Promise<KafkaConnectionTestResult>;

export class RecordingConnectionPort implements KafkaConnectionPort {
  readonly openOperations: OpenOperation[] = [];
  readonly testOperations: TestOperation[] = [];

  openConnection(
    input: SecureConnectionInput,
    signal: AbortSignal,
  ): Promise<KafkaActiveConnection> {
    const operation = this.openOperations.shift();
    if (operation === undefined) {
      throw new Error("No open operation was configured.");
    }
    return operation(input, signal);
  }

  testConnection(
    input: SecureConnectionInput,
    signal: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    const operation = this.testOperations.shift();
    if (operation === undefined) {
      throw new Error("No test operation was configured.");
    }
    return operation(input, signal);
  }
}

export function command(
  name:
    | "connection.connect"
    | "connection.disconnect"
    | "connection.test"
    | "messages.start"
    | "messages.stop"
    | "topics.list",
  id: string,
): HostCommand {
  if (name === "messages.start") {
    return {
      command: name,
      id,
      payload: tailRequest(),
      version: HOST_PROTOCOL_VERSION,
    };
  }
  return name === "connection.disconnect" || name === "messages.stop" || name === "topics.list"
    ? {
        command: name,
        id,
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }
    : {
        command: name,
        id,
        payload: connection,
        version: HOST_PROTOCOL_VERSION,
      };
}

export function fetchCommand(request: KafkaFetchRequest, id: string): HostCommand {
  return {
    command: "messages.start",
    id,
    payload: request,
    version: HOST_PROTOCOL_VERSION,
  };
}

export function tailRequest(topic = "test"): KafkaFetchRequest {
  return {
    maxMessages: 1_000,
    mode: "tail",
    topic,
  };
}

export function createFacade(
  port: RecordingConnectionPort,
  scheduleMessageFlush?: (flush: () => void, delayMs: number) => (() => void) | void,
  monotonicNow?: () => number,
  trustAcquisitions?: KafkaTrustAcquisitionServicePort,
  preferences?: KafkaOperationalPreferenceService,
  schemaRegistry?: SchemaRegistryPort,
): KafkaBackendFacade {
  let correlation = 0;
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(
    new InMemoryKafkaRuleStore({ durability: "session", state: "ready" }),
    evaluator,
  );
  const session = new KafkaApplicationSession(port);
  return new KafkaBackendFacade(
    session,
    new KafkaProfileService(
      new InMemoryKafkaProfileStore({
        durability: "session",
        protection: "memory",
        state: "ready",
      }),
      {
        decode: () => Promise.reject(new Error("No profile decode was requested.")),
      },
    ),
    new KafkaConnectionTemplateService(
      new InMemoryKafkaConnectionTemplateStore({
        durability: "session",
        state: "ready",
      }),
    ),
    rules,
    new KafkaLiveRuleRuntime(rules, evaluator),
    new KafkaTopicConfigurationService(
      session,
      new InMemoryKafkaTopicConfigurationHistoryStore({
        durability: "session",
        state: "ready",
      }),
    ),
    {
      createCorrelationId: () => `correlation-${++correlation}`,
      now: () => new Date("2026-07-25T13:00:00.000Z"),
      ...(monotonicNow === undefined ? {} : { monotonicNow }),
      ...(preferences === undefined ? {} : { preferences }),
      ...(schemaRegistry === undefined ? {} : { schemaRegistry }),
      ...(scheduleMessageFlush === undefined ? {} : { scheduleMessageFlush }),
      ...(trustAcquisitions === undefined ? {} : { trustAcquisitions }),
    },
  );
}

export function message(id: string, payload = "value"): KafkaMessage {
  return {
    headers: {},
    id,
    key: "key",
    offset: id,
    originalByteSize: Buffer.byteLength(payload) + 3,
    partition: 0,
    payload,
    preview: payload.slice(0, KAFKA_MESSAGE_LIMITS.previewBytes),
    timestamp: "2026-07-25T15:00:00.000Z",
    topic: "test",
    truncated: false,
  };
}

export async function settleAsyncIteration(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}
