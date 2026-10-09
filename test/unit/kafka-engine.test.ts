import { createServer, type Server } from "node:http";

import { afterEach, describe, expect, it } from "vitest";

import type {
  KafkaFetchRequest,
  KafkaMessage,
  KafkaReadCoverage,
  KafkaConsumerGroupDetails,
  OAuthConnectionInput,
  SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import { KAFKA_MESSAGE_LIMITS, utf8ByteLength } from "../../src/features/kafka/contracts";
import { KafkaEngineFailure } from "../../src/features/kafka/engine/failure";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { RedpandaTransformHttpAdapter } from "../../src/features/kafka/engine/redpanda-transform-http";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine/engine";
import {
  KafkaReadCheckpointError,
  type KafkaReadCheckpoint,
} from "../../src/features/kafka/application/read-checkpoint";
import type {
  KafkaAdminFactory,
  KafkaAdminInput,
  KafkaAdminPort,
  OAuthToken,
  OAuthTokenRequest,
} from "../../src/features/kafka/engine/types";
import type { KafkaClusterServiceContext } from "../../src/features/kafka/application";
import type {
  BoundedJsonHttpPort,
  BoundedJsonHttpRequest,
  BoundedJsonHttpResponse,
} from "../../src/features/kafka/engine/bounded-json-http";
import { probeKafkaNetwork } from "../../src/features/kafka/engine/latency-network";

const oauthConnection: OAuthConnectionInput = {
  clientId: "admin",
  clientSecret: "fixture-secret",
  scope: "kafka",
  tokenEndpoint: "http://localhost:15000/token",
};

const connection = {
  brokers: ["localhost:19093"],
  name: "Local aio",
  oauth: oauthConnection,
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
} as const satisfies SecureConnectionInput;

function tailRequest(topic = "test"): KafkaFetchRequest {
  return {
    maxMessages: 1_000,
    mode: "tail",
    topic,
  };
}

function codedError(code: string, message: string): Error {
  return Object.assign(new Error(message), { code });
}

class RecordingAdmin implements KafkaAdminPort {
  closeCalls = 0;
  consumerGroupDetailResult: (() => Promise<KafkaConsumerGroupDetails>) | undefined;

  constructor(
    private readonly listTopicsResult: readonly string[] | (() => Promise<readonly string[]>),
  ) {}

  alterTopicConfiguration(): Promise<void> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  describeBrokerConfiguration(): never {
    throw new Error("No broker-configuration operation was configured.");
  }

  describeClusterMetadata(): never {
    throw new Error("No cluster-metadata operation was configured.");
  }

  describeConsumerGroup(): Promise<KafkaConsumerGroupDetails> {
    if (this.consumerGroupDetailResult === undefined) {
      return Promise.reject(new Error("No consumer-group detail operation was configured."));
    }
    return this.consumerGroupDetailResult();
  }

  listConsumerGroups(): Promise<{ readonly groups: []; readonly omittedGroups: 0 }> {
    return Promise.resolve({ groups: [], omittedGroups: 0 });
  }

  listTopics(): Promise<readonly string[]> {
    return typeof this.listTopicsResult === "function"
      ? this.listTopicsResult()
      : Promise.resolve(this.listTopicsResult);
  }

  describeTopicConfiguration(): Promise<readonly never[]> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }
}

class RecordingAdminFactory implements KafkaAdminFactory {
  readonly inputs: KafkaAdminInput[] = [];

  constructor(private readonly admin: RecordingAdmin) {}

  create(input: KafkaAdminInput): KafkaAdminPort {
    this.inputs.push(input);
    return this.admin;
  }
}

class RecordingJsonHttp implements BoundedJsonHttpPort {
  readonly requests: BoundedJsonHttpRequest[] = [];

  constructor(private readonly responses: readonly BoundedJsonHttpResponse[]) {}

  request(input: BoundedJsonHttpRequest): Promise<BoundedJsonHttpResponse> {
    this.requests.push(input);
    return Promise.resolve(this.responses[this.requests.length - 1] ?? { body: null, status: 500 });
  }
}

interface RawMessage {
  readonly headers: ReadonlyMap<Buffer, Buffer>;
  readonly key?: Buffer;
  readonly offset: bigint;
  readonly partition: number;
  readonly timestamp: bigint;
  readonly topic: string;
  readonly value?: Buffer;
}

class RecordingRawMessageStream implements AsyncIterable<RawMessage> {
  closeCalls = 0;
  readonly acknowledgements: unknown[] = [];
  checkpointValue: KafkaReadCheckpoint | undefined;
  readonly coverageListeners = new Set<(coverage: KafkaReadCoverage) => void>();

  constructor(private readonly messages: readonly RawMessage[]) {}

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  acknowledge(message: unknown): void {
    this.acknowledgements.push(message);
  }

  checkpoint(): KafkaReadCheckpoint | undefined {
    return this.checkpointValue;
  }

  subscribeCoverage(listener: (coverage: KafkaReadCoverage) => void): () => void {
    this.coverageListeners.add(listener);
    return (): void => {
      this.coverageListeners.delete(listener);
    };
  }

  async *[Symbol.asyncIterator](): AsyncIterator<RawMessage> {
    for (const message of this.messages) {
      yield await Promise.resolve(message);
    }
  }
}

class RecordingConsumerFactory {
  readonly inputs: unknown[] = [];

  constructor(private readonly stream: RecordingRawMessageStream) {}

  open(input: unknown): Promise<RecordingRawMessageStream> {
    this.inputs.push(input);
    return Promise.resolve(this.stream);
  }
}

interface RunningOAuthServer {
  readonly endpoint: string;
  readonly requests: () => number;
  close(): Promise<void>;
}

const runningServers = new Set<RunningOAuthServer>();

async function closeServer(server: Server): Promise<void> {
  if (!server.listening) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

async function startOAuthServer(status: number, body: string): Promise<RunningOAuthServer> {
  let requestCount = 0;
  const server = createServer((_request, response) => {
    requestCount += 1;
    response.statusCode = status;
    response.setHeader("content-type", "application/json");
    response.end(body);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("OAuth test server did not expose a TCP port.");
  }
  const running: RunningOAuthServer = {
    close: async (): Promise<void> => {
      await closeServer(server);
      runningServers.delete(running);
    },
    endpoint: `http://localhost:${address.port}/token`,
    requests: () => requestCount,
  };
  runningServers.add(running);
  return running;
}

afterEach(async () => {
  await Promise.all([...runningServers].map(async (server) => server.close()));
});

describe("StreamSkope Kafka engine connection test", () => {
  it("keeps an established connection open until its owner closes it", async () => {
    const admin = new RecordingAdmin(["test"]);
    const adminFactory = new RecordingAdminFactory(admin);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory,
      requestOAuthToken: (): Promise<OAuthToken> =>
        Promise.resolve({
          expiresAt: Date.now() + 60_000,
          value: "active-token",
        }),
    });

    const activeConnection = await engine.openConnection(connection, new AbortController().signal);

    expect(admin.closeCalls).toBe(0);
    expect(adminFactory.inputs).toHaveLength(1);
    await expect(adminFactory.inputs[0]?.oauthTokenProvider?.()).resolves.toMatchObject({
      value: "active-token",
    });
    await activeConnection.close();
    await activeConnection.close();
    await expect(adminFactory.inputs[0]?.oauthTokenProvider?.()).rejects.toMatchObject({
      name: "AbortError",
    });
    expect(admin.closeCalls).toBe(1);
  });

  it("does not report a closed connection after an owned stream failed to close", async () => {
    const cleanupFailure = new Error("stream cleanup failed");
    class FailingCloseStream extends RecordingRawMessageStream {
      override close(): Promise<void> {
        this.closeCalls += 1;
        return Promise.reject(cleanupFailure);
      }
    }
    const rawStream = new FailingCloseStream([]);
    const admin = new RecordingAdmin(["test"]);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(admin),
      consumerFactory: new RecordingConsumerFactory(rawStream),
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
    });
    const activeConnection = await engine.openConnection(connection, new AbortController().signal);
    const stream = await activeConnection.openMessageStream(
      tailRequest(),
      new AbortController().signal,
    );

    await expect(stream.close()).rejects.toBe(cleanupFailure);
    await expect(activeConnection.close()).rejects.toMatchObject({ errors: [cleanupFailure] });
    await expect(activeConnection.close()).rejects.toMatchObject({ errors: [cleanupFailure] });
    expect(rawStream.closeCalls).toBe(1);
    expect(admin.closeCalls).toBe(1);
  });

  it("forwards owned checkpoints and acknowledges only the raw record behind a delivered projection", async () => {
    const raw: RawMessage = {
      topic: "test",
      partition: 0,
      offset: 1n,
      timestamp: 1000n,
      headers: new Map(),
      value: Buffer.from("visible"),
    };
    const checkpoint: KafkaReadCheckpoint = {
      clusterId: "cluster",
      topicId: "topic",
      partitionCount: 1,
      coverage: {
        reason: "result-limit",
        scannedRecords: 1,
        scannedBytes: 1,
        matchedRecords: 1,
        unavailableRecords: 0,
        partitions: [{ partition: 0, startOffset: "0", endOffset: "3", nextOffset: "1" }],
      },
    };
    const rawStream = new RecordingRawMessageStream([raw]);
    rawStream.checkpointValue = checkpoint;
    const factory = new RecordingConsumerFactory(rawStream);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin(["test"])),
      consumerFactory: factory,
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    try {
      const stream = await active.openMessageStream(
        { mode: "earliest", topic: "test", maxMessages: 1 },
        new AbortController().signal,
        checkpoint,
      );
      expect(factory.inputs[0]).toMatchObject({ checkpoint });
      expect(stream.checkpoint?.()).toBe(checkpoint);
      const progress: KafkaReadCoverage[] = [];
      const unsubscribe = stream.subscribeCoverage?.((coverage) => progress.push(coverage));
      expect(rawStream.coverageListeners.size).toBe(1);
      for (const listener of rawStream.coverageListeners) listener(checkpoint.coverage);
      expect(progress).toEqual([checkpoint.coverage]);
      unsubscribe?.();
      expect(rawStream.coverageListeners.size).toBe(0);
      const result = await stream[Symbol.asyncIterator]().next();
      if (result.done) throw new Error("Expected a fixture projection");
      stream.acknowledge?.({ ...result.value });
      expect(rawStream.acknowledgements).toEqual([]);
      stream.acknowledge?.(result.value);
      stream.acknowledge?.(result.value);
      expect(rawStream.acknowledgements).toEqual([raw]);
    } finally {
      await active.close();
    }
  });

  it("preserves actionable checkpoint invalidation errors instead of a generic broker failure", async () => {
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin(["test"])),
      consumerFactory: {
        open: (): Promise<never> =>
          Promise.reject(new KafkaReadCheckpointError("retention-changed")),
      },
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    try {
      await expect(
        active.openMessageStream(
          { mode: "earliest", topic: "test", maxMessages: 1 },
          new AbortController().signal,
        ),
      ).rejects.toMatchObject({
        code: "VALIDATION",
        stage: "validation",
        retryable: false,
        message: "The remaining captured offsets are no longer retained by Kafka.",
      });
    } finally {
      await active.close();
    }
  });

  it("reuses one OAuth refresh for concurrent authenticated Registry requests", async () => {
    let tokenRequests = 0;
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin(["test"])),
      requestOAuthToken: (): Promise<OAuthToken> => {
        tokenRequests += 1;
        return Promise.resolve(
          tokenRequests === 1
            ? { expiresAt: 0, value: "expired-token" }
            : { expiresAt: Date.now() + 60_000, value: "refreshed-token" },
        );
      },
    });
    const activeConnection = await engine.openConnection(
      {
        ...connection,
        services: {
          schemaRegistry: {
            authentication: "oauth",
            baseUrl: "https://schema.example.test:8081",
          },
        },
      },
      new AbortController().signal,
    );
    const context = activeConnection.clusterServiceContext?.("schemaRegistry");
    if (context === null || context === undefined) {
      throw new Error("The active connection did not expose Schema Registry context.");
    }

    await expect(Promise.all([context.authorization(), context.authorization()])).resolves.toEqual([
      "Bearer refreshed-token",
      "Bearer refreshed-token",
    ]);
    expect(tokenRequests).toBe(2);
    await activeConnection.close();
  });

  it("does not misreport an unknown consumer member as a missing group", async () => {
    const admin = new RecordingAdmin(["test"]);
    admin.consumerGroupDetailResult = (): Promise<KafkaConsumerGroupDetails> =>
      Promise.reject(codedError("UNKNOWN_MEMBER_ID", "The member is not known to the group."));
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(admin),
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
    });
    const activeConnection = await engine.openConnection(connection, new AbortController().signal);
    if (activeConnection.describeConsumerGroup === undefined) {
      throw new Error("The active Kafka connection does not expose consumer-group detail.");
    }

    await expect(activeConnection.describeConsumerGroup("orders-workers")).rejects.toMatchObject({
      code: "INTERNAL",
      stage: "internal",
    });
    await activeConnection.close();
  });

  it("translates raw Kafka records through the active connection", async () => {
    const timestamp = BigInt(Date.parse("2026-07-25T15:00:00.000Z"));
    const rawStream = new RecordingRawMessageStream([
      {
        headers: new Map([[Buffer.from("content-type"), Buffer.from("application/json")]]),
        key: Buffer.from("order-1"),
        offset: 42n,
        partition: 2,
        timestamp,
        topic: "test",
        value: Buffer.from('{"status":"ready"}'),
      },
    ]);
    const consumerFactory = new RecordingConsumerFactory(rawStream);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin(["test"])),
      consumerFactory,
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
    });
    const activeConnection = await engine.openConnection(connection, new AbortController().signal);

    const request = tailRequest();
    const stream = await activeConnection.openMessageStream(request, new AbortController().signal);
    const records = [];
    for await (const record of stream) {
      records.push(record);
    }

    expect(records).toEqual([
      {
        headers: { "content-type": "application/json" },
        id: "test:2:42",
        key: "order-1",
        offset: "42",
        originalByteSize: 25,
        recordByteSize: 53,
        partition: 2,
        payload: '{"status":"ready"}',
        payloadTruncated: false,
        original: {
          state: "complete",
          encoding: "base64",
          key: Buffer.from("order-1").toString("base64"),
          value: Buffer.from('{"status":"ready"}').toString("base64"),
          headers: [
            {
              key: Buffer.from("content-type").toString("base64"),
              value: Buffer.from("application/json").toString("base64"),
            },
          ],
        },
        preview: '{"status":"ready"}',
        timestamp: "2026-07-25T15:00:00.000Z",
        topic: "test",
        truncated: false,
      },
    ]);
    expect(consumerFactory.inputs[0]).toMatchObject({
      brokers: ["localhost:19093"],
      caPem: connection.tls.caPem,
      request,
    });
    await stream.close();
    await activeConnection.close();
    expect(rawStream.closeCalls).toBe(1);
  });

  it("revokes pending record preparation when its stream closes and cannot deliver a stale projection", async () => {
    const rawStream = new RecordingRawMessageStream([
      {
        headers: new Map(),
        offset: 0n,
        partition: 0,
        timestamp: 1n,
        topic: "test",
        value: Buffer.from("{}"),
      },
    ]);
    let entered!: () => void;
    const preparing = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let revoked = false;
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin(["test"])),
      consumerFactory: new RecordingConsumerFactory(rawStream),
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
      prepareRecord: (record, _context, signal): Promise<KafkaMessage> =>
        new Promise((resolve) => {
          signal.addEventListener(
            "abort",
            () => {
              revoked = true;
              resolve(record);
            },
            { once: true },
          );
          entered();
        }),
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    const stream = await active.openMessageStream(tailRequest(), new AbortController().signal);
    const pending = stream[Symbol.asyncIterator]().next();
    const rejected = expect(pending).rejects.toBeInstanceOf(Error);
    await preparing;
    await stream.close();
    await rejected;
    expect(revoked).toBe(true);
    expect(rawStream.closeCalls).toBe(1);
    await active.close();
  });

  it("does not retain an oversized payload as if it were complete", async () => {
    const oversizedPayload = Buffer.alloc(KAFKA_MESSAGE_LIMITS.messageBytes + 1, "a");
    const rawStream = new RecordingRawMessageStream([
      {
        headers: new Map(),
        offset: 7n,
        partition: 0,
        timestamp: BigInt(Date.parse("2026-07-25T15:00:00.000Z")),
        topic: "test",
        value: oversizedPayload,
      },
    ]);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin(["test"])),
      consumerFactory: new RecordingConsumerFactory(rawStream),
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
    });
    const activeConnection = await engine.openConnection(connection, new AbortController().signal);
    const stream = await activeConnection.openMessageStream(
      tailRequest(),
      new AbortController().signal,
    );
    const records = [];
    for await (const record of stream) {
      records.push(record);
    }

    expect(records[0]).toMatchObject({
      id: "test:0:7",
      key: null,
      originalByteSize: KAFKA_MESSAGE_LIMITS.messageBytes + 1,
      payload: null,
      truncated: true,
    });
    expect(records[0]?.preview).toHaveLength(KAFKA_MESSAGE_LIMITS.previewBytes);
    await stream.close();
    await activeConnection.close();
  });

  it("keeps invalid UTF-8 replacement data inside the retained string-byte bounds", async () => {
    const invalidUtf8Payload = Buffer.alloc(KAFKA_MESSAGE_LIMITS.messageBytes, 0xff);
    const rawStream = new RecordingRawMessageStream([
      {
        headers: new Map(),
        offset: 8n,
        partition: 0,
        timestamp: BigInt(Date.parse("2026-07-25T15:00:00.000Z")),
        topic: "test",
        value: invalidUtf8Payload,
      },
    ]);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(new RecordingAdmin(["test"])),
      consumerFactory: new RecordingConsumerFactory(rawStream),
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
    });
    const activeConnection = await engine.openConnection(connection, new AbortController().signal);
    const stream = await activeConnection.openMessageStream(
      tailRequest(),
      new AbortController().signal,
    );
    const records = [];
    for await (const record of stream) {
      records.push(record);
    }

    expect(records[0]?.originalByteSize).toBe(KAFKA_MESSAGE_LIMITS.messageBytes);
    expect(records[0]?.payload === null).toBe(true);
    expect(records[0]?.truncated).toBe(true);
    expect(utf8ByteLength(records[0]?.preview ?? null)).toBeLessThanOrEqual(
      KAFKA_MESSAGE_LIMITS.previewBytes,
    );
    await stream.close();
    await activeConnection.close();
  });

  it("confirms OAuth, TLS, Kafka authentication and metadata then closes the temporary admin", async () => {
    const admin = new RecordingAdmin(["test", "events"]);
    const adminFactory = new RecordingAdminFactory(admin);
    const tokenRequests: OAuthTokenRequest[] = [];
    const engine = new StreamSkopeKafkaEngine({
      adminFactory,
      requestOAuthToken: (request): Promise<OAuthToken> => {
        tokenRequests.push(request);
        return Promise.resolve({
          expiresAt: 1_800_000_000_000,
          value: "access-token",
        });
      },
    });

    await expect(engine.testConnection(connection)).resolves.toEqual({
      checks: ["oauth", "tls", "kafka-authentication", "metadata"],
      topicCount: 2,
    });
    expect(tokenRequests).toHaveLength(1);
    expect(tokenRequests[0]).toMatchObject({
      caPem: connection.tls.caPem,
      clientId: "admin",
      clientSecret: "fixture-secret",
      scope: "kafka",
      tokenEndpoint: "http://localhost:15000/token",
    });
    expect(adminFactory.inputs).toHaveLength(1);
    expect(adminFactory.inputs[0]).toMatchObject({
      brokers: ["localhost:19093"],
      caPem: connection.tls.caPem,
      operationTimeoutMs: 5_000,
    });
    await expect(adminFactory.inputs[0]?.oauthTokenProvider?.()).rejects.toMatchObject({
      name: "AbortError",
    });
    expect(admin.closeCalls).toBe(1);
  });

  it("tests and opens plaintext with OAuth without CA or TLS check evidence", async () => {
    const admin = new RecordingAdmin(["test"]);
    const adminFactory = new RecordingAdminFactory(admin);
    const tokenRequests: OAuthTokenRequest[] = [];
    const engine = new StreamSkopeKafkaEngine({
      adminFactory,
      requestOAuthToken: (request): Promise<OAuthToken> => {
        tokenRequests.push(request);
        return Promise.resolve({ value: "plaintext-token" });
      },
    });
    const plaintext: SecureConnectionInput = {
      brokers: ["localhost:19092"],
      name: "Plaintext OAuth",
      oauth: oauthConnection,
      tls: { enabled: false },
    };

    await expect(engine.testConnection(plaintext)).resolves.toEqual({
      checks: ["oauth", "kafka-authentication", "metadata"],
      topicCount: 1,
    });
    const active = await engine.openConnection(plaintext, new AbortController().signal);
    expect(tokenRequests).toHaveLength(2);
    expect(tokenRequests.every((request) => !Object.hasOwn(request, "caPem"))).toBe(true);
    expect(adminFactory.inputs).toHaveLength(2);
    expect(
      adminFactory.inputs.every(
        (input) => input.tlsEnabled === false && !Object.hasOwn(input, "caPem"),
      ),
    ).toBe(true);
    await active.close();
  });

  it("uses platform HTTP trust when no custom CA is owned and preserves an explicit CA", async () => {
    const server = await startOAuthServer(200, '{"access_token":"bounded-http"}');
    await expect(
      new NodeBoundedJsonHttp().request({
        method: "GET",
        signal: new AbortController().signal,
        url: server.endpoint,
      }),
    ).resolves.toMatchObject({ status: 200 });

    const withoutCustomCa: KafkaClusterServiceContext = {
      authorization: () => Promise.resolve(undefined),
      baseUrl: "https://schema.example.test:8081",
    };
    const schemaHttp = new RecordingJsonHttp([{ body: [], status: 200 }]);
    await new SchemaRegistryHttpAdapter(schemaHttp).listSubjects(
      withoutCustomCa,
      new AbortController().signal,
    );
    expect(schemaHttp.requests[0]).not.toHaveProperty("caPem");

    const withCustomCa: KafkaClusterServiceContext = {
      ...withoutCustomCa,
      baseUrl: "https://admin.example.test:9644",
      caPem: "explicit-service-ca",
    };
    const redpandaHttp = new RecordingJsonHttp([{ body: [], status: 200 }]);
    await new RedpandaTransformHttpAdapter(redpandaHttp).list(
      withCustomCa,
      new AbortController().signal,
    );
    expect(redpandaHttp.requests[0]).toMatchObject({ caPem: "explicit-service-ca" });
  });

  it("skips TLS socket creation for explicit plaintext latency input", async () => {
    const server = await startOAuthServer(200, "{}");
    const endpoint = new URL(server.endpoint);

    const result = await probeKafkaNetwork(
      {
        brokers: [`127.0.0.1:${endpoint.port}`],
        operationTimeoutMs: 5_000,
        tlsEnabled: false,
      },
      new AbortController().signal,
    );
    expect(result).toMatchObject({
      issues: [],
      tlsAttempted: false,
      tlsHandshakeMs: null,
    });
    expect(typeof result.tcpConnectMs).toBe("number");
    expect(server.requests()).toBe(0);
  });

  it.each([
    ["non-boolean TLS state", { enabled: 0 }],
    ["TLS without a CA", { enabled: true }],
    ["plaintext carrying a CA", { caPem: "must-not-be-ignored", enabled: false }],
    ["plaintext carrying an undefined CA field", { caPem: undefined, enabled: false }],
  ])("rejects malformed runtime input with %s", async (_label, tls) => {
    const adminFactory = new RecordingAdminFactory(new RecordingAdmin(["test"]));
    const engine = new StreamSkopeKafkaEngine({ adminFactory });
    const malformed = {
      brokers: ["localhost:19092"],
      name: "Malformed transport",
      tls,
    } as unknown as SecureConnectionInput;

    await expect(engine.testConnection(malformed)).rejects.toMatchObject({
      code: "VALIDATION",
      stage: "validation",
    });
    expect(adminFactory.inputs).toEqual([]);
  });

  it("does not retry a failed TLS connection as plaintext", async () => {
    const admin = new RecordingAdmin(() =>
      Promise.reject(codedError("SELF_SIGNED_CERT_IN_CHAIN", "untrusted certificate")),
    );
    const adminFactory = new RecordingAdminFactory(admin);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory,
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "access-token" }),
    });

    await expect(engine.testConnection(connection)).rejects.toMatchObject({
      code: "TLS_TRUST",
      stage: "tls",
    });
    expect(adminFactory.inputs).toHaveLength(1);
    expect(adminFactory.inputs[0]).toMatchObject({
      caPem: connection.tls.caPem,
      tlsEnabled: true,
    });
  });

  it("classifies rejected OAuth credentials before creating a Kafka client", async () => {
    const server = await startOAuthServer(
      401,
      '{"error":"invalid_client","error_description":"credentials rejected"}',
    );
    const admin = new RecordingAdmin([]);
    const adminFactory = new RecordingAdminFactory(admin);
    const engine = new StreamSkopeKafkaEngine({ adminFactory });
    const rejectedConnection = {
      ...connection,
      oauth: {
        ...oauthConnection,
        tokenEndpoint: server.endpoint,
      },
    };

    await expect(engine.testConnection(rejectedConnection)).rejects.toMatchObject({
      code: "OAUTH_REJECTED",
      stage: "oauth",
      target: server.endpoint,
    } satisfies Partial<KafkaEngineFailure>);
    expect(server.requests()).toBe(2);
    expect(adminFactory.inputs).toHaveLength(0);
    expect(admin.closeCalls).toBe(0);
  });

  it.each([
    {
      expectedCode: "TLS_TRUST",
      expectedStage: "tls",
      failure: codedError(
        "SELF_SIGNED_CERT_IN_CHAIN",
        "self-signed certificate in certificate chain",
      ),
      label: "TLS trust failure",
    },
    {
      expectedCode: "BROKER_UNREACHABLE",
      expectedStage: "broker",
      failure: codedError("ECONNREFUSED", "connect ECONNREFUSED"),
      label: "broker refusal",
    },
    {
      expectedCode: "KAFKA_AUTHENTICATION",
      expectedStage: "kafka",
      failure: codedError("SASL_AUTHENTICATION_FAILED", "SASL authentication failed"),
      label: "Kafka authentication failure",
    },
  ])(
    "classifies $label and closes the temporary admin",
    async ({ expectedCode, expectedStage, failure }) => {
      const admin = new RecordingAdmin(() => Promise.reject(failure));
      const engine = new StreamSkopeKafkaEngine({
        adminFactory: new RecordingAdminFactory(admin),
        requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "access-token" }),
      });

      await expect(engine.testConnection(connection)).rejects.toMatchObject({
        code: expectedCode,
        stage: expectedStage,
        target: "localhost:19093",
      });
      expect(admin.closeCalls).toBe(1);
    },
  );

  it("bounds a stalled metadata request and closes its temporary admin", async () => {
    const admin = new RecordingAdmin(() => new Promise<never>(() => undefined));
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(admin),
      operationTimeoutMs: 25,
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "access-token" }),
    });

    await expect(engine.testConnection(connection)).rejects.toMatchObject({
      code: "TIMEOUT",
      stage: "broker",
      target: "localhost:19093",
    });
    expect(admin.closeCalls).toBe(1);
  });

  it("reaches an IPv4-only local OAuth endpoint through localhost fallback", async () => {
    const server = await startOAuthServer(
      200,
      '{"access_token":"dual-stack-token","expires_in":60}',
    );
    const admin = new RecordingAdmin(["test"]);
    const adminFactory = new RecordingAdminFactory(admin);
    const engine = new StreamSkopeKafkaEngine({ adminFactory });

    await expect(
      engine.testConnection({
        ...connection,
        oauth: {
          ...oauthConnection,
          tokenEndpoint: server.endpoint,
        },
      }),
    ).resolves.toMatchObject({ topicCount: 1 });
    expect(server.requests()).toBe(1);
    await expect(adminFactory.inputs[0]?.oauthTokenProvider?.()).rejects.toMatchObject({
      name: "AbortError",
    });
    expect(admin.closeCalls).toBe(1);
  });

  it("qualifies independent service credentials with read-only calls and closes the temporary broker", async () => {
    const admin = new RecordingAdmin(["test"]);
    const http = new RecordingJsonHttp([
      { status: 200, body: ["orders-value"] },
      { status: 200, body: ["orders-sink"] },
      { status: 200, body: [{ class: "example.Sink" }] },
    ]);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(admin),
      serviceHttp: http,
    });
    const result = await engine.testConnection({
      name: "Independent services",
      brokers: ["broker.example:9092"],
      tls: { enabled: false },
      sasl: { mechanism: "SCRAM-SHA-512", username: "broker-user", password: "broker-secret" },
      services: {
        schemaRegistry: {
          baseUrl: "https://registry.example",
          authentication: "basic",
          basic: { username: "registry-user", password: "registry-secret" },
          tls: { caPem: "registry-ca" },
        },
        connect: {
          baseUrl: "https://connect.example",
          authentication: "bearer",
          bearer: "connect-secret",
          tls: {},
        },
      },
    });
    expect(result.checks).toEqual([
      "kafka-authentication",
      "metadata",
      "schema-registry",
      "connect",
    ]);
    expect(http.requests.map(({ method }) => method)).toEqual(["GET", "GET", "GET"]);
    expect(http.requests[0]).toMatchObject({
      caPem: "registry-ca",
      authorization: `Basic ${Buffer.from("registry-user:registry-secret").toString("base64")}`,
    });
    expect(http.requests[1]).toMatchObject({ authorization: "Bearer connect-secret" });
    expect(http.requests[1]).not.toHaveProperty("caPem");
    expect(admin.closeCalls).toBe(1);
  });

  it("reports service credential failures separately from a working Kafka connection without leaking response text", async () => {
    const admin = new RecordingAdmin(["test"]);
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: new RecordingAdminFactory(admin),
      serviceHttp: new RecordingJsonHttp([
        { status: 401, body: { message: "server-secret-fixture" } },
      ]),
    });
    const input = {
      name: "Service failure",
      brokers: ["broker.example:9092"],
      tls: { enabled: false },
      services: {
        schemaRegistry: {
          baseUrl: "https://registry.example",
          authentication: "basic",
          basic: { username: "user", password: "secret" },
          tls: {},
        },
      },
    } as const;
    await expect(engine.testConnection(input)).rejects.toMatchObject({
      code: "HTTPS_AUTHENTICATION",
      stage: "authorization",
      target: "schemaRegistry",
      message: "Schema Registry authentication was rejected.",
    });
    expect(admin.closeCalls).toBe(1);
    const active = await engine.openConnection(input, new AbortController().signal);
    await expect(active.listTopics()).resolves.toEqual(["test"]);
    await active.close();
  });

  it("owns the admitted connection credentials while asynchronous connection work is pending", async () => {
    const factory = new RecordingAdminFactory(new RecordingAdmin(["test"]));
    const engine = new StreamSkopeKafkaEngine({ adminFactory: factory });
    const input = {
      name: "Admitted connection",
      brokers: ["original.example:9093"],
      sasl: {
        mechanism: "SCRAM-SHA-256" as const,
        username: "original-user",
        password: "original-password",
      },
      tls: {
        enabled: true as const,
        caPem: "original-ca",
        clientIdentity: { certificatePem: "original-cert", privateKeyPem: "original-key" },
      },
      services: {
        schemaRegistry: {
          baseUrl: "https://registry.example",
          authentication: "basic" as const,
          basic: { username: "registry-user", password: "original-service-password" },
          tls: {},
        },
      },
    };
    const opening = engine.openConnection(input, new AbortController().signal);
    input.brokers[0] = "changed.example:9093";
    input.sasl.password = "changed-password";
    input.tls.clientIdentity.privateKeyPem = "changed-key";
    input.services.schemaRegistry.basic.password = "changed-service-password";
    const active = await opening;
    expect(factory.inputs[0]).toMatchObject({
      brokers: ["original.example:9093"],
      sasl: { password: "original-password" },
      clientIdentity: { privateKeyPem: "original-key" },
    });
    await expect(active.clusterServiceContext?.("schemaRegistry")?.authorization()).resolves.toBe(
      `Basic ${Buffer.from("registry-user:original-service-password").toString("base64")}`,
    );
    await active.close();
  });
});
