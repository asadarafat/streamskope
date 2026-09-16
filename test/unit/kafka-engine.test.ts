import { createServer, type Server } from "node:http";

import { afterEach, describe, expect, it } from "vitest";

import type {
  KafkaFetchRequest,
  KafkaConsumerGroupDetails,
  OAuthConnectionInput,
  SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import { KAFKA_MESSAGE_LIMITS, utf8ByteLength } from "../../src/features/kafka/contracts";
import {
  KafkaEngineFailure,
  StreamSkopeKafkaEngine,
  type KafkaAdminFactory,
  type KafkaAdminInput,
  type KafkaAdminPort,
  type OAuthToken,
  type OAuthTokenRequest,
} from "../../src/features/kafka/engine";

const oauthConnection: OAuthConnectionInput = {
  clientId: "admin",
  clientSecret: "fixture-secret",
  scope: "kafka",
  tokenEndpoint: "http://localhost:15000/token",
};

const connection: SecureConnectionInput = {
  brokers: ["localhost:19093"],
  name: "Local aio",
  oauth: oauthConnection,
  tls: {
    caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    enabled: true,
  },
};

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

  constructor(private readonly messages: readonly RawMessage[]) {}

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
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
    expect(admin.closeCalls).toBe(1);
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
        partition: 2,
        payload: '{"status":"ready"}',
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

    expect(records[0]?.originalByteSize).toBe(KAFKA_MESSAGE_LIMITS.messageBytes * 3);
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
    await expect(adminFactory.inputs[0]?.oauthTokenProvider?.()).resolves.toEqual({
      expiresAt: 1_800_000_000_000,
      value: "access-token",
    });
    expect(admin.closeCalls).toBe(1);
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
    await expect(adminFactory.inputs[0]?.oauthTokenProvider?.()).resolves.toMatchObject({
      value: "dual-stack-token",
    });
    expect(admin.closeCalls).toBe(1);
  });
});
