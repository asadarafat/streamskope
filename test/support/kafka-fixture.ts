import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { Admin, Producer, type BaseOptions } from "@platformatic/kafka";

export interface FixtureConfig {
  readonly oauthClientId: string;
  readonly oauthClientSecret: string;
  readonly oauthScope: string;
  readonly schemaDefinition: string;
  readonly schemaRegistryAudience: string;
  readonly schemaRegistryImage: string;
  readonly schemaRegistryRole: string;
  readonly schemaSubject: string;
  readonly seedPayload: string;
  readonly topic: string;
}

export interface FixtureConnection {
  readonly caPath: string;
  readonly kafkaEndpoint: string;
  readonly oauthEndpoint: string;
  readonly schemaRegistryEndpoint?: string;
}

export interface SeededFixtureTopic {
  readonly config: FixtureConfig;
  dispose(): Promise<void>;
}

const repositoryRoot = process.cwd();

async function readJson(path: string): Promise<unknown> {
  return JSON.parse(await readFile(path, "utf8")) as unknown;
}

export async function loadFixtureConfig(): Promise<FixtureConfig> {
  const value = await readJson(join(repositoryRoot, "aio-kafka", "fixture.config.json"));
  if (value === null || typeof value !== "object") {
    throw new Error("Fixture configuration is missing.");
  }
  const candidate = value as Partial<Record<keyof FixtureConfig, unknown>>;
  if (
    typeof candidate.oauthClientId !== "string" ||
    typeof candidate.oauthClientSecret !== "string" ||
    typeof candidate.oauthScope !== "string" ||
    typeof candidate.schemaDefinition !== "string" ||
    typeof candidate.schemaRegistryAudience !== "string" ||
    typeof candidate.schemaRegistryImage !== "string" ||
    typeof candidate.schemaRegistryRole !== "string" ||
    typeof candidate.schemaSubject !== "string" ||
    typeof candidate.seedPayload !== "string" ||
    typeof candidate.topic !== "string"
  ) {
    throw new Error("Fixture configuration has an invalid shape.");
  }
  return candidate as FixtureConfig;
}

export async function loadFixtureConnection(): Promise<FixtureConnection> {
  const explicitKafka = process.env.STREAMSKOPE_TEST_KAFKA_ENDPOINT;
  const explicitOAuth = process.env.STREAMSKOPE_TEST_OAUTH_ENDPOINT;
  const explicitCa = process.env.STREAMSKOPE_TEST_CA_PATH;
  const explicitSchemaRegistry = process.env.STREAMSKOPE_TEST_SCHEMA_REGISTRY_ENDPOINT;
  if (explicitKafka !== undefined || explicitOAuth !== undefined || explicitCa !== undefined) {
    if (explicitKafka === undefined || explicitOAuth === undefined || explicitCa === undefined) {
      throw new Error("All three STREAMSKOPE_TEST_* connection variables must be provided.");
    }
    return {
      caPath: explicitCa,
      kafkaEndpoint: explicitKafka,
      oauthEndpoint: explicitOAuth,
      ...(explicitSchemaRegistry === undefined
        ? {}
        : { schemaRegistryEndpoint: explicitSchemaRegistry }),
    };
  }

  const fixtureName = process.env.STREAMSKOPE_TEST_FIXTURE_NAME ?? "streamskope-kafka";
  const ownershipPath = join(
    repositoryRoot,
    "aio-kafka",
    "ownership",
    "records",
    `${fixtureName}.json`,
  );
  const record = await readJson(ownershipPath);
  if (record === null || typeof record !== "object") {
    throw new Error(`Fixture ownership record ${fixtureName} is unavailable.`);
  }
  const candidate = record as {
    readonly caPath?: unknown;
    readonly kafkaPort?: unknown;
    readonly oauthPort?: unknown;
    readonly schemaRegistryPort?: unknown;
  };
  if (
    typeof candidate.caPath !== "string" ||
    typeof candidate.kafkaPort !== "number" ||
    typeof candidate.oauthPort !== "number"
  ) {
    throw new Error(`Fixture ownership record ${fixtureName} has an invalid shape.`);
  }
  return {
    caPath: candidate.caPath,
    kafkaEndpoint: `127.0.0.1:${candidate.kafkaPort}`,
    oauthEndpoint: `http://127.0.0.1:${candidate.oauthPort}/rest-gateway/rest/api/v1/auth/token`,
    ...(typeof candidate.schemaRegistryPort === "number"
      ? { schemaRegistryEndpoint: `http://127.0.0.1:${candidate.schemaRegistryPort}` }
      : {}),
  };
}

export async function fetchFixtureToken(
  connection: FixtureConnection,
  config: FixtureConfig,
): Promise<string> {
  const authorization = Buffer.from(
    `${config.oauthClientId}:${config.oauthClientSecret}`,
    "utf8",
  ).toString("base64");
  const response = await fetch(connection.oauthEndpoint, {
    body: new URLSearchParams({
      grant_type: "client_credentials",
      scope: config.oauthScope,
    }),
    headers: {
      authorization: `Basic ${authorization}`,
      "content-type": "application/x-www-form-urlencoded",
    },
    method: "POST",
    signal: AbortSignal.timeout(5_000),
  });
  const value: unknown = await response.json();
  if (
    !response.ok ||
    value === null ||
    typeof value !== "object" ||
    !("access_token" in value) ||
    typeof (value as { readonly access_token?: unknown }).access_token !== "string"
  ) {
    throw new Error(`Fixture token request failed with HTTP ${response.status}.`);
  }
  return (value as { readonly access_token: string }).access_token;
}

export async function fixtureClientOptions(
  connection: FixtureConnection,
  config: FixtureConfig,
  clientId: string,
): Promise<BaseOptions> {
  return {
    bootstrapBrokers: [connection.kafkaEndpoint],
    clientId,
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    retries: 5,
    sasl: {
      mechanism: "OAUTHBEARER",
      token: await fetchFixtureToken(connection, config),
    },
    tls: {
      ca: [await readFile(connection.caPath, "utf8")],
      rejectUnauthorized: true,
    },
  };
}

export async function provisionSeededFixtureTopic(): Promise<SeededFixtureTopic> {
  const config = await loadFixtureConfig();
  const connection = await loadFixtureConnection();
  const topic = `streamskope-e2e-${randomUUID()}`;
  const options = await fixtureClientOptions(
    connection,
    config,
    `streamskope-e2e-provision-${randomUUID()}`,
  );
  const admin = new Admin(options);
  const producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
    ...options,
    autocreateTopics: false,
    clientId: `streamskope-e2e-seed-${randomUUID()}`,
  });
  let topicCreated = false;

  try {
    await admin.createTopics({ partitions: 1, replicas: 1, topics: [topic] });
    topicCreated = true;
    await producer.send({
      messages: [
        {
          key: Buffer.from("streamskope-seed", "utf8"),
          topic,
          value: Buffer.from(config.seedPayload, "utf8"),
        },
      ],
    });
  } catch (error: unknown) {
    if (topicCreated) {
      await admin.deleteTopics({ topics: [topic] }).catch(() => undefined);
    }
    throw error;
  } finally {
    await Promise.allSettled([producer.close(), admin.close()]);
  }

  let disposed = false;
  return {
    config: { ...config, topic },
    async dispose(): Promise<void> {
      if (disposed) {
        return;
      }
      disposed = true;
      const cleanupAdmin = new Admin(
        await fixtureClientOptions(connection, config, `streamskope-e2e-cleanup-${randomUUID()}`),
      );
      try {
        await cleanupAdmin.deleteTopics({ topics: [topic] });
      } finally {
        await cleanupAdmin.close();
      }
    },
  };
}
