import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import {
  Admin,
  Consumer,
  Producer,
  type AdminOptions,
  type MessagesStream,
} from "@platformatic/kafka";

import type { FixtureConnection } from "./lifecycle";
import type { FixtureSourceConfig } from "./node-runtime";

const READINESS_ATTEMPT_TIMEOUT_MS = 5_000;

async function postTokenRequest(
  endpoint: string,
  body: URLSearchParams,
  authorization: string | undefined,
): Promise<Response> {
  const headers = new Headers({ "content-type": "application/x-www-form-urlencoded" });
  if (authorization !== undefined) {
    headers.set("authorization", authorization);
  }

  return fetch(endpoint, {
    body,
    headers,
    method: "POST",
    signal: AbortSignal.timeout(READINESS_ATTEMPT_TIMEOUT_MS),
  });
}

async function requestOAuthToken(
  endpoint: string,
  config: FixtureSourceConfig,
): Promise<{ readonly expiresAt?: number; readonly value: string }> {
  const basicAuthorization = `Basic ${Buffer.from(
    `${config.oauthClientId}:${config.oauthClientSecret}`,
    "utf8",
  ).toString("base64")}`;
  let response = await postTokenRequest(
    endpoint,
    new URLSearchParams({ grant_type: "client_credentials", scope: config.oauthScope }),
    basicAuthorization,
  );

  if (!response.ok) {
    response = await postTokenRequest(
      endpoint,
      new URLSearchParams({
        client_id: config.oauthClientId,
        client_secret: config.oauthClientSecret,
        grant_type: "client_credentials",
        scope: config.oauthScope,
      }),
      undefined,
    );
  }

  const responseText = (await response.text()).slice(0, 4_096);
  if (!response.ok) {
    throw new Error(`OAuth token endpoint returned HTTP ${response.status}.`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(responseText);
  } catch (error: unknown) {
    throw new Error("OAuth token endpoint returned malformed JSON.", { cause: error });
  }

  if (
    parsed === null ||
    typeof parsed !== "object" ||
    !("access_token" in parsed) ||
    typeof (parsed as { readonly access_token?: unknown }).access_token !== "string" ||
    (parsed as { readonly access_token: string }).access_token.length === 0
  ) {
    throw new Error("OAuth token endpoint returned no access_token.");
  }

  const tokenResponse = parsed as {
    readonly access_token: string;
    readonly expires_in?: unknown;
  };
  const expiresInSeconds =
    typeof tokenResponse.expires_in === "number" && Number.isFinite(tokenResponse.expires_in)
      ? tokenResponse.expires_in
      : undefined;
  return expiresInSeconds === undefined
    ? { value: tokenResponse.access_token }
    : {
        expiresAt: Date.now() + expiresInSeconds * 1_000 - 5_000,
        value: tokenResponse.access_token,
      };
}

async function disconnectKafkaClients(
  admin: Admin | undefined,
  producer: Producer | undefined,
  consumer: Consumer | undefined,
  stream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | undefined,
): Promise<void> {
  const failures: unknown[] = [];
  for (const close of [
    stream === undefined ? undefined : (): Promise<void> => stream.close(),
    consumer === undefined ? undefined : (): Promise<void> => consumer.close(),
    producer === undefined ? undefined : (): Promise<void> => producer.close(),
    admin === undefined ? undefined : (): Promise<void> => admin.close(),
  ]) {
    if (close !== undefined) {
      try {
        await close();
      } catch (error: unknown) {
        failures.push(error);
      }
    }
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, "Kafka fixture readiness clients did not close cleanly.");
  }
}

async function confirmSeedConsumption(
  stream: MessagesStream<Buffer, Buffer, Buffer, Buffer>,
  seedPayload: string,
): Promise<void> {
  let timeoutId: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timeoutId = setTimeout(() => {
      reject(new Error("Kafka consumer-group readiness timed out."));
    }, READINESS_ATTEMPT_TIMEOUT_MS);
  });
  const seed = (async (): Promise<void> => {
    for await (const message of stream) {
      if (message.value.toString("utf8") === seedPayload) {
        return;
      }
    }
    throw new Error("Kafka readiness stream ended before the seed record.");
  })();

  try {
    await Promise.race([seed, timeout]);
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
  }
}

function fixtureKafkaOptions(
  connection: FixtureConnection,
  config: FixtureSourceConfig,
  ca: string,
  clientId: string,
): AdminOptions {
  return {
    bootstrapBrokers: [connection.kafkaEndpoint],
    clientId,
    connectTimeout: READINESS_ATTEMPT_TIMEOUT_MS,
    requestTimeout: READINESS_ATTEMPT_TIMEOUT_MS,
    retries: 0,
    sasl: {
      mechanism: "OAUTHBEARER",
      token: async (): Promise<string> =>
        (await requestOAuthToken(connection.oauthEndpoint, config)).value,
    },
    tls: { ca: [ca], rejectUnauthorized: true },
  };
}

export function safeReadinessMessage(error: unknown, secret: string): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.replaceAll(secret, "[REDACTED]").slice(0, 512);
}

function readinessError(error: unknown, fallback: string): Error {
  return error instanceof Error ? error : new Error(fallback, { cause: error });
}

export async function verifyFixtureReady(
  connection: FixtureConnection,
  config: FixtureSourceConfig,
): Promise<void> {
  const ca = await readFile(connection.caPath, "utf8");
  const options = fixtureKafkaOptions(connection, config, ca, "streamskope-fixture-readiness");
  let admin: Admin | undefined;
  let producer: Producer | undefined;
  let consumer: Consumer | undefined;
  let stream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | undefined;
  let readinessFailure: unknown;

  try {
    admin = new Admin(options);

    if (connection.ownership === "external") {
      await admin.listTopics();
    } else {
      const topics = await admin.listTopics();
      if (!topics.includes(config.topic)) {
        await admin.createTopics({
          partitions: 1,
          replicas: 1,
          topics: [config.topic],
        });
      }
      producer = new Producer({
        ...options,
        autocreateTopics: false,
        clientId: "streamskope-fixture-seed",
      });
      consumer = new Consumer({
        ...options,
        clientId: "streamskope-fixture-readiness-consumer",
        groupId: `streamskope-fixture-readiness-${randomUUID()}`,
        retries: 0,
      });
      stream = await consumer.consume({
        fallbackMode: "earliest",
        mode: "earliest",
        topics: [config.topic],
      });
      await producer.send({
        messages: [
          {
            key: Buffer.from("streamskope-seed", "utf8"),
            topic: config.topic,
            value: Buffer.from(config.seedPayload, "utf8"),
          },
        ],
      });
      await confirmSeedConsumption(stream, config.seedPayload);
      await verifySchemaRegistry(connection, config);
    }
  } catch (error: unknown) {
    readinessFailure = error;
  }
  let cleanupFailure: unknown;
  try {
    await disconnectKafkaClients(admin, producer, consumer, stream);
  } catch (error: unknown) {
    cleanupFailure = error;
  }
  if (readinessFailure !== undefined) {
    const operationError = readinessError(
      readinessFailure,
      "Kafka fixture readiness failed with a non-error value.",
    );
    if (cleanupFailure !== undefined) {
      throw new AggregateError(
        [
          operationError,
          readinessError(cleanupFailure, "Kafka fixture cleanup failed with a non-error value."),
        ],
        "Kafka fixture readiness and cleanup both failed.",
        { cause: operationError },
      );
    }
    throw operationError;
  }
  if (cleanupFailure !== undefined) {
    throw readinessError(cleanupFailure, "Kafka fixture cleanup failed with a non-error value.");
  }
}

async function verifySchemaRegistry(
  connection: FixtureConnection,
  config: FixtureSourceConfig,
): Promise<void> {
  if (connection.schemaRegistryEndpoint === undefined) {
    throw new Error("Owned fixture has no Schema Registry endpoint.");
  }
  const endpoint = connection.schemaRegistryEndpoint.replace(/\/+$/u, "");
  const missingAuthorization = await fetch(`${endpoint}/subjects`, {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(READINESS_ATTEMPT_TIMEOUT_MS),
  });
  if (missingAuthorization.status !== 401) {
    throw new Error(
      `Schema Registry accepted a request without OAuth; received HTTP ${String(missingAuthorization.status)}.`,
    );
  }
  const malformedAuthorization = await fetch(`${endpoint}/subjects`, {
    headers: {
      accept: "application/json",
      authorization: "Bearer malformed-fixture-token",
    },
    signal: AbortSignal.timeout(READINESS_ATTEMPT_TIMEOUT_MS),
  });
  if (malformedAuthorization.status !== 401) {
    throw new Error(
      `Schema Registry accepted a malformed OAuth bearer; received HTTP ${String(malformedAuthorization.status)}.`,
    );
  }
  const authorization = `Bearer ${(await requestOAuthToken(connection.oauthEndpoint, config)).value}`;
  const registryHeaders = {
    accept: "application/json",
    authorization,
  };
  const subjectsResponse = await fetch(`${endpoint}/subjects`, {
    headers: registryHeaders,
    signal: AbortSignal.timeout(READINESS_ATTEMPT_TIMEOUT_MS),
  });
  if (!subjectsResponse.ok) {
    throw new Error(
      `Schema Registry subjects endpoint returned HTTP ${String(subjectsResponse.status)}.`,
    );
  }
  const subjects: unknown = await subjectsResponse.json();
  if (!Array.isArray(subjects) || !subjects.every((subject) => typeof subject === "string")) {
    throw new Error("Schema Registry subjects endpoint returned malformed JSON.");
  }
  if (!subjects.includes(config.schemaSubject)) {
    const registration = await fetch(
      `${endpoint}/subjects/${encodeURIComponent(config.schemaSubject)}/versions`,
      {
        body: JSON.stringify({ schema: config.schemaDefinition, schemaType: "AVRO" }),
        headers: {
          accept: "application/json",
          authorization,
          "content-type": "application/vnd.schemaregistry.v1+json",
        },
        method: "POST",
        signal: AbortSignal.timeout(READINESS_ATTEMPT_TIMEOUT_MS),
      },
    );
    if (!registration.ok) {
      throw new Error(
        `Schema Registry seed registration returned HTTP ${String(registration.status)}.`,
      );
    }
  }
  const detail = await fetch(
    `${endpoint}/subjects/${encodeURIComponent(config.schemaSubject)}/versions/latest`,
    {
      headers: registryHeaders,
      signal: AbortSignal.timeout(READINESS_ATTEMPT_TIMEOUT_MS),
    },
  );
  if (!detail.ok) {
    throw new Error(`Schema Registry seed detail returned HTTP ${String(detail.status)}.`);
  }
  const payload: unknown = await detail.json();
  if (
    payload === null ||
    typeof payload !== "object" ||
    !("subject" in payload) ||
    payload.subject !== config.schemaSubject
  ) {
    throw new Error("Schema Registry seed detail did not confirm the fixture subject.");
  }
}
