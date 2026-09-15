import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import {
  Admin,
  Consumer,
  Producer,
  type AdminOptions,
  type MessagesStream,
} from "@platformatic/kafka";

import {
  FixtureRuntimeError,
  type FixtureConnection,
  type FixtureRuntime,
  type OwnedFixtureRecord,
  type OwnedFixtureRequest,
} from "./lifecycle";
import { DEFAULT_SCHEMA_REGISTRY_PORT } from "./defaults";

const COMMAND_TIMEOUT_MS = 600_000;
const DIAGNOSTIC_CHARACTER_LIMIT = 16_384;
const READINESS_ATTEMPT_TIMEOUT_MS = 5_000;
const READINESS_INTERVAL_MS = 500;
const READINESS_TIMEOUT_MS = 90_000;

export interface FixtureSourceConfig {
  readonly oauthClientId: string;
  readonly oauthClientSecret: string;
  readonly oauthImage: string;
  readonly oauthScope: string;
  readonly schemaDefinition: string;
  readonly schemaRegistryAudience: string;
  readonly schemaRegistryImage: string;
  readonly schemaRegistryRole: string;
  readonly schemaSubject: string;
  readonly seedPayload: string;
  readonly topic: string;
}

interface CommandRequest {
  readonly args: readonly string[];
  readonly command: string;
  readonly cwd: string;
  readonly environment?: Readonly<Record<string, string>>;
}

interface CommandOutput {
  readonly standardError: string;
  readonly standardOutput: string;
}

class FixtureCommandError extends Error {
  constructor(message: string, options: { readonly cause?: unknown } = {}) {
    super(message, { cause: options.cause });
    this.name = "FixtureCommandError";
  }
}

function appendBounded(current: string, chunk: string): string {
  const combined = current + chunk;
  return combined.length <= DIAGNOSTIC_CHARACTER_LIMIT
    ? combined
    : combined.slice(-DIAGNOSTIC_CHARACTER_LIMIT);
}

async function runCommand(request: CommandRequest): Promise<CommandOutput> {
  return new Promise<CommandOutput>((resolvePromise, rejectPromise) => {
    const child = spawn(request.command, [...request.args], {
      cwd: request.cwd,
      env: { ...process.env, ...request.environment },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");

    let standardOutput = "";
    let standardError = "";
    let settled = false;
    let timedOut = false;

    child.stdout.on("data", (chunk: string) => {
      standardOutput = appendBounded(standardOutput, chunk);
    });
    child.stderr.on("data", (chunk: string) => {
      standardError = appendBounded(standardError, chunk);
    });

    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
    }, COMMAND_TIMEOUT_MS);

    child.once("error", (error: Error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      rejectPromise(
        new FixtureCommandError(`Unable to execute ${request.command}.`, { cause: error }),
      );
    });

    child.once("close", (exitCode, signal) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);

      if (exitCode === 0 && !timedOut) {
        resolvePromise({ standardError, standardOutput });
        return;
      }

      const diagnostic = (standardError || standardOutput).trim();
      const outcome = timedOut
        ? `timed out after ${COMMAND_TIMEOUT_MS} ms`
        : `exited with code ${String(exitCode)}${signal === null ? "" : ` (${signal})`}`;
      rejectPromise(
        new FixtureCommandError(
          `${request.command} ${outcome}${diagnostic.length === 0 ? "" : `: ${diagnostic}`}`,
        ),
      );
    });
  });
}

function parseFixtureSourceConfig(value: unknown): FixtureSourceConfig {
  if (value === null || typeof value !== "object") {
    throw new Error("Fixture configuration must be an object.");
  }

  const candidate = value as Partial<Record<keyof FixtureSourceConfig, unknown>>;
  const keys = [
    "oauthClientId",
    "oauthClientSecret",
    "oauthImage",
    "oauthScope",
    "schemaDefinition",
    "schemaRegistryAudience",
    "schemaRegistryImage",
    "schemaRegistryRole",
    "schemaSubject",
    "seedPayload",
    "topic",
  ] as const;

  for (const key of keys) {
    if (typeof candidate[key] !== "string" || candidate[key].length === 0) {
      throw new Error(`Fixture configuration field ${key} must be a non-empty string.`);
    }
  }

  return {
    oauthClientId: candidate.oauthClientId as string,
    oauthClientSecret: candidate.oauthClientSecret as string,
    oauthImage: candidate.oauthImage as string,
    oauthScope: candidate.oauthScope as string,
    schemaDefinition: candidate.schemaDefinition as string,
    schemaRegistryAudience: candidate.schemaRegistryAudience as string,
    schemaRegistryImage: candidate.schemaRegistryImage as string,
    schemaRegistryRole: candidate.schemaRegistryRole as string,
    schemaSubject: candidate.schemaSubject as string,
    seedPayload: candidate.seedPayload as string,
    topic: candidate.topic as string,
  };
}

export async function loadFixtureSourceConfig(
  repositoryRoot: string,
): Promise<FixtureSourceConfig> {
  const raw = await readFile(join(repositoryRoot, "aio-kafka", "fixture.config.json"), "utf8");
  return parseFixtureSourceConfig(JSON.parse(raw) as unknown);
}

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

function safeReadinessMessage(error: unknown, secret: string): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.replaceAll(secret, "[REDACTED]").slice(0, 512);
}

function readinessError(error: unknown, fallback: string): Error {
  return error instanceof Error ? error : new Error(fallback, { cause: error });
}

export class NodeFixtureRuntime implements FixtureRuntime {
  private readonly fixtureRoot: string;

  constructor(private readonly repositoryRoot: string) {
    this.fixtureRoot = join(repositoryRoot, "aio-kafka");
  }

  async resumeOwned(request: OwnedFixtureRecord): Promise<void> {
    if (resolve(request.topologyPath) !== join(this.fixtureRoot, "topology.clab.yml")) {
      throw new Error("Refusing to resume a fixture from another repository.");
    }
    const names = ["oauth", "broker", "schema-registry"].map(
      (role) => `clab-${request.name}-${role}`,
    );
    const inspection = await runCommand({
      command: "docker",
      args: [
        "inspect",
        "--format",
        '{{.Id}}|{{.Name}}|{{.State.Status}}|{{index .Config.Labels "containerlab"}}|{{index .Config.Labels "clab-topo-file"}}',
        ...names,
      ],
      cwd: this.repositoryRoot,
    });
    const stopped: string[] = [];
    const lines = inspection.standardOutput.trim().split("\n");
    if (lines.length !== names.length) throw new Error("Owned fixture inspection was incomplete.");
    for (const [index, line] of lines.entries()) {
      const [id, name, status, lab, topology] = line.split("|");
      if (
        id === undefined ||
        !/^[a-f0-9]{64}$/u.test(id) ||
        name !== `/${names[index]}` ||
        lab !== request.name ||
        topology !== request.topologyPath
      ) {
        throw new Error(
          "Fixture container ownership could not be verified; no containers were started.",
        );
      }
      if (status === "exited" || status === "created") stopped.push(id);
      else if (status !== "running")
        throw new Error(
          `Owned fixture container ${name} is ${status}; manual recovery is required.`,
        );
    }
    if (stopped.length > 0) {
      await runCommand({
        command: "docker",
        args: ["start", ...stopped],
        cwd: this.repositoryRoot,
      });
    }
  }

  async findUnavailablePorts(ports: readonly number[]): Promise<readonly number[]> {
    const results = await Promise.all(
      ports.map(async (port) => ({ port, unavailable: await this.isPortUnavailable(port) })),
    );
    return results.filter(({ unavailable }) => unavailable).map(({ port }) => port);
  }

  async buildOAuthImage(request: OwnedFixtureRequest): Promise<void> {
    await runCommand({
      args: [
        "build",
        "--tag",
        request.oauthImage,
        join(this.fixtureRoot, "images", "oauth-service"),
      ],
      command: "docker",
      cwd: this.repositoryRoot,
    });
  }

  async generateCertificates(request: OwnedFixtureRequest): Promise<void> {
    const certificateDirectory = this.ownedCertificateDirectory(request);
    await rm(certificateDirectory, { force: true, recursive: true });
    await runCommand({
      args: [join(this.fixtureRoot, "make-certs.sh")],
      command: "bash",
      cwd: this.repositoryRoot,
      environment: {
        CERT_DIR: certificateDirectory,
        STREAMSKOPE_FIXTURE_NAME: request.name,
      },
    });
  }

  async deploy(request: OwnedFixtureRequest): Promise<void> {
    await runCommand({
      args: ["deploy", "--topo", request.topologyPath, "--name", request.name, "--reconfigure"],
      command: "containerlab",
      cwd: this.fixtureRoot,
      environment: await this.topologyEnvironment(request),
    });
  }

  async waitUntilReady(connection: FixtureConnection): Promise<void> {
    const config = await this.loadSourceConfig();
    const deadline = Date.now() + READINESS_TIMEOUT_MS;
    let lastFailure = "readiness was not attempted";

    while (Date.now() < deadline) {
      try {
        await this.verifyReadyOnce(connection, config);
        return;
      } catch (error: unknown) {
        lastFailure = safeReadinessMessage(error, config.oauthClientSecret);
        if (connection.ownership === "owned" && connection.name !== undefined) {
          const terminalFailure = await this.terminalContainerFailure(
            connection.name,
            config.oauthClientSecret,
          );
          if (terminalFailure !== undefined) {
            throw new FixtureRuntimeError(
              "A Kafka fixture container exited during startup.",
              terminalFailure,
            );
          }
        }
      }

      await delay(READINESS_INTERVAL_MS);
    }

    throw new FixtureRuntimeError(
      "Kafka fixture readiness timed out.",
      `Kafka fixture was not ready within ${READINESS_TIMEOUT_MS} ms: ${lastFailure}`,
    );
  }

  async destroy(request: OwnedFixtureRecord): Promise<void> {
    await runCommand({
      args: ["destroy", "--topo", request.topologyPath, "--name", request.name, "--cleanup"],
      command: "containerlab",
      cwd: this.fixtureRoot,
      environment: await this.topologyEnvironment(request),
    });
    await rm(this.ownedCertificateDirectory(request), { force: true, recursive: true });
  }

  private async isPortUnavailable(port: number): Promise<boolean> {
    return new Promise<boolean>((resolvePromise, rejectPromise) => {
      const server = createServer();
      server.unref();
      server.once("error", (error: NodeJS.ErrnoException) => {
        if (error.code === "EADDRINUSE" || error.code === "EACCES") {
          resolvePromise(true);
          return;
        }
        rejectPromise(error);
      });
      server.listen(port, "127.0.0.1", () => {
        server.close((error) => {
          if (error !== undefined) {
            rejectPromise(error);
            return;
          }
          resolvePromise(false);
        });
      });
    });
  }

  private ownedCertificateDirectory(request: Pick<OwnedFixtureRecord, "caPath" | "name">): string {
    const certificateDirectory = resolve(dirname(request.caPath));
    const expectedDirectory = resolve(this.fixtureRoot, "ownership", request.name, "certs");
    if (certificateDirectory !== expectedDirectory) {
      throw new Error(`Refusing generated-certificate operation outside ${expectedDirectory}.`);
    }
    return certificateDirectory;
  }

  private async loadSourceConfig(): Promise<FixtureSourceConfig> {
    return loadFixtureSourceConfig(this.repositoryRoot);
  }

  private async topologyEnvironment(
    request: OwnedFixtureRecord | OwnedFixtureRequest,
  ): Promise<Readonly<Record<string, string>>> {
    const config = await this.loadSourceConfig();
    return {
      STREAMSKOPE_CERT_DIR: this.ownedCertificateDirectory(request),
      STREAMSKOPE_FIXTURE_NAME: request.name,
      STREAMSKOPE_KAFKA_PORT: String(request.kafkaPort),
      STREAMSKOPE_OAUTH_CLIENT_ID: config.oauthClientId,
      STREAMSKOPE_OAUTH_CLIENT_SECRET: config.oauthClientSecret,
      STREAMSKOPE_OAUTH_IMAGE: request.oauthImage,
      STREAMSKOPE_OAUTH_ISSUER: `http://clab-${request.name}-oauth:5000`,
      STREAMSKOPE_OAUTH_PORT: String(request.oauthPort),
      STREAMSKOPE_OAUTH_SCOPE: config.oauthScope,
      STREAMSKOPE_SCHEMA_REGISTRY_AUDIENCE: config.schemaRegistryAudience,
      STREAMSKOPE_SCHEMA_REGISTRY_IMAGE: request.schemaRegistryImage ?? config.schemaRegistryImage,
      STREAMSKOPE_SCHEMA_REGISTRY_PORT: String(
        request.schemaRegistryPort ?? DEFAULT_SCHEMA_REGISTRY_PORT,
      ),
      STREAMSKOPE_SCHEMA_REGISTRY_ROLE: config.schemaRegistryRole,
      STREAMSKOPE_TOPIC: config.topic,
    };
  }

  private async verifyReadyOnce(
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
        await this.verifySchemaRegistry(connection, config);
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

  private async verifySchemaRegistry(
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

  private async terminalContainerFailure(
    name: string,
    secret: string,
  ): Promise<string | undefined> {
    const containerNames = [
      `clab-${name}-broker`,
      `clab-${name}-oauth`,
      `clab-${name}-schema-registry`,
    ];

    try {
      const inspection = await runCommand({
        args: [
          "inspect",
          "--format",
          "{{.Name}}|{{.State.Status}}|{{.State.ExitCode}}|{{.State.Error}}",
          ...containerNames,
        ],
        command: "docker",
        cwd: this.repositoryRoot,
      });
      const failedLine = inspection.standardOutput
        .trim()
        .split("\n")
        .find((line) => /\|(?:dead|exited|restarting)\|/u.test(line));
      if (failedLine === undefined) {
        return undefined;
      }

      const containerName = failedLine.split("|")[0]?.replace(/^\//u, "");
      if (containerName === undefined || !containerNames.includes(containerName)) {
        return "An owned fixture container exited, but its bounded diagnostics were unavailable.";
      }
      const logs = await runCommand({
        args: ["logs", "--tail", "80", containerName],
        command: "docker",
        cwd: this.repositoryRoot,
      });
      const logTail = (logs.standardError || logs.standardOutput).trim();
      return `${failedLine}\n${logTail}`
        .replaceAll(secret, "[REDACTED]")
        .slice(-DIAGNOSTIC_CHARACTER_LIMIT);
    } catch {
      return undefined;
    }
  }
}
