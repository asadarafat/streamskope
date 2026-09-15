export interface OwnedFixtureRequest {
  readonly caPath: string;
  readonly kafkaPort: number;
  readonly name: string;
  readonly oauthImage: string;
  readonly oauthPort: number;
  readonly schemaRegistryImage: string;
  readonly schemaRegistryPort: number;
  readonly topologyPath: string;
}

export interface ExternalFixtureRequest {
  readonly caPath: string;
  readonly kafkaEndpoint: string;
  readonly oauthEndpoint: string;
}

export interface OwnedFixtureRecord {
  readonly caPath: string;
  readonly kafkaPort: number;
  readonly name: string;
  readonly oauthImage: string;
  readonly oauthPort: number;
  readonly ownership: "owned";
  readonly schemaRegistryImage?: string;
  readonly schemaRegistryPort?: number;
  readonly topologyPath: string;
}

export interface FixtureConnection {
  readonly caPath: string;
  readonly kafkaEndpoint: string;
  readonly name: string | undefined;
  readonly oauthEndpoint: string;
  readonly ownership: "external" | "owned";
  readonly schemaRegistryEndpoint?: string;
}

export interface FixtureRuntime {
  resumeOwned(request: OwnedFixtureRecord): Promise<void>;
  findUnavailablePorts(ports: readonly number[]): Promise<readonly number[]>;
  buildOAuthImage(request: OwnedFixtureRequest): Promise<void>;
  generateCertificates(request: OwnedFixtureRequest): Promise<void>;
  deploy(request: OwnedFixtureRequest): Promise<void>;
  waitUntilReady(connection: FixtureConnection): Promise<void>;
  destroy(request: OwnedFixtureRecord): Promise<void>;
}

export interface FixtureOwnershipStore {
  load(name: string): Promise<OwnedFixtureRecord | undefined>;
  save(record: OwnedFixtureRecord): Promise<void>;
  remove(name: string): Promise<void>;
}

type FixtureLifecycleErrorCode =
  | "ALREADY_OWNED"
  | "EXTERNAL_UNAVAILABLE"
  | "INVALID_REQUEST"
  | "NOT_OWNED"
  | "PORT_IN_USE"
  | "STARTUP_CLEANUP_FAILED"
  | "STARTUP_FAILED";

interface FixtureLifecycleErrorOptions {
  readonly cause?: unknown;
  readonly diagnostic?: string;
  readonly unavailablePorts?: readonly number[];
}

export class FixtureLifecycleError extends Error {
  readonly code: FixtureLifecycleErrorCode;
  readonly diagnostic: string | undefined;
  readonly unavailablePorts: readonly number[] | undefined;

  constructor(
    code: FixtureLifecycleErrorCode,
    message: string,
    options: FixtureLifecycleErrorOptions = {},
  ) {
    super(message, { cause: options.cause });
    this.name = "FixtureLifecycleError";
    this.code = code;
    this.diagnostic = options.diagnostic;
    this.unavailablePorts = options.unavailablePorts;
  }
}

export class FixtureRuntimeError extends Error {
  readonly safeDetail: string;

  constructor(message: string, safeDetail: string, options: { readonly cause?: unknown } = {}) {
    super(message, { cause: options.cause });
    this.name = "FixtureRuntimeError";
    this.safeDetail = safeDetail.slice(0, 1_024);
  }
}

function validatePort(port: number, field: string): void {
  if (!Number.isInteger(port) || port < 1_024 || port > 65_535) {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      `${field} must be an integer from 1024 through 65535.`,
    );
  }
}

export function assertFixtureName(name: string): void {
  if (!/^[a-z][a-z0-9-]{2,47}$/u.test(name)) {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      "Fixture name must contain 3–48 lowercase letters, numbers, or hyphens.",
    );
  }
}

function validateOwnedRequest(request: OwnedFixtureRequest): void {
  assertFixtureName(request.name);

  validatePort(request.kafkaPort, "Kafka port");
  validatePort(request.oauthPort, "OAuth port");
  validatePort(request.schemaRegistryPort, "Schema Registry port");

  if (new Set([request.kafkaPort, request.oauthPort, request.schemaRegistryPort]).size !== 3) {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      "Kafka, OAuth, and Schema Registry host ports must be different.",
    );
  }

  if (
    request.caPath.length === 0 ||
    request.oauthImage.length === 0 ||
    request.schemaRegistryImage.length === 0 ||
    request.topologyPath.length === 0
  ) {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      "CA path, OAuth image, Schema Registry image, and topology path are required.",
    );
  }
}

export function parseOwnedFixtureRecord(value: unknown): OwnedFixtureRecord {
  if (value === null || typeof value !== "object") {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      "Fixture ownership record must be an object.",
    );
  }

  const candidate = value as Partial<Record<keyof OwnedFixtureRecord, unknown>>;
  const hasSchemaRegistryImage = candidate.schemaRegistryImage !== undefined;
  const hasSchemaRegistryPort = candidate.schemaRegistryPort !== undefined;
  if (
    typeof candidate.caPath !== "string" ||
    typeof candidate.kafkaPort !== "number" ||
    typeof candidate.name !== "string" ||
    typeof candidate.oauthImage !== "string" ||
    typeof candidate.oauthPort !== "number" ||
    candidate.ownership !== "owned" ||
    typeof candidate.topologyPath !== "string" ||
    hasSchemaRegistryImage !== hasSchemaRegistryPort ||
    (hasSchemaRegistryImage && typeof candidate.schemaRegistryImage !== "string") ||
    (hasSchemaRegistryPort && typeof candidate.schemaRegistryPort !== "number")
  ) {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      "Fixture ownership record has an invalid shape.",
    );
  }

  const record: OwnedFixtureRecord = {
    caPath: candidate.caPath,
    kafkaPort: candidate.kafkaPort,
    name: candidate.name,
    oauthImage: candidate.oauthImage,
    oauthPort: candidate.oauthPort,
    ownership: "owned",
    ...(hasSchemaRegistryImage && hasSchemaRegistryPort
      ? {
          schemaRegistryImage: candidate.schemaRegistryImage as string,
          schemaRegistryPort: candidate.schemaRegistryPort as number,
        }
      : {}),
    topologyPath: candidate.topologyPath,
  };
  assertFixtureName(record.name);
  validatePort(record.kafkaPort, "Kafka port");
  validatePort(record.oauthPort, "OAuth port");
  if (record.schemaRegistryPort !== undefined) {
    validatePort(record.schemaRegistryPort, "Schema Registry port");
    if (new Set([record.kafkaPort, record.oauthPort, record.schemaRegistryPort]).size !== 3) {
      throw new FixtureLifecycleError(
        "INVALID_REQUEST",
        "Kafka, OAuth, and Schema Registry host ports must be different.",
      );
    }
  } else if (record.kafkaPort === record.oauthPort) {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      "Kafka and OAuth host ports must be different.",
    );
  }
  if (
    record.caPath.length === 0 ||
    record.oauthImage.length === 0 ||
    record.topologyPath.length === 0 ||
    record.schemaRegistryImage === ""
  ) {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      "Fixture ownership paths and images must be non-empty.",
    );
  }
  return record;
}

function ownedConnection(request: OwnedFixtureRequest): FixtureConnection {
  return {
    caPath: request.caPath,
    kafkaEndpoint: `127.0.0.1:${request.kafkaPort}`,
    name: request.name,
    oauthEndpoint: `http://127.0.0.1:${request.oauthPort}/rest-gateway/rest/api/v1/auth/token`,
    ownership: "owned",
    schemaRegistryEndpoint: `http://127.0.0.1:${request.schemaRegistryPort}`,
  };
}

function externalConnection(request: ExternalFixtureRequest): FixtureConnection {
  if (
    request.caPath.length === 0 ||
    request.kafkaEndpoint.length === 0 ||
    request.oauthEndpoint.length === 0
  ) {
    throw new FixtureLifecycleError(
      "INVALID_REQUEST",
      "External CA path, Kafka endpoint, and OAuth endpoint are required.",
    );
  }

  try {
    new URL(request.oauthEndpoint);
  } catch (error: unknown) {
    throw new FixtureLifecycleError("INVALID_REQUEST", "OAuth endpoint must be a valid URL.", {
      cause: error,
    });
  }

  return {
    ...request,
    name: undefined,
    ownership: "external",
  };
}

export class KafkaFixtureLifecycle {
  constructor(
    private readonly runtime: FixtureRuntime,
    private readonly ownershipStore: FixtureOwnershipStore,
  ) {}

  async ensureOwned(request: OwnedFixtureRequest): Promise<FixtureConnection> {
    validateOwnedRequest(request);
    const record = await this.ownershipStore.load(request.name);
    if (record === undefined) return this.startOwned(request);
    for (const key of [
      "caPath",
      "kafkaPort",
      "oauthPort",
      "oauthImage",
      "schemaRegistryImage",
      "schemaRegistryPort",
      "topologyPath",
    ] as const) {
      if (record[key] !== request[key]) {
        throw new FixtureLifecycleError(
          "INVALID_REQUEST",
          `Owned fixture ${request.name} has different ${key}; it will not be recreated automatically.`,
        );
      }
    }
    await this.runtime.resumeOwned(record);
    const connection = ownedConnection(request);
    // Reuse readiness without publishing another probe message on every web launch.
    await this.runtime.waitUntilReady({ ...connection, ownership: "external" });
    return connection;
  }

  async startOwned(request: OwnedFixtureRequest): Promise<FixtureConnection> {
    validateOwnedRequest(request);

    if ((await this.ownershipStore.load(request.name)) !== undefined) {
      throw new FixtureLifecycleError(
        "ALREADY_OWNED",
        `Fixture ${request.name} already has an ownership record.`,
      );
    }

    const unavailablePorts = await this.runtime.findUnavailablePorts([
      request.kafkaPort,
      request.oauthPort,
      request.schemaRegistryPort,
    ]);
    if (unavailablePorts.length > 0) {
      throw new FixtureLifecycleError(
        "PORT_IN_USE",
        `Fixture ports are unavailable: ${unavailablePorts.join(", ")}.`,
        { unavailablePorts },
      );
    }

    const record: OwnedFixtureRecord = { ...request, ownership: "owned" };
    const connection = ownedConnection(request);
    let deployed = false;

    try {
      await this.runtime.buildOAuthImage(request);
      await this.runtime.generateCertificates(request);
      await this.runtime.deploy(request);
      deployed = true;
      await this.runtime.waitUntilReady(connection);
      await this.ownershipStore.save(record);
      return connection;
    } catch (error: unknown) {
      if (deployed) {
        try {
          await this.runtime.destroy(record);
        } catch (cleanupError: unknown) {
          throw new FixtureLifecycleError(
            "STARTUP_CLEANUP_FAILED",
            `Fixture ${request.name} failed to start and automatic cleanup also failed.`,
            { cause: cleanupError },
          );
        }
      }

      const options: FixtureLifecycleErrorOptions =
        error instanceof FixtureRuntimeError
          ? { cause: error, diagnostic: error.safeDetail }
          : { cause: error };
      throw new FixtureLifecycleError(
        "STARTUP_FAILED",
        `Fixture ${request.name} failed to become ready and no ownership was retained.`,
        options,
      );
    }
  }

  async attachExternal(request: ExternalFixtureRequest): Promise<FixtureConnection> {
    const connection = externalConnection(request);

    try {
      await this.runtime.waitUntilReady(connection);
      return connection;
    } catch (error: unknown) {
      throw new FixtureLifecycleError(
        "EXTERNAL_UNAVAILABLE",
        "The external Kafka fixture did not pass readiness checks; no resources were changed.",
        { cause: error },
      );
    }
  }

  async stopOwned(name: string): Promise<void> {
    const record = await this.ownershipStore.load(name);
    if (record === undefined) {
      throw new FixtureLifecycleError(
        "NOT_OWNED",
        `Fixture ${name} has no ownership record and will not be destroyed.`,
      );
    }

    await this.runtime.destroy(record);
    await this.ownershipStore.remove(name);
  }
}
