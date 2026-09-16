import { randomUUID } from "node:crypto";

import {
  HostContractValidationError,
  KAFKA_MESSAGE_LIMITS,
  parseKafkaFetchRequest,
  parseKafkaLatencyProbeRequest,
  utf8ByteLength,
  validateSecureConnectionInput,
  type HostErrorStage,
  type KafkaFetchRequest,
  type KafkaLatencyProbeRequest,
  type KafkaConfigurationEntry,
  type KafkaAclBinding,
  type KafkaConsumerGroupDetails,
  type KafkaMessage,
  type KafkaTopicConfigurationChange,
  type KafkaTopicConfigurationEntry,
  type SecureConnectionInput,
} from "../contracts";
import type {
  KafkaClusterMetadata,
  KafkaConnectionPort,
  KafkaConsumerGroupInventory,
  KafkaMessageStream,
  KafkaLatencyProbeMeasurement,
} from "../application";

import {
  KafkaEngineFailure,
  mapKafkaAdminFailure,
  mapKafkaConsumerGroupFailure,
  mapKafkaTopicConfigurationFailure,
  normalizeKafkaError,
} from "./failure";
import { OAuthEndpointResponseError, requestOAuthToken } from "./oauth";
import { PlatformaticAdminFactory } from "./platformatic-admin";
import { PlatformaticConsumerFactory } from "./platformatic-consumer";
import { PlatformaticLatencyProbe } from "./platformatic-latency";
import type {
  ConnectionCheck,
  KafkaAdminPort,
  KafkaClientInput,
  KafkaConsumerFactory,
  KafkaLatencyProbePort,
  KafkaEngineConnection,
  KafkaConnectionTestResult,
  KafkaRawMessage,
  KafkaRawMessageStream,
  OAuthToken,
  OAuthTokenProvider,
  StreamSkopeKafkaEngineOptions,
} from "./types";

const DEFAULT_OPERATION_TIMEOUT_MS = 5_000;

class OperationAborted extends Error {
  constructor() {
    super("Kafka engine operation was aborted.");
    this.name = "OperationAborted";
  }
}

function boundedOperation<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) {
    return Promise.reject(new OperationAborted());
  }
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => {
      reject(new OperationAborted());
    };
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(normalizeKafkaError(error));
      },
    );
  });
}

function cancelledFailure(stage: HostErrorStage, target: string): KafkaEngineFailure {
  return new KafkaEngineFailure({
    code: "CANCELLED",
    recovery: "Retry the operation when the current connection change is complete.",
    retryable: true,
    stage,
    summary: "The Kafka operation was cancelled.",
    target,
  });
}

function timeoutFailure(stage: HostErrorStage, target: string): KafkaEngineFailure {
  return new KafkaEngineFailure({
    code: "TIMEOUT",
    recovery:
      stage === "oauth"
        ? "Verify the OAuth endpoint and retry."
        : "Verify the broker endpoints and network path, then retry.",
    retryable: true,
    stage,
    summary:
      stage === "oauth"
        ? "OAuth token acquisition timed out."
        : "Kafka broker metadata access timed out.",
    target,
  });
}

function oauthFailure(error: unknown, target: string): KafkaEngineFailure {
  if (error instanceof KafkaEngineFailure) {
    return error;
  }
  if (error instanceof OAuthEndpointResponseError) {
    return new KafkaEngineFailure({
      cause: error,
      code:
        error.status === 400 || error.status === 401 || error.status === 403
          ? "OAUTH_REJECTED"
          : "OAUTH_UNREACHABLE",
      recovery: "Check the token endpoint, client identifier, client secret and required scope.",
      retryable: error.status >= 500,
      stage: "oauth",
      summary:
        error.status === 400 || error.status === 401 || error.status === 403
          ? "OAuth credentials were rejected."
          : "The OAuth token endpoint did not complete the request.",
      target,
    });
  }
  return new KafkaEngineFailure({
    cause: error,
    code: "OAUTH_UNREACHABLE",
    recovery: "Verify the OAuth endpoint, TLS trust and local network path, then retry.",
    retryable: true,
    stage: "oauth",
    summary: "The OAuth token endpoint could not be reached.",
    target,
  });
}

function withCleanupFailure(
  failure: KafkaEngineFailure,
  cleanupCause: unknown,
): KafkaEngineFailure {
  return new KafkaEngineFailure({
    cause: failure.cause,
    cleanupCause,
    code: failure.code,
    recovery: failure.recovery,
    retryable: failure.retryable,
    stage: failure.stage,
    summary: failure.message,
    ...(failure.target === undefined ? {} : { target: failure.target }),
  });
}

function decode(buffer: Buffer | undefined): string | null {
  return buffer === undefined ? null : buffer.toString("utf8");
}

function decodePrefix(buffer: Buffer | undefined, maximumBytes: number): string {
  if (buffer === undefined) {
    return "";
  }
  const decoded = buffer.subarray(0, maximumBytes).toString("utf8");
  if (utf8ByteLength(decoded) <= maximumBytes) {
    return decoded;
  }
  const retained: string[] = [];
  let retainedBytes = 0;
  for (const character of decoded) {
    const characterBytes = Buffer.byteLength(character, "utf8");
    if (retainedBytes + characterBytes > maximumBytes) {
      break;
    }
    retained.push(character);
    retainedBytes += characterBytes;
  }
  return retained.join("");
}

function translateHeaders(headers: ReadonlyMap<Buffer, Buffer>): {
  readonly headers: Readonly<Record<string, string>>;
  readonly truncated: boolean;
} {
  const translated: Array<readonly [string, string]> = [];
  let truncated = headers.size > KAFKA_MESSAGE_LIMITS.headerCount;
  for (const [key, value] of [...headers].slice(0, KAFKA_MESSAGE_LIMITS.headerCount)) {
    if (
      key.byteLength > KAFKA_MESSAGE_LIMITS.headerKeyBytes ||
      value.byteLength > KAFKA_MESSAGE_LIMITS.headerValueBytes
    ) {
      truncated = true;
    }
    translated.push([
      decodePrefix(key, KAFKA_MESSAGE_LIMITS.headerKeyBytes),
      decodePrefix(value, KAFKA_MESSAGE_LIMITS.headerValueBytes),
    ]);
  }
  return { headers: Object.fromEntries(translated), truncated };
}

function translateMessage(raw: KafkaRawMessage, expectedTopic: string): KafkaMessage {
  if (raw.topic !== expectedTopic) {
    throw new Error(`Kafka returned a record for unexpected topic ${raw.topic}.`);
  }
  if (!Number.isSafeInteger(raw.partition) || raw.partition < 0 || raw.offset < 0n) {
    throw new Error("Kafka returned invalid partition or offset metadata.");
  }
  const timestamp = Number(raw.timestamp);
  if (
    !Number.isSafeInteger(timestamp) ||
    timestamp < -8_640_000_000_000_000 ||
    timestamp > 8_640_000_000_000_000
  ) {
    throw new Error("Kafka returned an invalid record timestamp.");
  }

  const keyBytes = raw.key?.byteLength ?? 0;
  const payloadBytes = raw.value?.byteLength ?? 0;
  const key = decode(raw.key);
  const payload = decode(raw.value);
  const originalByteSize = Math.max(
    keyBytes + payloadBytes,
    utf8ByteLength(key) + utf8ByteLength(payload),
  );
  const contentTruncated = originalByteSize > KAFKA_MESSAGE_LIMITS.messageBytes;
  const translatedHeaders = translateHeaders(raw.headers);
  return {
    headers: translatedHeaders.headers,
    id: `${raw.topic}:${raw.partition}:${raw.offset.toString()}`,
    key: contentTruncated
      ? key !== null && utf8ByteLength(key) <= KAFKA_MESSAGE_LIMITS.previewBytes
        ? key
        : null
      : key,
    offset: raw.offset.toString(),
    originalByteSize,
    partition: raw.partition,
    payload: contentTruncated ? null : payload,
    preview: decodePrefix(raw.value, KAFKA_MESSAGE_LIMITS.previewBytes),
    timestamp: new Date(timestamp).toISOString(),
    topic: raw.topic,
    truncated: contentTruncated || translatedHeaders.truncated,
  };
}

class TranslatedKafkaMessageStream implements KafkaMessageStream {
  private closePromise: Promise<void> | undefined;

  constructor(
    private readonly rawStream: KafkaRawMessageStream,
    private readonly expectedTopic: string,
    private readonly target: string,
    private readonly onClose: () => void,
  ) {}

  close(): Promise<void> {
    this.closePromise ??= this.rawStream.close().finally(this.onClose);
    return this.closePromise;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    try {
      for await (const raw of this.rawStream) {
        yield translateMessage(raw, this.expectedTopic);
      }
    } catch (error) {
      throw mapKafkaAdminFailure(error, this.target);
    }
  }
}

class ActiveKafkaEngineConnection implements KafkaEngineConnection {
  private closePromise: Promise<void> | undefined;
  private readonly streams = new Set<TranslatedKafkaMessageStream>();

  constructor(
    private readonly admin: KafkaAdminPort,
    private readonly clientInput: KafkaClientInput,
    private readonly consumerFactory: KafkaConsumerFactory,
    private readonly latencyProbe: KafkaLatencyProbePort,
    private readonly lifecycleController: AbortController,
    private readonly operationTimeoutMs: number,
    private readonly services: SecureConnectionInput["services"],
    private readonly target: string,
  ) {}

  alterTopicConfiguration(
    topic: string,
    changes: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
    cancellationSignal?: AbortSignal,
  ): Promise<void> {
    const target = `${this.target} / ${topic}`;
    return this.runAdminOperation(
      () => this.admin.alterTopicConfiguration(topic, changes, validateOnly),
      cancellationSignal,
      target,
      mapKafkaTopicConfigurationFailure,
      "kafka",
    );
  }

  close(): Promise<void> {
    this.lifecycleController.abort();
    this.closePromise ??= this.closeResources();
    return this.closePromise;
  }

  createAcl(acl: KafkaAclBinding, cancellationSignal?: AbortSignal): Promise<void> {
    return this.runAdminOperation(
      () =>
        this.admin.createAcl === undefined
          ? Promise.reject(new Error("Kafka ACL creation is unavailable."))
          : this.admin.createAcl(acl),
      cancellationSignal,
      `${this.target} / ${acl.resourceType} ${acl.resourceName}`,
      mapKafkaAdminFailure,
      "kafka",
    );
  }

  deleteAcl(acl: KafkaAclBinding, cancellationSignal?: AbortSignal): Promise<void> {
    return this.runAdminOperation(
      () =>
        this.admin.deleteAcl === undefined
          ? Promise.reject(new Error("Kafka ACL deletion is unavailable."))
          : this.admin.deleteAcl(acl),
      cancellationSignal,
      `${this.target} / ${acl.resourceType} ${acl.resourceName}`,
      mapKafkaAdminFailure,
      "kafka",
    );
  }

  clusterServiceContext(
    service: "redpandaAdmin" | "schemaRegistry",
  ): import("../application").KafkaClusterServiceContext | null {
    const endpoint = this.services?.[service];
    if (endpoint === undefined) {
      return null;
    }
    return {
      baseUrl: endpoint.baseUrl,
      caPem: this.clientInput.caPem,
      authorization: async (): Promise<string | undefined> => {
        if (endpoint.authentication === "none") {
          return undefined;
        }
        const token = await this.clientInput.oauthTokenProvider?.();
        if (token === undefined) {
          throw new Error("OAuth service authentication has no active token provider.");
        }
        return `Bearer ${token.value}`;
      },
    };
  }

  describeTopicConfiguration(
    topic: string,
    cancellationSignal?: AbortSignal,
  ): Promise<readonly KafkaTopicConfigurationEntry[]> {
    const target = `${this.target} / ${topic}`;
    return this.runAdminOperation(
      () => this.admin.describeTopicConfiguration(topic),
      cancellationSignal,
      target,
      mapKafkaTopicConfigurationFailure,
      "kafka",
    );
  }

  describeBrokerConfiguration(
    brokerId: number,
    cancellationSignal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]> {
    const target = `${this.target} / broker ${String(brokerId)}`;
    return this.runAdminOperation(
      () => this.admin.describeBrokerConfiguration(brokerId),
      cancellationSignal,
      target,
      mapKafkaAdminFailure,
      "kafka",
    );
  }

  describeClusterMetadata(cancellationSignal?: AbortSignal): Promise<KafkaClusterMetadata> {
    return this.runAdminOperation(
      () => this.admin.describeClusterMetadata(),
      cancellationSignal,
      this.target,
      mapKafkaAdminFailure,
      "broker",
    );
  }

  describeConsumerGroup(
    groupId: string,
    cancellationSignal?: AbortSignal,
  ): Promise<KafkaConsumerGroupDetails> {
    const target = `${this.target} / consumer group ${groupId}`;
    return this.runAdminOperation(
      () => this.admin.describeConsumerGroup(groupId),
      cancellationSignal,
      target,
      mapKafkaConsumerGroupFailure,
      "kafka",
    );
  }

  listAcls(cancellationSignal?: AbortSignal): Promise<readonly KafkaAclBinding[]> {
    return this.runAdminOperation(
      () =>
        this.admin.listAcls === undefined
          ? Promise.reject(new Error("Kafka ACL inventory is unavailable."))
          : this.admin.listAcls(),
      cancellationSignal,
      `${this.target} / ACLs`,
      mapKafkaAdminFailure,
      "kafka",
    );
  }

  async openMessageStream(
    request: KafkaFetchRequest,
    cancellationSignal: AbortSignal,
  ): Promise<KafkaMessageStream> {
    let parsedRequest: KafkaFetchRequest;
    try {
      parsedRequest = parseKafkaFetchRequest(request, "request");
    } catch (error) {
      throw new KafkaEngineFailure({
        cause: error,
        code: "VALIDATION",
        recovery: "Select a valid Kafka topic and retry.",
        retryable: false,
        stage: "validation",
        summary:
          error instanceof HostContractValidationError
            ? `The Kafka fetch request is invalid: ${error.message}.`
            : "The Kafka fetch request is invalid.",
        target: request.topic,
      });
    }

    const timeoutController = new AbortController();
    const timeout = setTimeout(() => {
      timeoutController.abort();
    }, this.operationTimeoutMs);
    timeout.unref();
    const signal = AbortSignal.any([
      this.lifecycleController.signal,
      cancellationSignal,
      timeoutController.signal,
    ]);
    const target = `${this.target} / ${parsedRequest.topic}`;
    const operation = this.consumerFactory.open({
      ...this.clientInput,
      groupId: `streamskope-${randomUUID()}`,
      request: parsedRequest,
    });
    try {
      const rawStream = await boundedOperation(operation, signal);
      const translated = new TranslatedKafkaMessageStream(
        rawStream,
        parsedRequest.topic,
        target,
        () => {
          this.streams.delete(translated);
        },
      );
      this.streams.add(translated);
      return translated;
    } catch (error) {
      if (error instanceof OperationAborted) {
        let cleanupFailure: unknown;
        let lateStream: KafkaRawMessageStream | undefined;
        try {
          lateStream = await operation;
        } catch {
          lateStream = undefined;
        }
        try {
          if (lateStream !== undefined) {
            await lateStream.close();
          }
        } catch (lateError) {
          cleanupFailure = lateError;
        }
        const failure =
          cancellationSignal.aborted || this.lifecycleController.signal.aborted
            ? cancelledFailure("broker", target)
            : timeoutFailure("broker", target);
        throw cleanupFailure === undefined ? failure : withCleanupFailure(failure, cleanupFailure);
      }
      throw mapKafkaAdminFailure(error, target);
    } finally {
      clearTimeout(timeout);
    }
  }

  async runLatencyProbe(
    request: KafkaLatencyProbeRequest,
    runId: string,
    cancellationSignal: AbortSignal,
  ): Promise<KafkaLatencyProbeMeasurement> {
    let parsedRequest: KafkaLatencyProbeRequest;
    try {
      parsedRequest = parseKafkaLatencyProbeRequest(request, "request");
    } catch (error) {
      throw new KafkaEngineFailure({
        cause: error,
        code: "VALIDATION",
        recovery: "Choose a valid topic, message count, timeout, and acknowledgement mode.",
        retryable: false,
        stage: "validation",
        summary: "The Kafka latency request is invalid.",
        target: request.topic,
      });
    }
    const signal = AbortSignal.any([this.lifecycleController.signal, cancellationSignal]);
    try {
      return await this.latencyProbe.run(
        {
          ...this.clientInput,
          request: parsedRequest,
          runId,
        },
        signal,
      );
    } catch (error) {
      if (error instanceof KafkaEngineFailure) {
        throw error;
      }
      if (signal.aborted || (error instanceof Error && error.name === "AbortError")) {
        throw cancelledFailure("kafka", `${this.target} / ${parsedRequest.topic}`);
      }
      throw mapKafkaAdminFailure(error, `${this.target} / ${parsedRequest.topic}`);
    }
  }

  async listTopics(cancellationSignal?: AbortSignal): Promise<readonly string[]> {
    return this.runAdminOperation(
      () => this.admin.listTopics(),
      cancellationSignal,
      this.target,
      mapKafkaAdminFailure,
      "broker",
    );
  }

  listConsumerGroups(cancellationSignal?: AbortSignal): Promise<KafkaConsumerGroupInventory> {
    return this.runAdminOperation(
      () => this.admin.listConsumerGroups(),
      cancellationSignal,
      `${this.target} / consumer groups`,
      mapKafkaConsumerGroupFailure,
      "kafka",
    );
  }

  private async runAdminOperation<T>(
    start: () => Promise<T>,
    cancellationSignal: AbortSignal | undefined,
    target: string,
    mapFailure: (error: unknown, target: string) => KafkaEngineFailure,
    stage: HostErrorStage,
  ): Promise<T> {
    const timeoutController = new AbortController();
    const timeout = setTimeout(() => {
      timeoutController.abort();
    }, this.operationTimeoutMs);
    timeout.unref();
    const signals = [this.lifecycleController.signal, timeoutController.signal];
    if (cancellationSignal !== undefined) {
      signals.push(cancellationSignal);
    }
    const signal = AbortSignal.any(signals);

    try {
      const operation = Promise.resolve().then(start);
      return await boundedOperation(operation, signal);
    } catch (error) {
      if (error instanceof OperationAborted) {
        throw cancellationSignal?.aborted === true || this.lifecycleController.signal.aborted
          ? cancelledFailure(stage, target)
          : timeoutFailure(stage, target);
      }
      throw mapFailure(error, target);
    } finally {
      clearTimeout(timeout);
    }
  }

  private async closeResources(): Promise<void> {
    const results = await Promise.allSettled([
      ...[...this.streams].map(async (stream) => stream.close()),
      this.admin.close(),
    ]);
    const failures = results.flatMap((result) =>
      result.status === "rejected" ? [result.reason as unknown] : [],
    );
    if (failures.length > 0) {
      throw new AggregateError(failures, "Kafka connection resources did not close cleanly.");
    }
  }
}

interface EstablishedConnection {
  readonly connection: KafkaEngineConnection;
  readonly result: KafkaConnectionTestResult;
}

export class StreamSkopeKafkaEngine implements KafkaConnectionPort {
  private readonly adminFactory;
  private readonly consumerFactory;
  private readonly operationTimeoutMs: number;
  private readonly latencyProbe;
  private readonly tokenRequester;

  constructor(options: StreamSkopeKafkaEngineOptions = {}) {
    this.adminFactory = options.adminFactory ?? new PlatformaticAdminFactory();
    this.consumerFactory = options.consumerFactory ?? new PlatformaticConsumerFactory();
    this.latencyProbe = options.latencyProbe ?? new PlatformaticLatencyProbe();
    this.operationTimeoutMs = options.operationTimeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
    this.tokenRequester = options.requestOAuthToken ?? requestOAuthToken;
    if (!Number.isSafeInteger(this.operationTimeoutMs) || this.operationTimeoutMs < 1) {
      throw new RangeError("Kafka engine operation timeout must be a positive safe integer.");
    }
  }

  async openConnection(
    connection: SecureConnectionInput,
    cancellationSignal: AbortSignal,
  ): Promise<KafkaEngineConnection> {
    return (await this.establishConnection(connection, cancellationSignal)).connection;
  }

  async testConnection(
    connection: SecureConnectionInput,
    cancellationSignal?: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    const established = await this.establishConnection(connection, cancellationSignal);
    try {
      await established.connection.close();
    } catch (error) {
      throw new KafkaEngineFailure({
        cause: error,
        code: "INTERNAL",
        recovery: "Retry after verifying that no previous Kafka connection test remains active.",
        retryable: true,
        stage: "internal",
        summary: "The temporary Kafka client could not be closed.",
        target: connection.brokers.join(", "),
      });
    }
    return established.result;
  }

  private async acquireOAuthToken(
    connection: SecureConnectionInput & {
      readonly oauth: NonNullable<SecureConnectionInput["oauth"]>;
    },
    signal: AbortSignal,
    cancellationSignal?: AbortSignal,
  ): Promise<OAuthToken> {
    const target = connection.oauth.tokenEndpoint;
    try {
      return await boundedOperation(
        this.tokenRequester({
          caPem: connection.tls.caPem,
          clientId: connection.oauth.clientId,
          clientSecret: connection.oauth.clientSecret,
          scope: connection.oauth.scope,
          signal,
          tokenEndpoint: target,
        }),
        signal,
      );
    } catch (error) {
      if (error instanceof OperationAborted) {
        throw cancellationSignal?.aborted === true
          ? cancelledFailure("oauth", target)
          : timeoutFailure("oauth", target);
      }
      throw oauthFailure(error, target);
    }
  }

  private createOAuthTokenProvider(
    connection: SecureConnectionInput & {
      readonly oauth: NonNullable<SecureConnectionInput["oauth"]>;
    },
    initialToken: OAuthToken,
    lifecycleSignal: AbortSignal,
  ): OAuthTokenProvider {
    let currentToken = initialToken;
    let refresh: Promise<OAuthToken> | undefined;

    return async (): Promise<OAuthToken> => {
      if (currentToken.expiresAt === undefined || currentToken.expiresAt > Date.now()) {
        return currentToken;
      }
      const refreshController = new AbortController();
      const timeout = setTimeout(() => {
        refreshController.abort();
      }, this.operationTimeoutMs);
      timeout.unref();
      refresh ??= this.acquireOAuthToken(
        connection,
        AbortSignal.any([lifecycleSignal, refreshController.signal]),
        lifecycleSignal,
      );
      try {
        currentToken = await refresh;
        return currentToken;
      } finally {
        clearTimeout(timeout);
        refresh = undefined;
      }
    };
  }

  private async establishConnection(
    connection: SecureConnectionInput,
    cancellationSignal?: AbortSignal,
  ): Promise<EstablishedConnection> {
    const issue = validateSecureConnectionInput(connection)[0];
    if (issue !== undefined) {
      throw new KafkaEngineFailure({
        code: "VALIDATION",
        recovery: issue.message,
        retryable: false,
        stage: "validation",
        summary: "The Kafka connection configuration is invalid.",
        target: issue.field,
      });
    }

    const timeoutController = new AbortController();
    const lifecycleController = new AbortController();
    const timeout = setTimeout(() => {
      timeoutController.abort();
    }, this.operationTimeoutMs);
    timeout.unref();
    const signals = [lifecycleController.signal, timeoutController.signal];
    if (cancellationSignal !== undefined) {
      signals.push(cancellationSignal);
    }
    const signal = AbortSignal.any(signals);
    let stage: HostErrorStage = connection.oauth === undefined ? "broker" : "oauth";
    let target = connection.oauth?.tokenEndpoint ?? connection.brokers.join(", ");
    let activeConnection: KafkaEngineConnection | undefined;

    try {
      let oauthTokenProvider: OAuthTokenProvider | undefined;
      if (connection.oauth !== undefined) {
        const oauthConnection = {
          ...connection,
          oauth: connection.oauth,
        };
        const initialToken = await this.acquireOAuthToken(
          oauthConnection,
          signal,
          cancellationSignal,
        );
        oauthTokenProvider = this.createOAuthTokenProvider(
          oauthConnection,
          initialToken,
          lifecycleController.signal,
        );
      }

      stage = "broker";
      target = connection.brokers.join(", ");
      const clientInput: KafkaClientInput = {
        brokers: connection.brokers,
        caPem: connection.tls.caPem,
        ...(oauthTokenProvider === undefined ? {} : { oauthTokenProvider }),
        operationTimeoutMs: this.operationTimeoutMs,
      };
      const admin = this.adminFactory.create(clientInput);
      activeConnection = new ActiveKafkaEngineConnection(
        admin,
        clientInput,
        this.consumerFactory,
        this.latencyProbe,
        lifecycleController,
        this.operationTimeoutMs,
        connection.services,
        target,
      );
      const topics = await activeConnection.listTopics(cancellationSignal);
      const checks: ConnectionCheck[] = [
        ...(connection.oauth === undefined ? [] : (["oauth"] as const)),
        "tls",
        "kafka-authentication",
        "metadata",
      ];
      return {
        connection: activeConnection,
        result: { checks, topicCount: topics.length },
      };
    } catch (error) {
      lifecycleController.abort();
      let cleanupFailure: unknown;
      if (activeConnection !== undefined) {
        try {
          await activeConnection.close();
        } catch (cleanupError) {
          cleanupFailure = cleanupError;
        }
      }
      const failure =
        error instanceof KafkaEngineFailure
          ? error
          : stage === "oauth"
            ? oauthFailure(error, target)
            : mapKafkaAdminFailure(error, target);
      throw cleanupFailure === undefined ? failure : withCleanupFailure(failure, cleanupFailure);
    } finally {
      clearTimeout(timeout);
    }
  }
}
