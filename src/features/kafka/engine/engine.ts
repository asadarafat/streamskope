import { randomUUID } from "node:crypto";

import type { KafkaRecordLocator } from "../contracts/record-locator";
import {
  KafkaReadOpenCleanupError,
  type KafkaReadOpenCleanup,
} from "../application/read-open-cleanup";
import { KafkaRecordLocatorError } from "../application/record-locator-errors";
import {
  HostContractValidationError,
  parseKafkaFetchRequest,
  parseKafkaLatencyProbeRequest,
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
  KafkaClusterServiceContext,
} from "../application";
import { classifyConnectionFailure } from "../application/connection-diagnostics";
import { KafkaReadCheckpointError, type KafkaReadCheckpoint } from "../application/read-checkpoint";

import { serviceConnectionContext } from "./service-connection-context";
import { testClusterServices } from "./service-connection-test";
import { NodeBoundedJsonHttp } from "./bounded-json-http";
import { PlatformaticOffsetReset } from "./platformatic-offset-reset";
import { PlatformaticReviewedWrites } from "./platformatic-writes";
import { PlatformaticTopicAdministration } from "./platformatic-topic-administration";
import { TranslatedKafkaMessageStream } from "./translated-message-stream";
import { translateKafkaRecord } from "./message-record";
import {
  KafkaEngineFailure,
  mapKafkaAdminFailure,
  normalizeKafkaError,
  mapKafkaConsumerGroupFailure,
  mapKafkaTopicConfigurationFailure,
} from "./failure";
import { abortableOperation } from "./abortable-operation";
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
  KafkaRawMessageStream,
  KafkaRawMessage,
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

function openCleanupDiagnostic(
  error: KafkaReadOpenCleanupError,
  failure: KafkaEngineFailure,
): KafkaReadOpenCleanupError {
  return Object.assign(error, {
    code: failure.code,
    stage: failure.stage,
    recovery: failure.recovery,
    retryable: failure.retryable,
    target: failure.target,
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
  const category = classifyConnectionFailure(error);
  if (category === "tls-trust" || category === "tls-client") {
    return new KafkaEngineFailure({
      cause: error,
      code: "TLS_TRUST",
      recovery:
        "Verify the OAuth endpoint hostname, certificate validity and issuing CA in this profile's trust material.",
      retryable: false,
      stage: "tls",
      summary: "OAuth endpoint certificate validation failed.",
      target,
    });
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

class ActiveKafkaEngineConnection implements KafkaEngineConnection {
  topicAdministrationSnapshot(
    topic: string,
  ): Promise<import("../contracts/topic-administration").TopicAdministrationSnapshot> {
    return this.topicAdministration.snapshot(topic);
  }
  applyTopicAdministration(
    input: import("../contracts/topic-administration").TopicAdministrationInput,
    baseline: import("../contracts/topic-administration").TopicAdministrationSnapshot,
  ): Promise<import("../contracts/topic-administration").TopicAdministrationOutcome> {
    return this.topicAdministration.apply(input, baseline);
  }
  offsetResetSnapshot(
    input: import("../contracts/offset-reset").OffsetResetInput,
  ): Promise<import("../contracts/offset-reset").OffsetResetSnapshot> {
    return new PlatformaticOffsetReset(this.clientInput, this.lifecycleController.signal).snapshot(
      input,
    );
  }
  offsetResetExamples(
    input: import("../contracts/offset-reset").OffsetResetInput,
  ): Promise<
    Pick<import("../contracts/offset-reset").OffsetResetReview, "examples" | "exampleStatus">
  > {
    return new PlatformaticOffsetReset(this.clientInput, this.lifecycleController.signal).examples(
      input,
    );
  }
  resetGroupOffset(
    groupId: string,
    target: import("../contracts/offset-reset").OffsetResetTarget,
  ): Promise<import("../contracts/offset-reset").OffsetResetResult> {
    return new PlatformaticOffsetReset(this.clientInput, this.lifecycleController.signal).apply(
      groupId,
      target,
    );
  }
  describeTopicIdentity(
    topic: string,
  ): Promise<import("../contracts/reviewed-writes").KafkaWriteDestination> {
    // Review performs bounded metadata reads only; it never produces a record.
    return this.reviewWrite({
      kind: "record",
      topic,
      partition: 0,
      record: { state: "complete", encoding: "base64", key: null, value: null, headers: [] },
    });
  }
  reviewWrite(
    input: import("../contracts").KafkaWriteInput,
  ): Promise<import("../contracts/reviewed-writes").KafkaWriteDestination> {
    return new PlatformaticReviewedWrites(this.clientInput, this.lifecycleController.signal).review(
      input,
    );
  }
  applyWrite(
    input: import("../contracts").KafkaWriteInput,
  ): Promise<import("../contracts").KafkaWriteOutcome> {
    return new PlatformaticReviewedWrites(this.clientInput, this.lifecycleController.signal).apply(
      input,
    );
  }
  private closePromise: Promise<void> | undefined;
  private readonly streams = new Set<TranslatedKafkaMessageStream>();
  private readonly pendingReaderOpens = new Set<Promise<void>>();
  private readonly lateReaders = new Set<KafkaReadOpenCleanup>();
  private readonly serviceContexts = new Map<string, KafkaClusterServiceContext>();
  private readonly topicAdministration: PlatformaticTopicAdministration;

  constructor(
    private readonly admin: KafkaAdminPort,
    private readonly clientInput: KafkaClientInput,
    private readonly consumerFactory: KafkaConsumerFactory,
    private readonly latencyProbe: KafkaLatencyProbePort,
    private readonly lifecycleController: AbortController,
    private readonly operationTimeoutMs: number,
    private readonly services: SecureConnectionInput["services"],
    private readonly tokenRequester: NonNullable<
      StreamSkopeKafkaEngineOptions["requestOAuthToken"]
    >,
    private readonly target: string,
    private readonly protectRecord: (message: KafkaMessage) => KafkaMessage,
    private readonly prepareRecord: StreamSkopeKafkaEngineOptions["prepareRecord"],
  ) {
    this.topicAdministration = new PlatformaticTopicAdministration(
      clientInput,
      lifecycleController.signal,
    );
  }

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
    service: "connect" | "redpandaAdmin" | "schemaRegistry",
  ): KafkaClusterServiceContext | null {
    const endpoint = this.services?.[service];
    if (endpoint === undefined) return null;
    const existing = this.serviceContexts.get(service);
    if (existing !== undefined) return existing;
    const context = serviceConnectionContext(endpoint, this.clientInput, {
      lifecycleSignal: this.lifecycleController.signal,
      operationTimeoutMs: this.operationTimeoutMs,
      requestOAuthToken: this.tokenRequester,
    });
    this.serviceContexts.set(service, context);
    return context;
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

  observeTopicHealth(
    topic: string,
    signal?: AbortSignal,
  ): Promise<import("../contracts/observations").TopicHealth> {
    return this.runAdminOperation(
      (requestSignal) => {
        if (!this.admin.observeTopicHealth) throw new Error("Topic health is unavailable.");
        return this.admin.observeTopicHealth(topic, requestSignal);
      },
      signal,
      this.target,
      mapKafkaAdminFailure,
      "broker",
    );
  }

  observeConsumerGroup(
    groupId: string,
    topic: string,
    partitions: readonly number[],
    signal?: AbortSignal,
  ): Promise<import("../contracts/observations").ObservationGroupHealth> {
    return this.runAdminOperation(
      (requestSignal) => {
        if (!this.admin.observeConsumerGroup)
          throw new Error("Selected-topic group observations are unavailable.");
        return this.admin.observeConsumerGroup(groupId, topic, partitions, requestSignal);
      },
      signal,
      `${this.target} / ${groupId}`,
      mapKafkaConsumerGroupFailure,
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
    checkpoint?: KafkaReadCheckpoint,
    expectedLocator?: KafkaRecordLocator,
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
    const prepared = new WeakMap<KafkaRawMessage, Promise<KafkaMessage>>();
    const preparationController = new AbortController();
    const recordSignal = AbortSignal.any([
      this.lifecycleController.signal,
      cancellationSignal,
      preparationController.signal,
    ]);
    const prepare = (raw: KafkaRawMessage, consumerSignal?: AbortSignal): Promise<KafkaMessage> => {
      const previous = prepared.get(raw);
      if (previous) return previous;
      const workSignal = consumerSignal
        ? AbortSignal.any([recordSignal, consumerSignal])
        : recordSignal;
      const operation = (async (): Promise<KafkaMessage> => {
        workSignal.throwIfAborted();
        const translated = translateKafkaRecord(raw, parsedRequest.topic);
        const projected = this.prepareRecord
          ? await this.prepareRecord(
              translated,
              this.clusterServiceContext("schemaRegistry"),
              workSignal,
            )
          : translated;
        workSignal.throwIfAborted();
        return this.protectRecord(projected);
      })();
      prepared.set(raw, operation);
      return operation;
    };
    let settleOpening!: () => void;
    const openingSettled = new Promise<void>((resolve) => {
      settleOpening = resolve;
    });
    this.pendingReaderOpens.add(openingSettled);
    let operation: Promise<KafkaRawMessageStream> | undefined;
    try {
      operation = this.consumerFactory.open({
        ...(checkpoint === undefined ? {} : { checkpoint }),
        ...(expectedLocator === undefined ? {} : { expectedLocator }),
        prepareRecord: prepare,
        ...this.clientInput,
        groupId: `streamskope-${randomUUID()}`,
        request: parsedRequest,
        signal,
      });
      const rawStream = await abortableOperation(operation, signal, () => new OperationAborted());
      const translated = new TranslatedKafkaMessageStream(
        rawStream,
        target,
        () => {
          this.streams.delete(translated);
        },
        prepare,
        preparationController,
      );
      this.streams.add(translated);
      return translated;
    } catch (error) {
      preparationController.abort();
      if (error instanceof OperationAborted) {
        let cleanupFailure: unknown;
        let lateCleanup: KafkaReadOpenCleanup | undefined;
        let lateStream: KafkaRawMessageStream | undefined;
        let failedOpen: KafkaReadOpenCleanupError | undefined;
        try {
          lateStream = await operation;
        } catch (openingFailure) {
          lateStream = undefined;
          if (openingFailure instanceof KafkaReadOpenCleanupError) failedOpen = openingFailure;
        }
        try {
          if (lateStream !== undefined) {
            // Preserve the original connection owner when cancellation wins the open race.
            const owned = lateStream;
            this.lateReaders.add(owned);
            lateCleanup = {
              close: async (): Promise<void> => {
                await owned.close();
                this.lateReaders.delete(owned);
              },
            };
            await lateCleanup.close();
          }
        } catch (lateError) {
          cleanupFailure = lateError;
        }
        const failure =
          cancellationSignal.aborted || this.lifecycleController.signal.aborted
            ? cancelledFailure("broker", target)
            : timeoutFailure("broker", target);
        if (failedOpen !== undefined)
          throw openCleanupDiagnostic(
            this.retainOpenCleanup(
              new KafkaReadOpenCleanupError(failure, failedOpen.cleanupCause, failedOpen.cleanup),
            ),
            failure,
          );
        if (cleanupFailure !== undefined && lateCleanup !== undefined)
          throw openCleanupDiagnostic(
            new KafkaReadOpenCleanupError(failure, cleanupFailure, lateCleanup),
            failure,
          );
        throw cleanupFailure === undefined ? failure : withCleanupFailure(failure, cleanupFailure);
      }
      if (error instanceof KafkaReadOpenCleanupError)
        throw openCleanupDiagnostic(
          this.retainOpenCleanup(error),
          mapKafkaAdminFailure(error.cause, target),
        );
      if (error instanceof KafkaRecordLocatorError) throw error;
      if (error instanceof KafkaReadCheckpointError)
        throw new KafkaEngineFailure({
          cause: error,
          code: "VALIDATION",
          recovery: "Start a new read to capture the current topic identity and retained offsets.",
          retryable: false,
          stage: "validation",
          summary: error.message,
          target,
        });
      throw mapKafkaAdminFailure(error, target);
    } finally {
      clearTimeout(timeout);
      this.pendingReaderOpens.delete(openingSettled);
      settleOpening();
    }
  }

  private retainOpenCleanup(error: KafkaReadOpenCleanupError): KafkaReadOpenCleanupError {
    const cleanup = error.cleanup;
    this.lateReaders.add(cleanup);
    return new KafkaReadOpenCleanupError(error.cause, error.cleanupCause, {
      close: async (): Promise<void> => {
        await cleanup.close();
        this.lateReaders.delete(cleanup);
      },
    });
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
    start: (signal: AbortSignal) => Promise<T>,
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
      const operation = Promise.resolve().then(() => start(signal));
      return await abortableOperation(operation, signal, () => new OperationAborted());
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
    // Opens admitted before revocation can still return a reader. Join their cleanup before
    // taking the final inventory, including late readers whose first close failed.
    await Promise.all([...this.pendingReaderOpens]);
    const results = await Promise.allSettled([
      ...[...this.streams].map(async (stream) => stream.close()),
      ...[...this.lateReaders].map(async (stream): Promise<void> => {
        await stream.close();
        this.lateReaders.delete(stream);
      }),
      this.admin.close(),
      this.topicAdministration.close(),
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
  private readonly protectRecord;
  private readonly prepareRecord;
  private readonly serviceHttp;

  constructor(options: StreamSkopeKafkaEngineOptions = {}) {
    this.prepareRecord = options.prepareRecord;
    this.protectRecord =
      options.protectRecord ?? ((message: KafkaMessage): KafkaMessage => message);
    this.adminFactory = options.adminFactory ?? new PlatformaticAdminFactory();
    this.consumerFactory = options.consumerFactory ?? new PlatformaticConsumerFactory();
    this.latencyProbe = options.latencyProbe ?? new PlatformaticLatencyProbe();
    this.operationTimeoutMs = options.operationTimeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
    this.tokenRequester = options.requestOAuthToken ?? requestOAuthToken;
    this.serviceHttp =
      options.serviceHttp ?? new NodeBoundedJsonHttp({ timeoutMs: this.operationTimeoutMs });
    if (!Number.isSafeInteger(this.operationTimeoutMs) || this.operationTimeoutMs < 1) {
      throw new RangeError("Kafka engine operation timeout must be a positive safe integer.");
    }
  }

  async openConnection(
    connection: SecureConnectionInput,
    cancellationSignal: AbortSignal,
  ): Promise<KafkaEngineConnection> {
    connection = structuredClone(connection);
    return (await this.establishConnection(connection, cancellationSignal)).connection;
  }

  async testConnection(
    connection: SecureConnectionInput,
    cancellationSignal?: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    // Own admitted credentials and diagnostic targets for this entire asynchronous operation.
    connection = structuredClone(connection);
    const established = await this.establishConnection(connection, cancellationSignal);
    let serviceChecks: readonly ConnectionCheck[] = [];
    let failure: unknown;
    try {
      serviceChecks = await testClusterServices(
        established.connection,
        this.serviceHttp,
        cancellationSignal ?? new AbortController().signal,
      );
    } catch (error) {
      failure = error;
    }
    try {
      await established.connection.close();
    } catch (error) {
      if (failure instanceof KafkaEngineFailure) throw withCleanupFailure(failure, error);
      throw new KafkaEngineFailure({
        cause: failure ?? error,
        cleanupCause: failure === undefined ? undefined : error,
        code: "INTERNAL",
        recovery: "Retry after verifying that no previous Kafka connection test remains active.",
        retryable: true,
        stage: "internal",
        summary: "The temporary Kafka client could not be closed.",
        target: connection.brokers.join(", "),
      });
    }
    if (failure !== undefined) throw normalizeKafkaError(failure);
    return { ...established.result, checks: [...established.result.checks, ...serviceChecks] };
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
      return await abortableOperation(
        this.tokenRequester({
          ...(connection.tls.enabled === true ? { caPem: connection.tls.caPem } : {}),
          clientId: connection.oauth.clientId,
          clientSecret: connection.oauth.clientSecret,
          scope: connection.oauth.scope,
          signal,
          tokenEndpoint: target,
        }),
        signal,
        () => new OperationAborted(),
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
      lifecycleSignal.throwIfAborted();
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
        lifecycleSignal.throwIfAborted();
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
      const clientInput: KafkaClientInput =
        connection.tls.enabled === true
          ? {
              brokers: connection.brokers,
              caPem: connection.tls.caPem,
              ...(connection.tls.clientIdentity === undefined
                ? {}
                : { clientIdentity: connection.tls.clientIdentity }),
              ...(oauthTokenProvider === undefined ? {} : { oauthTokenProvider }),
              ...(connection.sasl === undefined ? {} : { sasl: connection.sasl }),
              operationTimeoutMs: this.operationTimeoutMs,
              tlsEnabled: true,
            }
          : {
              brokers: connection.brokers,
              ...(oauthTokenProvider === undefined ? {} : { oauthTokenProvider }),
              ...(connection.sasl === undefined ? {} : { sasl: connection.sasl }),
              operationTimeoutMs: this.operationTimeoutMs,
              tlsEnabled: false,
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
        this.tokenRequester,
        target,
        this.protectRecord,
        this.prepareRecord,
      );
      const topics = await activeConnection.listTopics(cancellationSignal);
      const checks: ConnectionCheck[] = [
        ...(connection.oauth === undefined ? [] : (["oauth"] as const)),
        ...(connection.tls.enabled === true ? (["tls"] as const) : []),
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
