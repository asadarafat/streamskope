import {
  KAFKA_MESSAGE_LIMITS,
  type KafkaConfigurationEntry,
  type KafkaConsumerGroupDetails,
  type KafkaFetchRequest,
  type KafkaLatencyProbeRequest,
  type KafkaTopicConfigurationChange,
  type KafkaTopicConfigurationEntry,
  type SecureConnectionInput,
} from "../contracts";

import {
  ConnectionAttemptSupersededError,
  KafkaCleanupTimeoutError,
  NoActiveKafkaConnectionError,
} from "./session-errors";
import {
  abortSignals,
  cleanupFailures,
  closeCancelledConnection,
  ownedCleanupFailure,
} from "./session-lifecycle";
import type { RedpandaTransformLogResult } from "./transform-log-reader";
import type {
  KafkaActiveConnection,
  KafkaConsumptionObserver,
  KafkaConnectionPort,
  KafkaConnectionSnapshot,
  KafkaConnectionSnapshotListener,
  KafkaConnectionTestResult,
  KafkaClusterMetadata,
  KafkaClusterServiceContext,
  KafkaConsumerGroupInventory,
  KafkaMessageStream,
} from "./types";
import { KafkaSessionRequests } from "./session-requests";
import type { KafkaLatencyProbeMeasurement } from "./latency-types";
import type { KafkaReadCheckpoint } from "./read-checkpoint";
import {
  KafkaConnectionScopes,
  type AclReviewScope,
  type ObservationScope,
  type OffsetResetScope,
  type RecordReadScope,
  type TopicCatalogScope,
  type TopicAdministrationScope,
  type ReviewedWriteScope,
  type SchemaRegistryReviewScope,
} from "./connection-scope";

export { ConnectionAttemptSupersededError, NoActiveKafkaConnectionError } from "./session-errors";

const CONSUMPTION_CLEANUP_TIMEOUT_MS = 5_000;

interface PendingConsumption {
  readonly connection: KafkaActiveConnection;
  readonly controller: AbortController;
  readonly generation: number;
  operation: Promise<void> | undefined;
  openingStarted: boolean;
}

interface ActiveConsumption {
  closePromise: Promise<void> | undefined;
  readonly connection: KafkaActiveConnection;
  readonly connectionGeneration: number;
  readonly controller: AbortController;
  emptyTimer: ReturnType<typeof setTimeout> | undefined;
  readonly generation: number;
  readonly observer: KafkaConsumptionObserver;
  pump: Promise<void> | undefined;
  receivedMessage: boolean;
  readonly request: KafkaFetchRequest;
  readonly stream: KafkaMessageStream;
}

export class KafkaApplicationSession {
  private activeConnectionBrokers: readonly string[] | undefined;
  private activeConnectionTarget: string | undefined;
  private activeConnection: KafkaActiveConnection | undefined;
  private activeConsumption: ActiveConsumption | undefined;
  private connectionAttempt: Promise<void> | undefined;
  private connectionController: AbortController | undefined;
  private consumptionGeneration = 0;
  private consumptionStop: Promise<void> | undefined;
  private consumptionStopConnection: KafkaActiveConnection | undefined;
  private consumptionStopSettled = false;
  private consumptionStopCoveredByConnectionClose = true;
  private readonly closedConnections = new WeakSet<KafkaActiveConnection>();
  private readonly connectionClosures = new Map<
    KafkaActiveConnection,
    {
      readonly operation: Promise<void>;
      pending: boolean;
    }
  >();
  private pendingConsumption: PendingConsumption | undefined;
  private generation = 0;
  private readonly listeners = new Set<KafkaConnectionSnapshotListener>();
  private shutdownPromise: Promise<void> | undefined;
  private shuttingDown = false;
  private currentSnapshot: KafkaConnectionSnapshot = {
    connectionName: null,
    state: "disconnected",
  };

  private readonly requests: KafkaSessionRequests;
  private readonly scopes = new KafkaConnectionScopes(() => this.writeContext());

  constructor(private readonly connectionPort: KafkaConnectionPort) {
    this.requests = new KafkaSessionRequests(connectionPort, () => ({
      connection: this.activeConnection,
      connected: this.currentSnapshot.state === "connected",
      generation: this.generation,
      shuttingDown: this.shuttingDown,
    }));
  }

  writeContext(): {
    readonly connection: KafkaActiveConnection;
    readonly generation: number;
    readonly connectionName: string;
  } | null {
    return this.activeConnection !== undefined &&
      this.currentSnapshot.state === "connected" &&
      this.currentSnapshot.connectionName !== null
      ? {
          connection: this.activeConnection,
          generation: this.generation,
          connectionName: this.currentSnapshot.connectionName,
        }
      : null;
  }

  reviewedWriteScope(): ReviewedWriteScope | null {
    return this.scopes.reviewedWrite();
  }

  aclReviewScope(): AclReviewScope | null {
    return this.scopes.aclReview();
  }

  offsetResetScope(): OffsetResetScope | null {
    return this.scopes.offsetReset();
  }

  observationScope(): ObservationScope | null {
    return this.scopes.observation();
  }

  topicCatalogScope(): TopicCatalogScope | null {
    return this.scopes.topicCatalog();
  }

  topicAdministrationScope(): TopicAdministrationScope | null {
    return this.scopes.topicAdministration();
  }

  recordReadScope(): RecordReadScope | null {
    return this.scopes.recordRead();
  }

  schemaRegistryReviewScope(): SchemaRegistryReviewScope | null {
    return this.scopes.schemaRegistry();
  }

  activeConnectionContext(): {
    readonly connectionBrokers: readonly string[];
    readonly connectionName: string;
    readonly connectionTarget: string;
  } | null {
    const connectionName = this.currentSnapshot.connectionName;
    return this.currentSnapshot.state === "connected" &&
      connectionName !== null &&
      this.activeConnectionBrokers !== undefined &&
      this.activeConnectionTarget !== undefined
      ? {
          connectionBrokers: this.activeConnectionBrokers,
          connectionName,
          connectionTarget: this.activeConnectionTarget,
        }
      : null;
  }

  clusterServiceContext(
    service: "connect" | "redpandaAdmin" | "schemaRegistry",
  ): KafkaClusterServiceContext | null {
    return this.currentSnapshot.state === "connected"
      ? (this.activeConnection?.clusterServiceContext?.(service) ?? null)
      : null;
  }

  async connect(connection: SecureConnectionInput, externalSignal?: AbortSignal): Promise<void> {
    this.assertAvailable();
    externalSignal?.throwIfAborted();
    const consumptionStop =
      this.activeConsumption !== undefined ||
      this.pendingConsumption !== undefined ||
      this.consumptionStop !== undefined
        ? this.stopConsumption()
        : undefined;
    const generation = this.nextGeneration();
    const latencyRequests = this.requests.cancelForReconnect();
    this.connectionController?.abort();
    const controller = new AbortController();
    this.connectionController = controller;
    const previousConnection = this.activeConnection;
    this.activeConnection = undefined;
    this.activeConnectionBrokers = undefined;
    this.activeConnectionTarget = undefined;
    this.publish({
      connectionName: connection.name,
      state: "connecting",
    });

    const operation = this.establishConnection(
      connection,
      generation,
      controller,
      abortSignals(controller.signal, externalSignal),
      previousConnection,
      consumptionStop,
      latencyRequests,
    );
    this.connectionAttempt = operation;
    operation.then(
      () => {
        this.clearConnectionAttempt(operation, controller);
      },
      () => {
        this.clearConnectionAttempt(operation, controller);
      },
    );
    return operation;
  }

  disconnect(): Promise<void> {
    if (this.shuttingDown) {
      return this.shutdownPromise ?? Promise.resolve();
    }
    const consumptionStop =
      this.activeConsumption !== undefined ||
      this.pendingConsumption !== undefined ||
      this.consumptionStop !== undefined
        ? this.stopConsumption()
        : undefined;
    const generation = this.nextGeneration();
    const topicRequests = this.requests.cancelTopicRequests();
    const configurationRequests = this.requests.cancelAdministrativeRequests();
    const latencyRequests = this.requests.cancelLatencyRequests();
    this.connectionController?.abort();
    this.connectionController = undefined;
    const connectionAttempt = this.connectionAttempt;
    const activeConnection = this.activeConnection;
    this.activeConnection = undefined;
    this.activeConnectionBrokers = undefined;
    this.activeConnectionTarget = undefined;
    const connectionName = this.currentSnapshot.connectionName;
    this.publish({
      connectionName,
      state: "disconnecting",
    });

    return this.completeDisconnect(
      generation,
      connectionName,
      activeConnection,
      connectionAttempt,
      consumptionStop,
      topicRequests,
      configurationRequests,
      latencyRequests,
    );
  }

  shutdown(): Promise<void> {
    if (this.shutdownPromise !== undefined) {
      return this.shutdownPromise;
    }
    const consumptionStop =
      this.activeConsumption !== undefined ||
      this.pendingConsumption !== undefined ||
      this.consumptionStop !== undefined
        ? this.stopConsumption()
        : undefined;
    this.shuttingDown = true;
    const generation = this.nextGeneration();
    const topicRequests = this.requests.cancelTopicRequests();
    const configurationRequests = this.requests.cancelAdministrativeRequests();
    const latencyRequests = this.requests.cancelLatencyRequests();
    this.connectionController?.abort();
    this.connectionController = undefined;
    const temporaryTests = this.requests.cancelTests();
    const connectionAttempt = this.connectionAttempt;
    const activeConnection = this.activeConnection;
    this.activeConnection = undefined;
    this.activeConnectionBrokers = undefined;
    this.activeConnectionTarget = undefined;
    this.publish({
      connectionName: this.currentSnapshot.connectionName,
      state: "disconnecting",
    });
    this.shutdownPromise = this.completeShutdown(
      generation,
      activeConnection,
      connectionAttempt,
      consumptionStop,
      topicRequests,
      configurationRequests,
      latencyRequests,
      temporaryTests,
    );
    return this.shutdownPromise;
  }

  snapshot(): KafkaConnectionSnapshot {
    return this.currentSnapshot;
  }

  subscribe(listener: KafkaConnectionSnapshotListener): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }

  testConnection(
    connection: SecureConnectionInput,
    externalSignal?: AbortSignal,
  ): Promise<KafkaConnectionTestResult> {
    return this.requests.testConnection(connection, externalSignal);
  }

  listTopics(externalSignal?: AbortSignal): Promise<readonly string[]> {
    return this.requests.listTopics(externalSignal);
  }

  alterTopicConfiguration(
    topic: string,
    changes: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
    externalSignal?: AbortSignal,
  ): Promise<void> {
    return this.requests.alterTopicConfiguration(topic, changes, validateOnly, externalSignal);
  }

  describeTopicConfiguration(
    topic: string,
    externalSignal?: AbortSignal,
  ): Promise<readonly KafkaTopicConfigurationEntry[]> {
    return this.requests.describeTopicConfiguration(topic, externalSignal);
  }

  describeBrokerConfiguration(
    brokerId: number,
    externalSignal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]> {
    return this.requests.describeBrokerConfiguration(brokerId, externalSignal);
  }

  describeClusterMetadata(externalSignal?: AbortSignal): Promise<KafkaClusterMetadata> {
    return this.requests.describeClusterMetadata(externalSignal);
  }

  createAcl(
    acl: import("../contracts").KafkaAclBinding,
    externalSignal?: AbortSignal,
  ): Promise<void> {
    return this.requests.createAcl(acl, externalSignal);
  }

  deleteAcl(
    acl: import("../contracts").KafkaAclBinding,
    externalSignal?: AbortSignal,
  ): Promise<void> {
    return this.requests.deleteAcl(acl, externalSignal);
  }

  listAcls(
    externalSignal?: AbortSignal,
  ): Promise<readonly import("../contracts").KafkaAclBinding[]> {
    return this.requests.listAcls(externalSignal);
  }

  loadTransformLogs(
    name: string,
    externalSignal?: AbortSignal,
  ): Promise<RedpandaTransformLogResult> {
    return this.requests.loadTransformLogs(name, externalSignal);
  }

  describeConsumerGroup(
    groupId: string,
    externalSignal?: AbortSignal,
  ): Promise<KafkaConsumerGroupDetails> {
    return this.requests.describeConsumerGroup(groupId, externalSignal);
  }

  listConsumerGroups(externalSignal?: AbortSignal): Promise<KafkaConsumerGroupInventory> {
    return this.requests.listConsumerGroups(externalSignal);
  }

  runLatencyProbe(
    request: KafkaLatencyProbeRequest,
    runId: string,
    externalSignal: AbortSignal,
  ): Promise<KafkaLatencyProbeMeasurement> {
    return this.requests.runLatencyProbe(request, runId, externalSignal);
  }

  async startConsumption(
    request: KafkaFetchRequest,
    observer: KafkaConsumptionObserver,
    externalSignal?: AbortSignal,
    checkpoint?: KafkaReadCheckpoint,
  ): Promise<void> {
    this.assertAvailable();
    externalSignal?.throwIfAborted();
    const previousStop = this.stopConsumption();
    const connection = this.activeConnection;
    const connectionGeneration = this.generation;
    if (connection === undefined || this.currentSnapshot.state !== "connected") {
      await previousStop;
      throw new NoActiveKafkaConnectionError("consuming messages");
    }

    const pending: PendingConsumption = {
      connection,
      controller: new AbortController(),
      generation: this.consumptionGeneration,
      operation: undefined,
      openingStarted: false,
    };
    this.pendingConsumption = pending;
    const operation = this.openConsumption(
      pending,
      previousStop,
      connection,
      connectionGeneration,
      request,
      observer,
      externalSignal,
      checkpoint,
    );
    pending.operation = operation;
    try {
      await operation;
    } finally {
      if (this.pendingConsumption === pending) this.pendingConsumption = undefined;
    }
  }

  private async openConsumption(
    pending: PendingConsumption,
    previousStop: Promise<void>,
    connection: KafkaActiveConnection,
    connectionGeneration: number,
    request: KafkaFetchRequest,
    observer: KafkaConsumptionObserver,
    externalSignal?: AbortSignal,
    checkpoint?: KafkaReadCheckpoint,
  ): Promise<void> {
    const { controller, generation } = pending;
    const signal = abortSignals(controller.signal, externalSignal);
    await previousStop;
    const isCurrent = (): boolean =>
      this.pendingConsumption === pending &&
      generation === this.consumptionGeneration &&
      this.isCurrentConnection(connection, connectionGeneration) &&
      !signal.aborted;
    if (!isCurrent()) throw new ConnectionAttemptSupersededError();
    let stream: KafkaMessageStream;
    try {
      pending.openingStarted = true;
      stream = await connection.openMessageStream(request, signal, checkpoint);
    } catch (error) {
      if (!isCurrent()) throw new ConnectionAttemptSupersededError(ownedCleanupFailure(error));
      throw error;
    }
    if (!isCurrent()) {
      try {
        await stream.close();
      } catch (error) {
        throw new ConnectionAttemptSupersededError(error);
      }
      throw new ConnectionAttemptSupersededError();
    }

    const consumption: ActiveConsumption = {
      closePromise: undefined,
      connection,
      connectionGeneration,
      controller,
      emptyTimer: undefined,
      generation,
      observer,
      pump: undefined,
      receivedMessage: false,
      request,
      stream,
    };
    if (request.mode === "tail") {
      consumption.emptyTimer = setTimeout(() => {
        if (this.isCurrentConsumption(consumption) && !consumption.receivedMessage) {
          consumption.observer.onEmpty();
        }
      }, KAFKA_MESSAGE_LIMITS.emptyObservationMs);
      consumption.emptyTimer.unref?.();
    }
    this.pendingConsumption = undefined;
    this.activeConsumption = consumption;
    consumption.pump = this.pumpConsumption(consumption);
    void consumption.pump;
  }

  stopConsumption(): Promise<void> {
    const consumption = this.activeConsumption;
    const pending = this.pendingConsumption;
    const previousStop = this.consumptionStop;
    this.activeConsumption = undefined;
    this.pendingConsumption = undefined;
    this.consumptionGeneration += 1;
    pending?.controller.abort();
    if (consumption === undefined && pending === undefined) {
      return this.waitForConsumptionStop(previousStop);
    }
    consumption?.controller.abort();
    if (consumption !== undefined) this.clearEmptyTimer(consumption);
    const operation = this.completeConsumptionStop(consumption, pending, previousStop);
    // A pending open may create an unregistered stream after connection teardown.
    this.trackConsumptionStop(
      operation,
      consumption?.connection ?? pending?.connection ?? this.consumptionStopConnection,
      (previousStop === undefined || this.consumptionStopCoveredByConnectionClose) &&
        pending?.openingStarted !== true,
    );
    return this.waitForConsumptionStop(operation);
  }

  private trackConsumptionStop(
    operation: Promise<void>,
    connection: KafkaActiveConnection | undefined,
    coveredByConnectionClose: boolean,
  ): void {
    this.consumptionStop = operation;
    this.consumptionStopCoveredByConnectionClose = coveredByConnectionClose;
    this.consumptionStopConnection = connection;
    this.consumptionStopSettled = false;
    const settled = (failed: boolean): void => {
      if (this.consumptionStop !== operation) return;
      this.consumptionStopSettled = true;
      if (
        !failed ||
        (this.consumptionStopCoveredByConnectionClose &&
          this.consumptionStopConnection !== undefined &&
          this.closedConnections.has(this.consumptionStopConnection))
      ) {
        this.consumptionStop = undefined;
        this.consumptionStopConnection = undefined;
      }
    };
    // Only registered-stream cleanup can be confirmed by closing its connection.
    void operation.then(
      () => settled(false),
      () => settled(true),
    );
  }

  private waitForConsumptionStop(operation: Promise<void> | undefined): Promise<void> {
    return operation === undefined
      ? Promise.resolve()
      : this.waitForCleanup(operation, "consumption");
  }

  private waitForCleanup<Value>(
    operation: Promise<Value>,
    kind: "consumption" | "connection" | "shutdown",
  ): Promise<Value> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new KafkaCleanupTimeoutError(kind));
      }, CONSUMPTION_CLEANUP_TIMEOUT_MS);
      timer.unref?.();
      void operation.then(
        (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        (error: unknown) => {
          clearTimeout(timer);
          reject(
            error instanceof Error ? error : new Error("Kafka cleanup failed.", { cause: error }),
          );
        },
      );
    });
  }

  private async completeConsumptionStop(
    consumption: ActiveConsumption | undefined,
    pending: PendingConsumption | undefined,
    previousStop: Promise<void> | undefined,
  ): Promise<void> {
    const results = await Promise.allSettled([
      ...(previousStop === undefined ? [] : [previousStop]),
      ...(pending?.operation === undefined ? [] : [pending.operation]),
      ...(consumption === undefined
        ? []
        : [this.closeConsumptionStream(consumption), consumption.pump]),
    ]);
    const pendingIndex = pending?.operation === undefined ? -1 : previousStop === undefined ? 0 : 1;
    const failures = results.flatMap((result, index) => {
      if (result.status !== "rejected" || (index === pendingIndex && !pending?.openingStarted))
        return [];
      if (result.reason instanceof ConnectionAttemptSupersededError) {
        return result.reason.cleanupFailure === undefined ? [] : [result.reason.cleanupFailure];
      }
      return [result.reason as unknown];
    });
    if (failures.length === 1) {
      throw failures[0] instanceof Error
        ? failures[0]
        : new Error("The Kafka message stream rejected cleanup with a non-error value.", {
            cause: failures[0],
          });
    }
    if (failures.length > 1) {
      throw new AggregateError(failures, "Kafka message streams did not close cleanly.");
    }
    if (consumption !== undefined) this.reportConsumptionCheckpoint(consumption);
  }

  private reportConsumptionCheckpoint(consumption: ActiveConsumption): void {
    const checkpoint = consumption.stream.checkpoint?.();
    if (checkpoint !== undefined) consumption.observer.onCheckpoint?.(checkpoint);
  }

  private assertAvailable(): void {
    if (this.shuttingDown) {
      throw new Error("Kafka application session is shut down.");
    }
  }

  private clearEmptyTimer(consumption: ActiveConsumption): void {
    if (consumption.emptyTimer !== undefined) {
      clearTimeout(consumption.emptyTimer);
      consumption.emptyTimer = undefined;
    }
  }

  private closeConsumptionStream(consumption: ActiveConsumption): Promise<void> {
    if (consumption.closePromise === undefined) {
      try {
        consumption.closePromise = consumption.stream.close();
      } catch (error) {
        consumption.closePromise = Promise.reject(
          error instanceof Error
            ? error
            : new Error("Kafka message stream cleanup failed.", { cause: error }),
        );
      }
    }
    return consumption.closePromise;
  }

  private clearConnectionAttempt(operation: Promise<void>, controller: AbortController): void {
    if (this.connectionAttempt === operation) {
      this.connectionAttempt = undefined;
    }
    if (this.connectionController === controller) {
      this.connectionController = undefined;
    }
  }

  private async closeSuperseded(connection: KafkaActiveConnection): Promise<never> {
    try {
      await connection.close();
    } catch (error) {
      throw new ConnectionAttemptSupersededError(error);
    }
    throw new ConnectionAttemptSupersededError();
  }

  private closeConnectionResources(connection: KafkaActiveConnection): Promise<void> {
    const existing = this.connectionClosures.get(connection);
    if (existing?.pending) return existing.operation;
    const operation = this.finishConnectionClose(connection);
    const closure = { operation, pending: true };
    this.connectionClosures.set(connection, closure);
    void operation.then(
      () => {
        if (this.connectionClosures.get(connection) === closure)
          this.connectionClosures.delete(connection);
      },
      () => {
        closure.pending = false;
      },
    );
    return operation;
  }

  private async finishConnectionClose(connection: KafkaActiveConnection): Promise<void> {
    await connection.close();
    this.closedConnections.add(connection);
    if (
      this.consumptionStopCoveredByConnectionClose &&
      this.consumptionStopConnection === connection &&
      this.consumptionStopSettled
    ) {
      this.consumptionStop = undefined;
      this.consumptionStopConnection = undefined;
    }
  }

  private closeTrackedConnections(connection: KafkaActiveConnection | undefined): Promise<void>[] {
    const connections = new Set(this.connectionClosures.keys());
    if (connection !== undefined) connections.add(connection);
    return [...connections].map((owned) => this.closeConnectionResources(owned));
  }

  private async closeReplacedConnection(
    connection: KafkaActiveConnection | undefined,
    latencyRequests: readonly Promise<KafkaLatencyProbeMeasurement>[],
  ): Promise<void> {
    const closes = this.closeTrackedConnections(connection);
    const results = await Promise.allSettled([...closes, ...latencyRequests]);
    const failures = [
      ...results
        .slice(0, closes.length)
        .flatMap((result) => (result.status === "rejected" ? [result.reason as unknown] : [])),
      ...cleanupFailures(results.slice(closes.length)),
    ];
    if (failures.length === 1) throw failures[0];
    if (failures.length > 1)
      throw new AggregateError(failures, "The replaced Kafka resources did not close cleanly.");
  }

  private async completeDisconnect(
    generation: number,
    connectionName: string | null,
    activeConnection: KafkaActiveConnection | undefined,
    connectionAttempt: Promise<void> | undefined,
    consumptionStop: Promise<void> | undefined,
    topicRequests: readonly Promise<readonly string[]>[],
    configurationRequests: readonly Promise<unknown>[],
    latencyRequests: readonly Promise<KafkaLatencyProbeMeasurement>[],
  ): Promise<void> {
    const closes = this.closeTrackedConnections(activeConnection);
    const operations: Promise<unknown>[] = [...closes];
    if (connectionAttempt !== undefined) {
      operations.push(connectionAttempt);
    }
    const consumptionIndex =
      consumptionStop === undefined ? undefined : operations.push(consumptionStop) - 1;
    operations.push(...topicRequests);
    operations.push(...configurationRequests);
    operations.push(...latencyRequests);
    let deadlineFailure: unknown;
    const results = await this.waitForCleanup(Promise.allSettled(operations), "connection").catch(
      (error: unknown) => {
        deadlineFailure = error;
        return [];
      },
    );
    const failures = [
      ...(deadlineFailure === undefined ? [] : [deadlineFailure]),
      ...results.flatMap((result, index) =>
        index < closes.length && result.status === "rejected" ? [result.reason as unknown] : [],
      ),
      ...(consumptionIndex !== undefined && results[consumptionIndex]?.status === "rejected"
        ? [results[consumptionIndex].reason as unknown]
        : []),
      ...cleanupFailures(results),
    ];
    if (generation === this.generation) {
      this.publish(
        failures.length === 0
          ? { connectionName: null, state: "disconnected" }
          : {
              connectionName,
              failure: failures[0],
              state: "failed",
            },
      );
    }
    if (failures.length > 0) {
      if (failures.length === 1 && failures[0] instanceof KafkaCleanupTimeoutError)
        throw failures[0];
      throw new AggregateError(failures, "The Kafka connection did not close cleanly.");
    }
  }

  private async completeShutdown(
    generation: number,
    activeConnection: KafkaActiveConnection | undefined,
    connectionAttempt: Promise<void> | undefined,
    consumptionStop: Promise<void> | undefined,
    topicRequests: readonly Promise<readonly string[]>[],
    configurationRequests: readonly Promise<unknown>[],
    latencyRequests: readonly Promise<KafkaLatencyProbeMeasurement>[],
    temporaryTests: readonly Promise<KafkaConnectionTestResult>[],
  ): Promise<void> {
    const closes = this.closeTrackedConnections(activeConnection);
    let deadlineFailure: unknown;
    const results = await this.waitForCleanup(
      Promise.allSettled([
        ...closes,
        ...(connectionAttempt === undefined ? [] : [connectionAttempt]),
        ...(consumptionStop === undefined ? [] : [consumptionStop]),
        ...topicRequests,
        ...configurationRequests,
        ...latencyRequests,
        ...temporaryTests,
      ]),
      "shutdown",
    ).catch((error: unknown) => {
      deadlineFailure = error;
      return [];
    });
    const closeFailure = results
      .slice(0, closes.length)
      .flatMap((result) => (result.status === "rejected" ? [result.reason as unknown] : []));
    const consumptionIndex =
      consumptionStop === undefined
        ? undefined
        : closes.length + (connectionAttempt === undefined ? 0 : 1);
    const consumptionFailure =
      consumptionIndex !== undefined && results[consumptionIndex]?.status === "rejected"
        ? [results[consumptionIndex].reason as unknown]
        : [];
    const failures = [
      ...(deadlineFailure === undefined ? [] : [deadlineFailure]),
      ...closeFailure,
      ...consumptionFailure,
      ...cleanupFailures(results),
    ];
    if (generation === this.generation) {
      this.publish(
        failures.length === 0
          ? { connectionName: null, state: "disconnected" }
          : {
              connectionName: this.currentSnapshot.connectionName,
              state: "failed",
              failure: failures[0],
            },
      );
    }
    if (failures.length > 0) {
      if (failures.length === 1 && failures[0] instanceof KafkaCleanupTimeoutError)
        throw failures[0];
      throw new AggregateError(failures, "Kafka resources did not close cleanly during shutdown.");
    }
  }

  private async pumpConsumption(consumption: ActiveConsumption): Promise<void> {
    const reportCoverage = (): void => {
      const coverage = consumption.stream.coverage?.();
      if (coverage !== undefined) consumption.observer.onCoverage?.(coverage);
    };
    let failure: unknown;
    let cleanupFailure: unknown;
    let completed = false;
    const observerFailed = (error: unknown): void => {
      failure ??=
        error instanceof Error
          ? error
          : new Error("Kafka consumption observer failed.", { cause: error });
    };
    let receivingCoverage = true;
    let unsubscribeCoverage: (() => void) | undefined;
    try {
      if (consumption.request.mode !== "tail") {
        unsubscribeCoverage = consumption.stream.subscribeCoverage?.((coverage) => {
          if (
            !receivingCoverage ||
            !this.isCurrentConsumption(consumption) ||
            failure !== undefined
          )
            return;
          try {
            consumption.observer.onCoverage?.(coverage);
          } catch (error) {
            observerFailed(error);
            receivingCoverage = false;
            const cleanup = this.closeConsumptionStream(consumption);
            // The pump awaits this same cleanup. Retain a rejection even if the
            // iterator is still unwinding, so a later lifecycle action owns it.
            void cleanup.catch(() => {
              if (this.isCurrentConsumption(consumption))
                this.trackConsumptionStop(cleanup, consumption.connection, true);
            });
          }
        });
      }
      for await (const message of consumption.stream) {
        if (!this.isCurrentConsumption(consumption) || failure !== undefined) {
          break;
        }
        consumption.receivedMessage = true;
        this.clearEmptyTimer(consumption);
        consumption.observer.onMessage(message);
        consumption.stream.acknowledge?.(message);
      }
      if (this.isCurrentConsumption(consumption) && !consumption.controller.signal.aborted) {
        if (consumption.request.mode === "tail") {
          failure = new Error("The Kafka message stream ended unexpectedly.");
        } else {
          completed = true;
        }
      }
    } catch (error) {
      if (this.isCurrentConsumption(consumption) && !consumption.controller.signal.aborted) {
        failure ??= error;
      }
    } finally {
      receivingCoverage = false;
      try {
        unsubscribeCoverage?.();
      } catch (error) {
        observerFailed(error);
      }
      this.clearEmptyTimer(consumption);
      const cleanup = this.closeConsumptionStream(consumption);
      try {
        await cleanup;
      } catch (error) {
        if (this.isCurrentConsumption(consumption)) {
          // Automatic completion/failure owns the same cleanup obligation as explicit Stop.
          cleanupFailure = error;
          this.trackConsumptionStop(cleanup, consumption.connection, true);
          if (failure === undefined) failure = error;
        }
      }
      if (this.isCurrentConsumption(consumption)) {
        if (failure === undefined) {
          try {
            reportCoverage();
            this.reportConsumptionCheckpoint(consumption);
          } catch (error) {
            observerFailed(error);
          }
        }
        this.activeConsumption = undefined;
        if (failure === undefined && completed) {
          try {
            consumption.observer.onComplete();
          } catch (error) {
            observerFailed(error);
          }
        }
        if (failure !== undefined) {
          try {
            consumption.observer.onFailure(failure);
          } catch (notificationError) {
            const deliveryFailure = Promise.reject(
              new AggregateError(
                [
                  failure,
                  ...(cleanupFailure === undefined ? [] : [cleanupFailure]),
                  notificationError,
                ],
                "Kafka consumption failed and its failure observer also failed.",
              ),
            );
            this.trackConsumptionStop(deliveryFailure, consumption.connection, true);
          }
        }
      }
    }
  }

  private async establishConnection(
    connection: SecureConnectionInput,
    generation: number,
    controller: AbortController,
    signal: AbortSignal,
    previousConnection: KafkaActiveConnection | undefined,
    consumptionStop: Promise<void> | undefined,
    latencyRequests: readonly Promise<KafkaLatencyProbeMeasurement>[],
  ): Promise<void> {
    try {
      const cleanupOperations = [
        ...(consumptionStop === undefined ? [] : [consumptionStop]),
        ...(previousConnection === undefined &&
        latencyRequests.length === 0 &&
        this.connectionClosures.size === 0
          ? []
          : [this.closeReplacedConnection(previousConnection, latencyRequests)]),
      ];
      const cleanup =
        cleanupOperations.length === 0
          ? []
          : await this.waitForCleanup(Promise.allSettled(cleanupOperations), "connection");
      const failures = cleanup.flatMap((result) =>
        result.status === "rejected" ? [result.reason as unknown] : [],
      );
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1)
        throw new AggregateError(failures, "The replaced Kafka resources did not close cleanly.");
      if (!this.isCurrent(generation, controller)) {
        throw new ConnectionAttemptSupersededError();
      }
      signal.throwIfAborted();
      const openedConnection = await this.connectionPort.openConnection(connection, signal);
      if (!this.isCurrent(generation, controller)) {
        return await this.closeSuperseded(openedConnection);
      }
      await closeCancelledConnection(openedConnection, signal);
      this.activeConnection = openedConnection;
      this.activeConnectionBrokers = [...connection.brokers];
      this.activeConnectionTarget = connection.brokers.join(", ");
      this.publish({
        connectionName: connection.name,
        state: "connected",
      });
    } catch (error) {
      if (!this.isCurrent(generation, controller)) {
        if (error instanceof ConnectionAttemptSupersededError) {
          throw error;
        }
        throw new ConnectionAttemptSupersededError();
      }
      this.publish({
        connectionName: connection.name,
        failure: error,
        state: "failed",
      });
      throw error;
    }
  }

  private isCurrent(generation: number, controller: AbortController): boolean {
    return (
      !this.shuttingDown &&
      generation === this.generation &&
      this.connectionController === controller &&
      !controller.signal.aborted
    );
  }

  private isCurrentConnection(connection: KafkaActiveConnection, generation: number): boolean {
    return (
      !this.shuttingDown &&
      generation === this.generation &&
      this.activeConnection === connection &&
      this.currentSnapshot.state === "connected"
    );
  }

  private isCurrentConsumption(consumption: ActiveConsumption): boolean {
    return (
      this.activeConsumption === consumption &&
      consumption.generation === this.consumptionGeneration &&
      !consumption.controller.signal.aborted &&
      this.isCurrentConnection(consumption.connection, consumption.connectionGeneration)
    );
  }

  private nextGeneration(): number {
    this.generation += 1;
    return this.generation;
  }

  private publish(snapshot: KafkaConnectionSnapshot): void {
    this.currentSnapshot = snapshot;
    for (const listener of this.listeners) {
      listener(snapshot);
    }
  }
}
