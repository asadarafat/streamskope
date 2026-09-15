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

import { ConnectionAttemptSupersededError, NoActiveKafkaConnectionError } from "./session-errors";
import {
  abortSignals,
  cleanupFailures,
  closeCancelledConnection,
  ownedCleanupFailure,
} from "./session-lifecycle";
import { readRedpandaTransformLogs, type RedpandaTransformLogResult } from "./transform-log-reader";
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
import type { KafkaLatencyProbeMeasurement } from "./latency-types";

export { ConnectionAttemptSupersededError, NoActiveKafkaConnectionError } from "./session-errors";

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
  private readonly aclRequests = new Map<Promise<unknown>, AbortController>();
  private activeConnectionBrokers: readonly string[] | undefined;
  private activeConnectionTarget: string | undefined;
  private activeConnection: KafkaActiveConnection | undefined;
  private activeConsumption: ActiveConsumption | undefined;
  private connectionAttempt: Promise<void> | undefined;
  private connectionController: AbortController | undefined;
  private readonly configurationRequests = new Map<Promise<unknown>, AbortController>();
  private readonly consumerGroupDetailRequests = new Map<
    Promise<KafkaConsumerGroupDetails>,
    AbortController
  >();
  private readonly consumerGroupInventoryRequests = new Map<
    Promise<KafkaConsumerGroupInventory>,
    AbortController
  >();
  private consumptionGeneration = 0;
  private generation = 0;
  private readonly latencyRequests = new Map<
    Promise<KafkaLatencyProbeMeasurement>,
    AbortController
  >();
  private readonly listeners = new Set<KafkaConnectionSnapshotListener>();
  private shutdownPromise: Promise<void> | undefined;
  private shuttingDown = false;
  private readonly topicRequests = new Map<Promise<readonly string[]>, AbortController>();
  private currentSnapshot: KafkaConnectionSnapshot = {
    connectionName: null,
    state: "disconnected",
  };
  private readonly temporaryTests = new Map<Promise<KafkaConnectionTestResult>, AbortController>();
  private readonly transformLogRequests = new Map<Promise<unknown>, AbortController>();

  constructor(private readonly connectionPort: KafkaConnectionPort) {}

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
    service: "redpandaAdmin" | "schemaRegistry",
  ): KafkaClusterServiceContext | null {
    return this.currentSnapshot.state === "connected"
      ? (this.activeConnection?.clusterServiceContext?.(service) ?? null)
      : null;
  }

  async connect(connection: SecureConnectionInput, externalSignal?: AbortSignal): Promise<void> {
    this.assertAvailable();
    externalSignal?.throwIfAborted();
    const consumptionStop =
      this.activeConsumption === undefined ? undefined : this.stopConsumption();
    const generation = this.nextGeneration();
    void Promise.allSettled(this.cancelTopicRequests());
    void Promise.allSettled(this.cancelConfigurationRequests());
    void Promise.allSettled(this.cancelFeatureRequests(this.aclRequests));
    void Promise.allSettled(this.cancelFeatureRequests(this.transformLogRequests));
    void Promise.allSettled(this.cancelConsumerGroupRequests());
    const latencyRequests = this.cancelLatencyRequests();
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
      this.activeConsumption === undefined ? undefined : this.stopConsumption();
    const generation = this.nextGeneration();
    const topicRequests = this.cancelTopicRequests();
    const configurationRequests = [
      ...this.cancelConfigurationRequests(),
      ...this.cancelConsumerGroupRequests(),
      ...this.cancelFeatureRequests(this.aclRequests),
      ...this.cancelFeatureRequests(this.transformLogRequests),
    ];
    const latencyRequests = this.cancelLatencyRequests();
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
      this.activeConsumption === undefined ? undefined : this.stopConsumption();
    this.shuttingDown = true;
    const generation = this.nextGeneration();
    const topicRequests = this.cancelTopicRequests();
    const configurationRequests = [
      ...this.cancelConfigurationRequests(),
      ...this.cancelConsumerGroupRequests(),
      ...this.cancelFeatureRequests(this.aclRequests),
      ...this.cancelFeatureRequests(this.transformLogRequests),
    ];
    const latencyRequests = this.cancelLatencyRequests();
    this.connectionController?.abort();
    this.connectionController = undefined;
    for (const controller of this.temporaryTests.values()) {
      controller.abort();
    }
    const connectionAttempt = this.connectionAttempt;
    const temporaryTests = [...this.temporaryTests.keys()];
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
    this.assertAvailable();
    const controller = new AbortController();
    let operation: Promise<KafkaConnectionTestResult>;
    try {
      operation = this.connectionPort.testConnection(
        connection,
        abortSignals(controller.signal, externalSignal),
      );
    } catch (error) {
      return Promise.reject(
        error instanceof Error
          ? error
          : new Error("The connection port rejected with a non-error value.", {
              cause: error,
            }),
      );
    }
    this.temporaryTests.set(operation, controller);
    operation.then(
      () => {
        this.temporaryTests.delete(operation);
      },
      () => {
        this.temporaryTests.delete(operation);
      },
    );
    return operation;
  }

  listTopics(externalSignal?: AbortSignal): Promise<readonly string[]> {
    this.assertAvailable();
    const connection = this.activeConnection;
    const generation = this.generation;
    if (connection === undefined || this.currentSnapshot.state !== "connected") {
      return Promise.reject(new NoActiveKafkaConnectionError());
    }

    void Promise.allSettled(this.cancelTopicRequests());
    const controller = new AbortController();
    const operation = this.listTopicsForGeneration(
      connection,
      generation,
      controller,
      abortSignals(controller.signal, externalSignal),
    );
    this.topicRequests.set(operation, controller);
    operation.then(
      () => {
        this.topicRequests.delete(operation);
      },
      () => {
        this.topicRequests.delete(operation);
      },
    );
    return operation;
  }

  alterTopicConfiguration(
    topic: string,
    changes: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
    externalSignal?: AbortSignal,
  ): Promise<void> {
    return this.startAdminRequest(
      this.configurationRequests,
      "changing topic configuration",
      (connection, signal) =>
        connection.alterTopicConfiguration(topic, changes, validateOnly, signal),
      externalSignal,
    );
  }

  describeTopicConfiguration(
    topic: string,
    externalSignal?: AbortSignal,
  ): Promise<readonly KafkaTopicConfigurationEntry[]> {
    return this.startAdminRequest(
      this.configurationRequests,
      "reading topic configuration",
      (connection, signal) => connection.describeTopicConfiguration(topic, signal),
      externalSignal,
    );
  }

  describeBrokerConfiguration(
    brokerId: number,
    externalSignal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]> {
    return this.startAdminRequest(
      this.configurationRequests,
      "reading broker configuration",
      (connection, signal) => connection.describeBrokerConfiguration(brokerId, signal),
      externalSignal,
    );
  }

  describeClusterMetadata(externalSignal?: AbortSignal): Promise<KafkaClusterMetadata> {
    return this.startAdminRequest(
      this.configurationRequests,
      "reading cluster metadata",
      (connection, signal) => connection.describeClusterMetadata(signal),
      externalSignal,
    );
  }

  createAcl(
    acl: import("../contracts").KafkaAclBinding,
    externalSignal?: AbortSignal,
  ): Promise<void> {
    return this.startAdminRequest(
      this.aclRequests,
      "creating an ACL",
      (connection, signal) => {
        if (connection.createAcl === undefined) {
          return Promise.reject(
            new Error("The active Kafka adapter does not support ACL creation."),
          );
        }
        return connection.createAcl(acl, signal);
      },
      externalSignal,
    );
  }

  deleteAcl(
    acl: import("../contracts").KafkaAclBinding,
    externalSignal?: AbortSignal,
  ): Promise<void> {
    return this.startAdminRequest(
      this.aclRequests,
      "deleting an ACL",
      (connection, signal) => {
        if (connection.deleteAcl === undefined) {
          return Promise.reject(
            new Error("The active Kafka adapter does not support ACL deletion."),
          );
        }
        return connection.deleteAcl(acl, signal);
      },
      externalSignal,
    );
  }

  listAcls(
    externalSignal?: AbortSignal,
  ): Promise<readonly import("../contracts").KafkaAclBinding[]> {
    return this.startAdminRequest(
      this.aclRequests,
      "listing ACLs",
      (connection, signal) => {
        if (connection.listAcls === undefined) {
          return Promise.reject(
            new Error("The active Kafka adapter does not support ACL inventory."),
          );
        }
        return connection.listAcls(signal);
      },
      externalSignal,
    );
  }

  loadTransformLogs(
    name: string,
    externalSignal?: AbortSignal,
  ): Promise<RedpandaTransformLogResult> {
    return this.startAdminRequest(
      this.transformLogRequests,
      "reading transform logs",
      (connection, signal) => readRedpandaTransformLogs(connection, name, signal),
      externalSignal,
    );
  }

  describeConsumerGroup(
    groupId: string,
    externalSignal?: AbortSignal,
  ): Promise<KafkaConsumerGroupDetails> {
    return this.startConsumerGroupRequest(
      this.consumerGroupDetailRequests,
      "reading a consumer group",
      (connection, signal) => {
        if (connection.describeConsumerGroup === undefined) {
          return Promise.reject(
            new Error("The active Kafka adapter does not support consumer-group detail."),
          );
        }
        return connection.describeConsumerGroup(groupId, signal);
      },
      externalSignal,
    );
  }

  listConsumerGroups(externalSignal?: AbortSignal): Promise<KafkaConsumerGroupInventory> {
    return this.startConsumerGroupRequest(
      this.consumerGroupInventoryRequests,
      "listing consumer groups",
      (connection, signal) => {
        if (connection.listConsumerGroups === undefined) {
          return Promise.reject(
            new Error("The active Kafka adapter does not support consumer-group inventory."),
          );
        }
        return connection.listConsumerGroups(signal);
      },
      externalSignal,
    );
  }

  runLatencyProbe(
    request: KafkaLatencyProbeRequest,
    runId: string,
    externalSignal: AbortSignal,
  ): Promise<KafkaLatencyProbeMeasurement> {
    this.assertAvailable();
    const connection = this.activeConnection;
    const generation = this.generation;
    if (connection === undefined || this.currentSnapshot.state !== "connected") {
      return Promise.reject(new NoActiveKafkaConnectionError("running a latency probe"));
    }
    if (connection.runLatencyProbe === undefined) {
      return Promise.reject(
        new Error("The active Kafka adapter does not support latency probing."),
      );
    }

    void Promise.allSettled(this.cancelLatencyRequests());
    const controller = new AbortController();
    const signal = abortSignals(controller.signal, externalSignal);
    let started: Promise<KafkaLatencyProbeMeasurement>;
    try {
      started = connection.runLatencyProbe(request, runId, signal);
    } catch (error) {
      started = Promise.reject(
        error instanceof Error
          ? error
          : new Error("The Kafka latency port threw a non-error value.", {
              cause: error,
            }),
      );
    }
    const operation = this.configurationForGeneration(connection, generation, controller, started);
    this.latencyRequests.set(operation, controller);
    operation.then(
      () => {
        this.latencyRequests.delete(operation);
      },
      () => {
        this.latencyRequests.delete(operation);
      },
    );
    return operation;
  }

  async startConsumption(
    request: KafkaFetchRequest,
    observer: KafkaConsumptionObserver,
    externalSignal?: AbortSignal,
  ): Promise<void> {
    this.assertAvailable();
    await this.stopConsumption();
    this.assertAvailable();
    const connection = this.activeConnection;
    const connectionGeneration = this.generation;
    if (connection === undefined || this.currentSnapshot.state !== "connected") {
      throw new NoActiveKafkaConnectionError("consuming messages");
    }

    this.consumptionGeneration += 1;
    const generation = this.consumptionGeneration;
    const controller = new AbortController();
    const signal = abortSignals(controller.signal, externalSignal);
    const stream = await connection.openMessageStream(request, signal);
    if (
      !this.isCurrentConnection(connection, connectionGeneration) ||
      generation !== this.consumptionGeneration ||
      signal.aborted
    ) {
      await stream.close();
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
    this.activeConsumption = consumption;
    consumption.pump = this.pumpConsumption(consumption);
    void consumption.pump;
  }

  async stopConsumption(): Promise<void> {
    const consumption = this.activeConsumption;
    if (consumption === undefined) {
      return;
    }
    this.activeConsumption = undefined;
    this.consumptionGeneration += 1;
    consumption.controller.abort();
    this.clearEmptyTimer(consumption);
    const close = this.closeConsumptionStream(consumption);
    const results = await Promise.allSettled([
      close,
      ...(consumption.pump === undefined ? [] : [consumption.pump]),
    ]);
    const closeResult = results[0];
    if (closeResult?.status === "rejected") {
      throw closeResult.reason instanceof Error
        ? closeResult.reason
        : new Error("The Kafka message stream rejected cleanup with a non-error value.", {
            cause: closeResult?.reason,
          });
    }
  }

  private assertAvailable(): void {
    if (this.shuttingDown) {
      throw new Error("Kafka application session is shut down.");
    }
  }

  private cancelConfigurationRequests(): readonly Promise<unknown>[] {
    const operations = [...this.configurationRequests.keys()];
    for (const controller of this.configurationRequests.values()) {
      controller.abort();
    }
    return operations;
  }

  private cancelConsumerGroupRequests(): readonly Promise<unknown>[] {
    const requests = [
      ...this.consumerGroupInventoryRequests.keys(),
      ...this.consumerGroupDetailRequests.keys(),
    ];
    for (const controller of this.consumerGroupInventoryRequests.values()) {
      controller.abort();
    }
    for (const controller of this.consumerGroupDetailRequests.values()) {
      controller.abort();
    }
    return requests;
  }

  private cancelFeatureRequests(
    requests: ReadonlyMap<Promise<unknown>, AbortController>,
  ): readonly Promise<unknown>[] {
    const operations = [...requests.keys()];
    for (const controller of requests.values()) controller.abort();
    return operations;
  }

  private cancelLatencyRequests(): readonly Promise<KafkaLatencyProbeMeasurement>[] {
    const operations = [...this.latencyRequests.keys()];
    for (const controller of this.latencyRequests.values()) {
      controller.abort();
    }
    return operations;
  }

  private cancelTopicRequests(): readonly Promise<readonly string[]>[] {
    const operations = [...this.topicRequests.keys()];
    for (const controller of this.topicRequests.values()) {
      controller.abort();
    }
    return operations;
  }

  private clearEmptyTimer(consumption: ActiveConsumption): void {
    if (consumption.emptyTimer !== undefined) {
      clearTimeout(consumption.emptyTimer);
      consumption.emptyTimer = undefined;
    }
  }

  private closeConsumptionStream(consumption: ActiveConsumption): Promise<void> {
    consumption.closePromise ??= consumption.stream.close();
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

  private async closeReplacedConnection(
    connection: KafkaActiveConnection | undefined,
    latencyRequests: readonly Promise<KafkaLatencyProbeMeasurement>[],
  ): Promise<void> {
    const closeOperation = connection?.close();
    const results = await Promise.allSettled([
      ...(closeOperation === undefined ? [] : [closeOperation]),
      ...latencyRequests,
    ]);
    if (closeOperation !== undefined && results[0]?.status === "rejected") {
      throw results[0].reason;
    }
    const latencyResults = results.slice(closeOperation === undefined ? 0 : 1);
    const failures = cleanupFailures(latencyResults);
    if (failures.length > 0) {
      throw new AggregateError(failures, "The replaced Kafka latency probe did not close cleanly.");
    }
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
    const operations: Promise<unknown>[] = [];
    if (activeConnection !== undefined) {
      operations.push(activeConnection.close());
    }
    if (connectionAttempt !== undefined) {
      operations.push(connectionAttempt);
    }
    const consumptionIndex =
      consumptionStop === undefined ? undefined : operations.push(consumptionStop) - 1;
    operations.push(...topicRequests);
    operations.push(...configurationRequests);
    operations.push(...latencyRequests);
    const results = await Promise.allSettled(operations);
    const failures = [
      ...results.flatMap((result, index) =>
        index === 0 && activeConnection !== undefined && result.status === "rejected"
          ? [result.reason as unknown]
          : [],
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
    const closeOperation = activeConnection === undefined ? undefined : activeConnection.close();
    const results = await Promise.allSettled([
      ...(closeOperation === undefined ? [] : [closeOperation]),
      ...(connectionAttempt === undefined ? [] : [connectionAttempt]),
      ...(consumptionStop === undefined ? [] : [consumptionStop]),
      ...topicRequests,
      ...configurationRequests,
      ...latencyRequests,
      ...temporaryTests,
    ]);
    const closeFailure =
      closeOperation !== undefined && results[0]?.status === "rejected"
        ? [results[0].reason as unknown]
        : [];
    const consumptionIndex =
      consumptionStop === undefined
        ? undefined
        : (closeOperation === undefined ? 0 : 1) + (connectionAttempt === undefined ? 0 : 1);
    const consumptionFailure =
      consumptionIndex !== undefined && results[consumptionIndex]?.status === "rejected"
        ? [results[consumptionIndex].reason as unknown]
        : [];
    const failures = [...closeFailure, ...consumptionFailure, ...cleanupFailures(results)];
    if (generation === this.generation) {
      this.publish({
        connectionName: null,
        state: "disconnected",
      });
    }
    if (failures.length > 0) {
      throw new AggregateError(failures, "Kafka resources did not close cleanly during shutdown.");
    }
  }

  private async listTopicsForGeneration(
    connection: KafkaActiveConnection,
    generation: number,
    controller: AbortController,
    signal: AbortSignal,
  ): Promise<readonly string[]> {
    try {
      const topics = await connection.listTopics(signal);
      if (!this.isCurrentTopicRequest(connection, generation, controller)) {
        throw new ConnectionAttemptSupersededError();
      }
      return topics;
    } catch (error) {
      if (!this.isCurrentTopicRequest(connection, generation, controller)) {
        throw error instanceof ConnectionAttemptSupersededError
          ? error
          : new ConnectionAttemptSupersededError(ownedCleanupFailure(error));
      }
      throw error;
    }
  }

  private async configurationForGeneration<T>(
    connection: KafkaActiveConnection,
    generation: number,
    controller: AbortController,
    operation: Promise<T>,
  ): Promise<T> {
    try {
      const result = await operation;
      if (!this.isCurrentAdminRequest(connection, generation, controller)) {
        throw new ConnectionAttemptSupersededError();
      }
      return result;
    } catch (error) {
      if (!this.isCurrentAdminRequest(connection, generation, controller)) {
        throw error instanceof ConnectionAttemptSupersededError
          ? error
          : new ConnectionAttemptSupersededError(ownedCleanupFailure(error));
      }
      throw error;
    }
  }

  private async pumpConsumption(consumption: ActiveConsumption): Promise<void> {
    let failure: unknown;
    let completed = false;
    try {
      for await (const message of consumption.stream) {
        if (!this.isCurrentConsumption(consumption)) {
          break;
        }
        consumption.receivedMessage = true;
        this.clearEmptyTimer(consumption);
        consumption.observer.onMessage(message);
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
        failure = error;
      }
    } finally {
      this.clearEmptyTimer(consumption);
      try {
        await this.closeConsumptionStream(consumption);
      } catch (error) {
        if (failure === undefined && this.isCurrentConsumption(consumption)) {
          failure = error;
        }
      }
      if (this.isCurrentConsumption(consumption)) {
        this.activeConsumption = undefined;
        if (failure !== undefined) {
          consumption.observer.onFailure(failure);
        } else if (completed) {
          consumption.observer.onComplete();
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
      if (consumptionStop !== undefined) {
        await consumptionStop;
      }
      if (previousConnection !== undefined || latencyRequests.length > 0) {
        await this.closeReplacedConnection(previousConnection, latencyRequests);
      }
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

  private isCurrentAdminRequest(
    connection: KafkaActiveConnection,
    generation: number,
    controller: AbortController,
  ): boolean {
    return (
      !this.shuttingDown &&
      generation === this.generation &&
      this.activeConnection === connection &&
      this.currentSnapshot.state === "connected" &&
      !controller.signal.aborted
    );
  }

  private isCurrentTopicRequest(
    connection: KafkaActiveConnection,
    generation: number,
    controller: AbortController,
  ): boolean {
    return this.isCurrentAdminRequest(connection, generation, controller);
  }

  private nextGeneration(): number {
    this.generation += 1;
    return this.generation;
  }

  private startAdminRequest<T>(
    requests: Map<Promise<unknown>, AbortController>,
    operationDescription: string,
    start: (connection: KafkaActiveConnection, signal: AbortSignal) => Promise<T>,
    externalSignal?: AbortSignal,
  ): Promise<T> {
    this.assertAvailable();
    const connection = this.activeConnection;
    const generation = this.generation;
    if (connection === undefined || this.currentSnapshot.state !== "connected") {
      return Promise.reject(new NoActiveKafkaConnectionError(operationDescription));
    }

    void Promise.allSettled(this.cancelFeatureRequests(requests));
    const controller = new AbortController();
    const signal = abortSignals(controller.signal, externalSignal);
    let started: Promise<T>;
    try {
      started = start(connection, signal);
    } catch (error) {
      started = Promise.reject(
        error instanceof Error
          ? error
          : new Error("The Kafka feature port threw a non-error value.", { cause: error }),
      );
    }
    const operation = this.configurationForGeneration(connection, generation, controller, started);
    requests.set(operation, controller);
    operation.then(
      () => requests.delete(operation),
      () => requests.delete(operation),
    );
    return operation;
  }

  private startConsumerGroupRequest<T>(
    requests: Map<Promise<T>, AbortController>,
    operationDescription: string,
    start: (connection: KafkaActiveConnection, signal: AbortSignal) => Promise<T>,
    externalSignal?: AbortSignal,
  ): Promise<T> {
    this.assertAvailable();
    const connection = this.activeConnection;
    const generation = this.generation;
    if (connection === undefined || this.currentSnapshot.state !== "connected") {
      return Promise.reject(new NoActiveKafkaConnectionError(operationDescription));
    }

    const obsolete = [...requests.keys()];
    for (const controller of requests.values()) {
      controller.abort();
    }
    void Promise.allSettled(obsolete);
    const controller = new AbortController();
    const signal = abortSignals(controller.signal, externalSignal);
    let started: Promise<T>;
    try {
      started = start(connection, signal);
    } catch (error) {
      started = Promise.reject(
        error instanceof Error
          ? error
          : new Error("The Kafka consumer-group port threw a non-error value.", {
              cause: error,
            }),
      );
    }
    const operation = this.configurationForGeneration(connection, generation, controller, started);
    requests.set(operation, controller);
    operation.then(
      () => {
        requests.delete(operation);
      },
      () => {
        requests.delete(operation);
      },
    );
    return operation;
  }

  private publish(snapshot: KafkaConnectionSnapshot): void {
    this.currentSnapshot = snapshot;
    for (const listener of this.listeners) {
      listener(snapshot);
    }
  }
}
