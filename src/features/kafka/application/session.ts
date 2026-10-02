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
import { abortSignals, cleanupFailures, closeCancelledConnection } from "./session-lifecycle";
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
  private activeConnectionBrokers: readonly string[] | undefined;
  private activeConnectionTarget: string | undefined;
  private activeConnection: KafkaActiveConnection | undefined;
  private activeConsumption: ActiveConsumption | undefined;
  private connectionAttempt: Promise<void> | undefined;
  private connectionController: AbortController | undefined;
  private consumptionGeneration = 0;
  private generation = 0;
  private readonly listeners = new Set<KafkaConnectionSnapshotListener>();
  private shutdownPromise: Promise<void> | undefined;
  private shuttingDown = false;
  private currentSnapshot: KafkaConnectionSnapshot = {
    connectionName: null,
    state: "disconnected",
  };

  private readonly requests: KafkaSessionRequests;

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
      this.activeConsumption === undefined ? undefined : this.stopConsumption();
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
      this.activeConsumption === undefined ? undefined : this.stopConsumption();
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

  private async pumpConsumption(consumption: ActiveConsumption): Promise<void> {
    const reportCoverage = (): void => {
      const coverage = consumption.stream.coverage?.();
      if (coverage !== undefined) consumption.observer.onCoverage?.(coverage);
    };
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
      reportCoverage();
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
