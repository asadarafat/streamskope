import type {
  KafkaConfigurationEntry,
  KafkaConsumerGroupDetails,
  KafkaLatencyProbeRequest,
  KafkaTopicConfigurationChange,
  KafkaTopicConfigurationEntry,
  SecureConnectionInput,
} from "../contracts";

import { ConnectionAttemptSupersededError, NoActiveKafkaConnectionError } from "./session-errors";
import { abortSignals, ownedCleanupFailure } from "./session-lifecycle";
import { readRedpandaTransformLogs, type RedpandaTransformLogResult } from "./transform-log-reader";
import type {
  KafkaActiveConnection,
  KafkaConnectionPort,
  KafkaConnectionTestResult,
  KafkaClusterMetadata,
  KafkaConsumerGroupInventory,
} from "./types";
import type { KafkaLatencyProbeMeasurement } from "./latency-types";

interface SessionRequestContext {
  readonly connection: KafkaActiveConnection | undefined;
  readonly connected: boolean;
  readonly generation: number;
  readonly shuttingDown: boolean;
}

// Owns in-flight requests; the session supplies connection identity and lifecycle state.
export class KafkaSessionRequests {
  private readonly aclRequests = new Map<Promise<unknown>, AbortController>();
  private readonly configurationRequests = new Map<Promise<unknown>, AbortController>();
  private readonly consumerGroupDetailRequests = new Map<
    Promise<KafkaConsumerGroupDetails>,
    AbortController
  >();
  private readonly consumerGroupInventoryRequests = new Map<
    Promise<KafkaConsumerGroupInventory>,
    AbortController
  >();
  private readonly latencyRequests = new Map<
    Promise<KafkaLatencyProbeMeasurement>,
    AbortController
  >();
  private readonly topicRequests = new Map<Promise<readonly string[]>, AbortController>();
  private readonly temporaryTests = new Map<Promise<KafkaConnectionTestResult>, AbortController>();
  private readonly transformLogRequests = new Map<Promise<unknown>, AbortController>();

  constructor(
    private readonly connectionPort: KafkaConnectionPort,
    private readonly context: () => SessionRequestContext,
  ) {}

  cancelForReconnect(): readonly Promise<KafkaLatencyProbeMeasurement>[] {
    void Promise.allSettled(this.cancelTopicRequests());
    void Promise.allSettled(this.cancelConfigurationRequests());
    void Promise.allSettled(this.cancelFeatureRequests(this.aclRequests));
    void Promise.allSettled(this.cancelFeatureRequests(this.transformLogRequests));
    void Promise.allSettled(this.cancelConsumerGroupRequests());
    // Latency probes own cleanup that must finish before replacing the connection.
    return this.cancelLatencyRequests();
  }

  cancelAdministrativeRequests(): readonly Promise<unknown>[] {
    return [
      ...this.cancelConfigurationRequests(),
      ...this.cancelConsumerGroupRequests(),
      ...this.cancelFeatureRequests(this.aclRequests),
      ...this.cancelFeatureRequests(this.transformLogRequests),
    ];
  }

  cancelTests(): readonly Promise<KafkaConnectionTestResult>[] {
    const operations = [...this.temporaryTests.keys()];
    for (const controller of this.temporaryTests.values()) controller.abort();
    return operations;
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
    const { connection, generation, connected } = this.context();
    if (connection === undefined || !connected) {
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
    const { connection, generation, connected } = this.context();
    if (connection === undefined || !connected) {
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

  private assertAvailable(): void {
    if (this.context().shuttingDown) {
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

  cancelLatencyRequests(): readonly Promise<KafkaLatencyProbeMeasurement>[] {
    const operations = [...this.latencyRequests.keys()];
    for (const controller of this.latencyRequests.values()) {
      controller.abort();
    }
    return operations;
  }

  cancelTopicRequests(): readonly Promise<readonly string[]>[] {
    const operations = [...this.topicRequests.keys()];
    for (const controller of this.topicRequests.values()) {
      controller.abort();
    }
    return operations;
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

  private isCurrentAdminRequest(
    connection: KafkaActiveConnection,
    generation: number,
    controller: AbortController,
  ): boolean {
    const current = this.context();
    return (
      !current.shuttingDown &&
      generation === current.generation &&
      current.connection === connection &&
      current.connected &&
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

  private startAdminRequest<T>(
    requests: Map<Promise<unknown>, AbortController>,
    operationDescription: string,
    start: (connection: KafkaActiveConnection, signal: AbortSignal) => Promise<T>,
    externalSignal?: AbortSignal,
  ): Promise<T> {
    this.assertAvailable();
    const { connection, generation, connected } = this.context();
    if (connection === undefined || !connected) {
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
    const { connection, generation, connected } = this.context();
    if (connection === undefined || !connected) {
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
}
