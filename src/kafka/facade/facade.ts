import {
  HOST_PROTOCOL_VERSION,
  type ConsumptionState,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostEvent,
  type HostEventListener,
  type KafkaMessage,
  type KafkaRuleSnapshot,
  type SecureConnectionInput,
  type StreamSkopeBackend,
} from "../contracts";
import {
  ConnectionAttemptSupersededError,
  InMemoryKafkaOperationalPreferenceStore,
  KafkaOperationalPreferenceService,
  type KafkaConnectionTemplateService,
  type KafkaProfileService,
  type KafkaProfileSnapshot,
  type KafkaRuleService,
  type KafkaTopicConfigurationServicePort,
  type KafkaTrustAcquisitionServicePort,
  type KafkaApplicationSession,
  type KafkaConnectionSnapshot,
  type KafkaLiveRuleRuntime,
} from "../application";
import { ActivityHistory } from "../../platform/activity";

import type { KafkaBackendFacadeOptions } from "./types";
import {
  backendAvailabilityEvent,
  connectionStateEvent,
  connectionFromCommand,
  defaultCorrelationId,
  defaultScheduleMessageFlush,
  failureResponse,
  fetchDescription,
  isStructuredFailure,
  profileActivityObject,
  profilesChangedEvent,
  ruleEvaluationEvent,
  rulesChangedEvent,
  sensitiveValues,
  successResponse,
  topicsChangedEvent,
  translateFacadeFailure,
  type ActiveFacadeConsumption,
  type ActivityInput,
  type FailureContext,
} from "./facade-support";
import { ruleActivityObject, ruleOperation, type RuleHostCommand } from "./rule-activity";
import {
  exploredKafkaMessage,
  internalLiveRuleFailureActivity,
  unavailableLiveRuleActivity,
} from "./live-rule-support";
import { flushFacadeMessages } from "./message-flush";
import { appendFacadeMessage } from "./message-queue";
import { executeTopicConfigurationCommand } from "./topic-configuration-facade";
import {
  createClusterDetailsService,
  executeClusterDetailsCommand,
} from "./cluster-diagnostics-facade";
import { executeConnectionTestCommand } from "./connection-test-facade";
import { ConsumerGroupFacadeController } from "./consumer-group-facade";
import {
  createLatencyService,
  executeLatencyCommand,
  invalidateLatencyEvent,
  latencyHistoryEvent,
  latencyEvent,
} from "./latency-facade";
import { executeProfileCommand, executeProfileTestCommand } from "./profile-facade";
import { resolveHostConnection } from "./host-connection";
import {
  createStreamMonitoring,
  emitConsumptionState,
  emitStreamMetrics,
  recordStreamQueueBounds,
  recordStreamQueueStart,
} from "./stream-monitor-facade";
import {
  executeTrustAcquisitionCommand,
  isTrustAcquisitionCommand,
} from "./trust-acquisition-facade";
import { executeOperationalPreferenceCommand } from "./operational-preference-facade";
import { executeTemplateCommand } from "./template-facade";
import { executeTrustRecipeCommand, isTrustRecipeCommand } from "./trust-recipe-facade";
import { executeTopicListCommand } from "./topic-list-facade";
import { ClusterServiceFacadeController } from "./cluster-service-facades";

export class KafkaBackendFacade implements StreamSkopeBackend {
  private activeConsumption: ActiveFacadeConsumption | undefined;
  private readonly activity = new ActivityHistory();
  private activitySequence = 0;
  private available = true;
  private messagePresentationPaused = false;
  private readonly consumerGroups;
  private readonly createCorrelationId;
  private readonly clusterDiagnostics;
  private readonly clusterServices;
  private readonly listeners = new Set<HostEventListener>();
  private readonly latencyProbe;
  private readonly monotonicNow;
  private readonly now;
  private readonly preferences: KafkaOperationalPreferenceService;
  private readonly scheduleMessageFlush;
  private sequence = 0;
  private shutdownPromise: Promise<void> | undefined;
  private readonly trustAcquisitions: KafkaTrustAcquisitionServicePort | undefined;

  constructor(
    private readonly session: KafkaApplicationSession,
    private readonly profiles: KafkaProfileService,
    private readonly templates: KafkaConnectionTemplateService,
    private readonly rules: KafkaRuleService,
    private readonly liveRules: KafkaLiveRuleRuntime,
    private readonly topicConfigurations: KafkaTopicConfigurationServicePort,
    options: KafkaBackendFacadeOptions = {},
  ) {
    this.clusterDiagnostics = createClusterDetailsService(session, options);
    this.latencyProbe = createLatencyService(session, options);
    this.createCorrelationId = options.createCorrelationId ?? defaultCorrelationId;
    this.monotonicNow = options.monotonicNow ?? ((): number => globalThis.performance.now());
    this.now = options.now ?? ((): Date => new Date());
    this.consumerGroups = new ConsumerGroupFacadeController({
      available: (): boolean => this.available,
      nextSequence: this.nextSequence.bind(this),
      now: this.now,
      publish: this.publish.bind(this),
      recordActivity: this.recordActivity.bind(this),
      session,
    });
    this.preferences =
      options.preferences ??
      new KafkaOperationalPreferenceService(
        new InMemoryKafkaOperationalPreferenceStore({
          durability: "session",
          state: "ready",
        }),
      );
    this.clusterServices = new ClusterServiceFacadeController({
      nextSequence: this.nextSequence.bind(this),
      now: this.now,
      ...(options.schemaRegistry === undefined ? {} : { schemaRegistry: options.schemaRegistry }),
      publish: this.publish.bind(this),
      recordActivity: this.recordActivity.bind(this),
      session,
      ...(options.transforms === undefined ? {} : { transforms: options.transforms }),
    });
    this.scheduleMessageFlush = options.scheduleMessageFlush ?? defaultScheduleMessageFlush;
    this.trustAcquisitions = options.trustAcquisitions;
  }

  connectionSnapshot(): KafkaConnectionSnapshot {
    return this.session.snapshot();
  }

  async execute(command: HostCommand): Promise<HostCommandResponse> {
    const correlationId = this.createCorrelationId();
    if (!this.available) {
      return failureResponse(
        command,
        this.translateFailure(new Error("Kafka application session is unavailable."), {
          activeStateChanged: false,
          connection: connectionFromCommand(command),
          correlationId,
        }).error,
      );
    }

    const templateBindings = {
      available: (): boolean => this.available,
      nextSequence: this.nextSequence.bind(this),
      publish: this.publish.bind(this),
      recordActivity: this.recordActivity.bind(this),
      templates: this.templates,
    };
    if (isTrustRecipeCommand(command)) {
      return executeTrustRecipeCommand(command, correlationId, {
        ...templateBindings,
        profiles: this.profiles,
      });
    }
    if (isTrustAcquisitionCommand(command)) {
      return executeTrustAcquisitionCommand(command, correlationId, {
        profiles: this.profiles,
        acquisitions: this.trustAcquisitions,
        available: this.available,
        recordActivity: this.recordActivity.bind(this),
      });
    }
    switch (command.command) {
      case "connection.connect":
        return this.connect(command, correlationId);
      case "connection.disconnect":
        return this.disconnect(command, correlationId);
      case "connection.test":
        return executeConnectionTestCommand(command, correlationId, {
          acquisitions: this.trustAcquisitions,
          available: this.available,
          recordActivity: this.recordActivity.bind(this),
          session: this.session,
        });
      case "profiles.connect":
        return this.connectProfile(command, correlationId);
      case "profiles.test":
        return executeProfileTestCommand(command, correlationId, {
          available: (): boolean => this.available,
          profiles: this.profiles,
          recordActivity: this.recordActivity.bind(this),
          session: this.session,
        });
      case "profiles.create":
      case "profiles.delete":
      case "profiles.list":
      case "profiles.binding.get":
      case "profiles.update":
        return executeProfileCommand(command, correlationId, {
          available: (): boolean => this.available,
          nextSequence: this.nextSequence.bind(this),
          profiles: this.profiles,
          publish: this.publish.bind(this),
          recordActivity: this.recordActivity.bind(this),
        });
      case "templates.create":
      case "templates.delete":
      case "templates.list":
      case "templates.select":
      case "templates.update":
        return executeTemplateCommand(command, correlationId, templateBindings);
      case "preferences.get":
      case "preferences.reset":
      case "preferences.update":
        return executeOperationalPreferenceCommand(command, correlationId, {
          nextSequence: this.nextSequence.bind(this),
          preferences: this.preferences,
          publish: this.publish.bind(this),
          recordActivity: this.recordActivity.bind(this),
        });
      case "rules.create":
      case "rules.delete":
      case "rules.evaluate":
      case "rules.list":
      case "rules.update":
      case "rules.validate":
        return this.changeRules(command, correlationId);
      case "topics.list":
        return executeTopicListCommand(command, correlationId, {
          now: this.now,
          publishTopics: this.publishTopics.bind(this),
          recordActivity: this.recordActivity.bind(this),
          recordFailureActivity: this.recordFailureActivity.bind(this),
          session: this.session,
          translateFailure: (error, requestId) =>
            this.translateFailure(error, {
              activeStateChanged: false,
              connection: undefined,
              correlationId: requestId,
            }),
        });
      case "consumerGroups.list":
      case "consumerGroups.load":
        return this.consumerGroups.execute(command, correlationId);
      case "schemas.list":
      case "schemas.load":
      case "schemas.compatibility.check":
      case "schemas.register":
      case "schemas.delete":
      case "acls.list":
      case "acls.create":
      case "acls.delete":
      case "transforms.list":
      case "transforms.load":
      case "transforms.logs.load":
      case "transforms.delete":
        return this.clusterServices.execute(command, correlationId);
      case "topicConfiguration.load":
      case "topicConfiguration.validate":
      case "topicConfiguration.apply":
      case "topicConfiguration.history":
        return executeTopicConfigurationCommand(command, correlationId, {
          nextSequence: this.nextSequence.bind(this),
          publish: this.publish.bind(this),
          recordActivity: this.recordActivity.bind(this),
          service: this.topicConfigurations,
          session: this.session,
        });
      case "clusterDetails.load":
      case "clusterDetails.export":
        return executeClusterDetailsCommand(command, correlationId, {
          nextSequence: this.nextSequence.bind(this),
          profiles: this.profiles,
          publish: this.publish.bind(this),
          recordActivity: this.recordActivity.bind(this),
          service: this.clusterDiagnostics,
          session: this.session,
        });
      case "latency.export":
      case "latency.start":
      case "latency.stop":
        return executeLatencyCommand(command, correlationId, {
          nextSequence: this.nextSequence.bind(this),
          publish: this.publish.bind(this),
          recordActivity: this.recordActivity.bind(this),
          service: this.latencyProbe,
        });
      case "messages.start":
        return this.startMessages(command, correlationId);
      case "messages.stop":
        return this.stopMessages(command, correlationId);
    }
  }

  shutdown(): Promise<void> {
    this.shutdownPromise ??= this.completeShutdown();
    return this.shutdownPromise;
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    const payload = this.available
      ? ({ state: "ready" } as const)
      : ({
          recovery: "Restart StreamSkope to create a new application session.",
          state: "unavailable",
        } as const);
    listener(backendAvailabilityEvent(payload, this.nextSequence()));
    return (): void => {
      this.listeners.delete(listener);
    };
  }

  private async changeRules(
    command: RuleHostCommand,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    const operation = ruleOperation(command);
    const object = ruleActivityObject(command);
    try {
      if (command.command === "rules.validate") {
        const result = this.rules.validate(command.payload.rule);
        this.publishRuleEvaluation({
          kind: "validation",
          requestId: command.id,
          results: [result],
        });
        this.recordActivity({
          correlationId,
          detail: `Rule validation completed with outcome ${result.outcome}.`,
          object,
          operation,
          outcome: "succeeded",
          severity: result.outcome === "invalid" ? "warning" : "info",
        });
        return successResponse(command, correlationId);
      }
      if (command.command === "rules.evaluate") {
        const results = this.rules.evaluate(command.payload);
        this.publishRuleEvaluation({
          kind: "evaluation",
          requestId: command.id,
          results,
        });
        this.recordActivity({
          correlationId,
          detail: `Offline evaluation completed for ${String(results.length)} rule${
            results.length === 1 ? "" : "s"
          }; no Kafka operation was invoked.`,
          object,
          operation,
          outcome: "succeeded",
          severity: "info",
        });
        return successResponse(command, correlationId);
      }

      let snapshot: KafkaRuleSnapshot;
      switch (command.command) {
        case "rules.create":
          snapshot = await this.rules.create(command.payload.rule);
          break;
        case "rules.delete":
          snapshot = await this.rules.delete(command.payload.name);
          break;
        case "rules.list":
          snapshot = await this.rules.list();
          break;
        case "rules.update":
          snapshot = await this.rules.update(command.payload.originalName, command.payload.rule);
          break;
      }
      this.liveRules.synchronize(snapshot);
      if (this.activeConsumption !== undefined) {
        this.publishConsumption(this.activeConsumption, this.activeConsumption.state);
      }
      this.publishRules(snapshot);
      this.recordActivity({
        correlationId,
        detail: `${operation} completed and the bounded rule catalog was refreshed.`,
        object,
        operation,
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      const safeError = isStructuredFailure(error)
        ? error
        : new Error("The rule operation failed without safe upstream detail.");
      const translated = this.translateFailure(safeError, {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      });
      if (command.command !== "rules.evaluate" && command.command !== "rules.validate") {
        this.publishRules(this.rules.currentSnapshot());
      }
      this.recordFailureActivity(object, operation, correlationId, translated.detail);
      return failureResponse(command, translated.error);
    }
  }

  private async completeShutdown(): Promise<void> {
    this.invalidateClusterState();
    this.trustAcquisitions?.clear();
    const consumption = this.activeConsumption;
    let shutdownFailure: unknown;
    try {
      await this.session.shutdown();
    } catch (error) {
      shutdownFailure = error;
    }
    if (consumption !== undefined && this.activeConsumption === consumption) {
      this.flushMessages(consumption, true);
      this.activeConsumption = undefined;
      this.liveRules.deactivate();
      this.publishConsumption(consumption, "stopped");
    } else {
      this.liveRules.deactivate();
    }
    this.available = false;
    this.publish(
      backendAvailabilityEvent(
        {
          recovery: "Restart StreamSkope to create a new application session.",
          state: "unavailable",
        },
        this.nextSequence(),
      ),
    );
    if (shutdownFailure !== undefined) {
      throw shutdownFailure instanceof Error
        ? shutdownFailure
        : new Error("Kafka application shutdown rejected with a non-error value.", {
            cause: shutdownFailure,
          });
    }
  }

  private async connect(
    command: Extract<HostCommand, { readonly command: "connection.connect" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    let acquisitionId: string | undefined;
    let connection: SecureConnectionInput | undefined;
    let connectionStarted = false;
    try {
      const resolved = resolveHostConnection(command.payload, this.trustAcquisitions);
      acquisitionId = resolved.acquisitionId;
      connection = resolved.connection;
      resolved.lifetimeSignal?.throwIfAborted();
      this.invalidateClusterState();
      this.clearActiveProfile();
      const operation = this.session.connect(connection, resolved.lifetimeSignal);
      connectionStarted = true;
      this.publishConnection(this.session.snapshot());
      await operation;
      if (acquisitionId !== undefined) {
        this.trustAcquisitions?.consume(acquisitionId);
      }
      this.publishConnection(this.session.snapshot());
      this.publish(
        latencyEvent({ evidence: null, request: null, state: "idle" }, this.nextSequence()),
      );
      this.recordActivity({
        correlationId,
        detail: "OAuth, TLS, Kafka authentication and broker metadata were confirmed.",
        object: connection.name,
        operation: "Connect",
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      const translated = this.translateFailure(error, {
        activeStateChanged:
          connectionStarted && !(error instanceof ConnectionAttemptSupersededError),
        connection: connection ?? command.payload,
        correlationId,
      });
      if (connectionStarted && !(error instanceof ConnectionAttemptSupersededError)) {
        this.publishConnection(this.session.snapshot(), translated.error);
      }
      this.recordFailureActivity(
        command.payload.name,
        "Connect",
        correlationId,
        translated.detail,
        sensitiveValues(connection ?? command.payload),
        error instanceof ConnectionAttemptSupersededError ? "cancelled" : "failed",
      );
      return failureResponse(command, translated.error);
    }
  }

  private async connectProfile(
    command: Extract<HostCommand, { readonly command: "profiles.connect" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    let connection: SecureConnectionInput | undefined;
    let connectionStarted = false;
    try {
      connection = await this.profiles.resolveConnection(command.payload.profileId);
      this.invalidateClusterState();
      this.clearActiveProfile();
      const operation = this.session.connect(connection);
      connectionStarted = true;
      this.publishConnection(this.session.snapshot());
      await operation;
      this.publishConnection(this.session.snapshot());
      this.publish(
        latencyEvent({ evidence: null, request: null, state: "idle" }, this.nextSequence()),
      );
      this.publishProfiles(await this.profiles.markActive(command.payload.profileId));
      this.recordActivity({
        correlationId,
        detail:
          "Protected profile values were resolved in the host and Kafka confirmed broker metadata.",
        object: profileActivityObject(connection),
        operation: "Connect profile",
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      const translated = this.translateFailure(error, {
        activeStateChanged:
          connectionStarted && !(error instanceof ConnectionAttemptSupersededError),
        connection,
        correlationId,
      });
      if (connectionStarted && !(error instanceof ConnectionAttemptSupersededError)) {
        this.publishConnection(this.session.snapshot(), translated.error);
      }
      this.publishProfiles(this.profiles.currentSnapshot());
      this.recordFailureActivity(
        connection === undefined ? command.payload.profileId : profileActivityObject(connection),
        "Connect profile",
        correlationId,
        translated.detail,
        sensitiveValues(connection),
        error instanceof ConnectionAttemptSupersededError ? "cancelled" : "failed",
      );
      return failureResponse(command, translated.error);
    }
  }

  private async disconnect(
    command: Extract<HostCommand, { readonly command: "connection.disconnect" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    this.invalidateClusterState();
    const connectionName = this.session.snapshot().connectionName ?? "No active connection";
    const operation = this.session.disconnect();
    this.publishConnection(this.session.snapshot());
    try {
      await operation;
      this.publishConnection(this.session.snapshot());
      this.clearActiveProfile();
      this.recordActivity({
        correlationId,
        detail: "Kafka resources closed and the active connection was cleared.",
        object: connectionName,
        operation: "Disconnect",
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      const translated = this.translateFailure(error, {
        activeStateChanged: true,
        connection: undefined,
        correlationId,
      });
      this.publishConnection(this.session.snapshot(), translated.error);
      this.recordFailureActivity(connectionName, "Disconnect", correlationId, translated.detail);
      return failureResponse(command, translated.error);
    }
  }

  private nextSequence(): number {
    this.sequence += 1;
    return this.sequence;
  }

  private invalidateLatency(): void {
    this.publish(invalidateLatencyEvent(this.latencyProbe, this.nextSequence()));
    this.publish(latencyHistoryEvent(this.latencyProbe.historySnapshot(), this.nextSequence()));
  }

  private invalidateConsumerGroups(): void {
    this.consumerGroups.invalidate();
  }

  private invalidateClusterState(): void {
    this.clusterDiagnostics.clear();
    this.invalidateLatency();
    this.invalidateConsumerGroups();
    this.clusterServices.invalidate();
  }

  private enqueueMessage(consumption: ActiveFacadeConsumption, message: KafkaMessage): void {
    if (this.activeConsumption !== consumption || message.topic !== consumption.request.topic) {
      return;
    }
    consumption.receivedMessages += 1;
    const exploredMessage = exploredKafkaMessage(this.liveRules, message);
    const { ruleEvaluation } = exploredMessage;
    if (
      ruleEvaluation.state === "unavailable" &&
      ruleEvaluation.reason === "internal" &&
      !consumption.ruleFailureRecorded
    ) {
      consumption.ruleFailureRecorded = true;
      this.recordActivity(internalLiveRuleFailureActivity(consumption));
    }
    recordStreamQueueStart(consumption, this.monotonicNow);
    appendFacadeMessage(consumption, {
      message: exploredMessage,
      ruleOutput: { ...this.preferences.currentSnapshot().preferences.rules },
    });
    recordStreamQueueBounds(consumption);
    if (consumption.state === "empty" || consumption.state === "loading") {
      this.publishConsumption(
        consumption,
        consumption.request.mode === "tail" ? "streaming" : "fetching",
      );
    }
    this.scheduleConsumptionFlush(consumption);
  }

  private cancelConsumptionFlush(consumption: ActiveFacadeConsumption): void {
    consumption.cancelScheduledFlush?.();
    consumption.cancelScheduledFlush = undefined;
    consumption.flushScheduled = false;
  }

  setMessagePresentationPaused(paused: boolean): void {
    this.messagePresentationPaused = paused;
    const consumption = this.activeConsumption;
    if (!consumption) return;
    if (paused) this.cancelConsumptionFlush(consumption);
    else if (consumption.messages.length > 0) this.scheduleConsumptionFlush(consumption);
  }

  private scheduleConsumptionFlush(consumption: ActiveFacadeConsumption): void {
    if (consumption.flushScheduled || this.messagePresentationPaused) return;
    consumption.flushScheduled = true;
    let invoked = false;
    const cancel = this.scheduleMessageFlush(() => {
      invoked = true;
      consumption.cancelScheduledFlush = undefined;
      consumption.flushScheduled = false;
      if (this.activeConsumption === consumption) this.flushMessages(consumption);
    }, consumption.streamTuning.intervalMs);
    if (!invoked && typeof cancel === "function") {
      consumption.cancelScheduledFlush = cancel;
    }
  }

  private flushMessages(consumption: ActiveFacadeConsumption, drainAll = false): void {
    this.cancelConsumptionFlush(consumption);
    if (consumption.messages.length === 0 || (this.messagePresentationPaused && !drainAll)) {
      return;
    }
    const { completedAtMs, droppedSincePrevious } = flushFacadeMessages(consumption, drainAll, {
      monotonicNow: this.monotonicNow,
      nextSequence: this.nextSequence.bind(this),
      publish: this.publish.bind(this),
      recordActivity: this.recordActivity.bind(this),
    });
    this.publishStreamMetrics(consumption, consumption.state, droppedSincePrevious);
    if (!drainAll && consumption.messages.length > 0) {
      consumption.streamMonitoring.queueStartedAtMs = completedAtMs;
      this.scheduleConsumptionFlush(consumption);
    }
  }

  private publish(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  private publishConnection(snapshot: KafkaConnectionSnapshot, error?: HostError): void {
    this.publish(connectionStateEvent(snapshot, this.nextSequence(), error));
  }

  private publishStreamMetrics(
    consumption: ActiveFacadeConsumption,
    state: ConsumptionState,
    droppedSincePrevious = Math.max(
      0,
      consumption.droppedMessages - consumption.streamMonitoring.lastReportedDroppedMessages,
    ),
  ): void {
    emitStreamMetrics(
      consumption,
      state,
      {
        connectionName: this.session.snapshot().connectionName,
        nextSequence: this.nextSequence.bind(this),
        publish: this.publish.bind(this),
        sampledAt: this.now().toISOString(),
      },
      droppedSincePrevious,
    );
  }

  private publishConsumption(
    consumption: ActiveFacadeConsumption,
    state: ConsumptionState,
    error?: HostError,
  ): void {
    emitConsumptionState(consumption, state, error, this.liveRules.capability(), {
      connectionName: this.session.snapshot().connectionName,
      nextSequence: this.nextSequence.bind(this),
      publish: this.publish.bind(this),
      sampledAt: this.now().toISOString(),
    });
  }

  private clearActiveProfile(): void {
    const wasActive = this.profiles.currentSnapshot().profiles.some((profile) => profile.active);
    const snapshot = this.profiles.clearActive();
    if (wasActive) {
      this.publishProfiles(snapshot);
    }
  }

  private publishProfiles(snapshot: KafkaProfileSnapshot): void {
    this.publish(profilesChangedEvent(snapshot, this.nextSequence()));
  }

  private publishRuleEvaluation(
    payload: Extract<HostEvent, { readonly event: "rules.evaluation" }>["payload"],
  ): void {
    this.publish(ruleEvaluationEvent(payload, this.nextSequence()));
  }

  private publishRules(snapshot: KafkaRuleSnapshot): void {
    this.publish(rulesChangedEvent(snapshot, this.nextSequence()));
  }

  private publishTopics(
    payload: Extract<HostEvent, { readonly event: "topics.changed" }>["payload"],
  ): void {
    this.publish(topicsChangedEvent(payload, this.nextSequence()));
  }

  private recordActivity(input: ActivityInput): void {
    const entry = this.activity.record(
      {
        correlationId: input.correlationId,
        detail: input.detail,
        id: `activity-${input.correlationId}-${String(++this.activitySequence)}`,
        object: input.object,
        operation: input.operation,
        outcome: input.outcome,
        severity: input.severity,
        timestamp: this.now().toISOString(),
      },
      input.sensitiveValues,
    );
    this.publish({
      event: "activity.recorded",
      payload: entry,
      sequence: this.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }

  private recordFailureActivity(
    object: string,
    operation: string,
    correlationId: string,
    detail: string,
    secrets: readonly string[] = [],
    outcome: "cancelled" | "failed" = "failed",
  ): void {
    this.recordActivity({
      correlationId,
      detail,
      object,
      operation,
      outcome,
      sensitiveValues: secrets,
      severity: outcome === "cancelled" ? "warning" : "error",
    });
  }

  private async startMessages(
    command: Extract<HostCommand, { readonly command: "messages.start" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    const preferenceSnapshot = await this.preferences.get();
    const previous = this.activeConsumption;
    if (previous !== undefined) {
      this.cancelConsumptionFlush(previous);
      previous.messages.length = 0;
      previous.queuedBytes = 0;
    }
    const consumption: ActiveFacadeConsumption = {
      cancelScheduledFlush: undefined,
      correlationId,
      droppedMessages: 0,
      flushScheduled: false,
      messages: [],
      queuedBytes: 0,
      receivedMessages: 0,
      request: command.payload,
      ruleFailureRecorded: false,
      state: "loading",
      streamMonitoring: createStreamMonitoring(this.monotonicNow()),
      streamTuning: {
        ...preferenceSnapshot.preferences.stream,
        source: preferenceSnapshot.store.state === "ready" ? "confirmed" : "factory-fallback",
      },
    };
    this.activeConsumption = consumption;
    let startAccepted = false;
    let pendingTerminal: (() => void) | undefined;
    const runTerminal = (terminal: () => void): void => {
      if (startAccepted) {
        terminal();
      } else {
        pendingTerminal = terminal;
      }
    };
    try {
      await this.session.stopConsumption();
      if (this.activeConsumption !== consumption) {
        throw new ConnectionAttemptSupersededError();
      }
      const capability = await this.liveRules.prepare(command.payload.topic);
      if (this.activeConsumption !== consumption) {
        throw new ConnectionAttemptSupersededError();
      }
      this.publishConsumption(consumption, "loading");
      if (capability.state === "unavailable") {
        this.recordActivity(unavailableLiveRuleActivity(consumption, capability));
      }
      await this.session.startConsumption(command.payload, {
        onComplete: (): void => {
          runTerminal(() => {
            if (this.activeConsumption !== consumption) {
              return;
            }
            this.flushMessages(consumption, true);
            this.activeConsumption = undefined;
            this.liveRules.deactivate();
            const state = consumption.receivedMessages === 0 ? "empty" : "complete";
            this.publishConsumption(consumption, state);
            this.recordActivity({
              correlationId: consumption.correlationId,
              detail: `${fetchDescription(consumption.request)} completed with ${String(
                consumption.receivedMessages,
              )} message${consumption.receivedMessages === 1 ? "" : "s"}.`,
              object: consumption.request.topic,
              operation: "Consume messages",
              outcome: "succeeded",
              severity: "info",
            });
          });
        },
        onEmpty: (): void => {
          if (this.activeConsumption === consumption) {
            this.flushMessages(consumption, true);
            this.publishConsumption(consumption, "empty");
          }
        },
        onFailure: (error): void => {
          runTerminal(() => {
            if (this.activeConsumption !== consumption) {
              return;
            }
            this.flushMessages(consumption, true);
            this.activeConsumption = undefined;
            this.liveRules.deactivate();
            const translated = this.translateFailure(error, {
              activeStateChanged: false,
              connection: undefined,
              correlationId: consumption.correlationId,
            });
            this.publishConsumption(consumption, "failed", translated.error);
            this.recordFailureActivity(
              consumption.request.topic,
              "Consume messages",
              consumption.correlationId,
              `${fetchDescription(consumption.request)} failed. ${translated.detail}`,
            );
          });
        },
        onMessage: (message): void => {
          this.enqueueMessage(consumption, message);
        },
      });
      if (this.activeConsumption !== consumption) {
        throw new ConnectionAttemptSupersededError();
      }
      if (consumption.state === "loading") {
        this.publishConsumption(
          consumption,
          consumption.request.mode === "tail" ? "streaming" : "fetching",
        );
      }
      this.recordActivity({
        correlationId,
        detail: `${fetchDescription(consumption.request)} started.`,
        object: consumption.request.topic,
        operation: "Consume messages",
        outcome: "started",
        severity: "info",
      });
      startAccepted = true;
      pendingTerminal?.();
      return successResponse(command, correlationId);
    } catch (error) {
      const operationError =
        this.activeConsumption !== consumption ||
        (error instanceof Error && error.name === "AbortError")
          ? new ConnectionAttemptSupersededError()
          : error;
      const translated = this.translateFailure(operationError, {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      });
      if (this.activeConsumption === consumption) {
        this.cancelConsumptionFlush(consumption);
        consumption.messages.length = 0;
        consumption.queuedBytes = 0;
        this.activeConsumption = undefined;
        this.liveRules.deactivate();
        this.publishConsumption(consumption, "failed", translated.error);
      }
      this.recordFailureActivity(
        command.payload.topic,
        "Consume messages",
        correlationId,
        `${fetchDescription(command.payload)} failed. ${translated.detail}`,
        [],
        operationError instanceof ConnectionAttemptSupersededError ? "cancelled" : "failed",
      );
      return failureResponse(command, translated.error);
    }
  }

  private async stopMessages(
    command: Extract<HostCommand, { readonly command: "messages.stop" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    const consumption = this.activeConsumption;
    try {
      await this.session.stopConsumption();
      if (consumption !== undefined && this.activeConsumption === consumption) {
        this.flushMessages(consumption, true);
        this.activeConsumption = undefined;
        this.liveRules.deactivate();
        this.publishConsumption(consumption, "stopped");
      } else if (consumption === undefined) {
        this.liveRules.deactivate();
        this.publish({
          event: "consumption.state",
          payload: {
            droppedMessages: 0,
            receivedMessages: 0,
            request: null,
            ruleEvaluation: this.liveRules.capability(),
            state: "stopped",
          },
          sequence: this.nextSequence(),
          version: HOST_PROTOCOL_VERSION,
        });
      }
      this.recordActivity({
        correlationId,
        detail:
          consumption === undefined
            ? "No active Kafka message operation remained to stop."
            : `${fetchDescription(consumption.request)} stopped and the consumer closed.`,
        object: consumption?.request.topic ?? "No active topic",
        operation: "Stop consumption",
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      const translated = this.translateFailure(error, {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      });
      if (consumption !== undefined && this.activeConsumption === consumption) {
        this.flushMessages(consumption, true);
        this.publishConsumption(consumption, "failed", translated.error);
      }
      this.recordFailureActivity(
        consumption?.request.topic ?? "No active topic",
        "Stop consumption",
        correlationId,
        translated.detail,
      );
      return failureResponse(command, translated.error);
    }
  }

  private translateFailure(
    error: unknown,
    context: FailureContext,
  ): ReturnType<typeof translateFacadeFailure> {
    return translateFacadeFailure(error, context, this.available);
  }
}
