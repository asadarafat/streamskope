import { SavedReplayDestinations } from "../application/replay-destination";
import { KafkaReviewedWriteService } from "../application/reviewed-write-service";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostEvent,
  type HostEventListener,
  type KafkaRuleSnapshot,
  type SecureConnectionInput,
  type StreamSkopeBackend,
} from "../contracts";
import {
  ConnectionAttemptSupersededError,
  InMemoryKafkaOperationalPreferenceStore,
  KafkaOperationalPreferenceService,
  KafkaQueryLibrary,
  type KafkaTrustRecipeLibrary,
  type KafkaProfileService,
  type KafkaProfileSnapshot,
  type KafkaRuleService,
  type KafkaTopicConfigurationServicePort,
  type KafkaTrustAcquisitionServicePort,
  type KafkaApplicationSession,
  type KafkaConnectionSnapshot,
  type KafkaLiveRuleRuntime,
} from "../application";
import { ActivityHistory } from "../../../platform/activity";

import { RelationshipFacade } from "./relationship-facade";
import { ObservationFacade } from "./observation-facade";
import { ConnectFacade } from "./connect-facade";
import { EnvironmentFacade } from "./environment-facade";
import { AclReviewFacade } from "./acl-review-facade";
import { RecordReplayFacade } from "./record-replay-facade";
import { OffsetResetFacade } from "./offset-reset-facade";
import { CorrelationTraceFacade } from "./correlation-trace-facade";
import { SchemaSamplesFacade } from "./schema-samples-facade";
import { SchemaInspectionFacade } from "./schema-inspection-facade";
import { executeWriteCommand } from "./write-facade";
import { RecordCodecFacade } from "./record-codec-facade";
import { KafkaCommandProtection } from "./command-protection";
import { KafkaCommandAdmission } from "./command-admission";
import { executeQueryCommand } from "./query-facade";
import { ConsumptionFacadeController } from "./consumption-facade";
import type { KafkaBackendFacadeOptions } from "./types";
import {
  backendAvailabilityEvent,
  connectionStateEvent,
  connectionFromCommand,
  defaultCorrelationId,
  failureResponse,
  isStructuredFailure,
  profileActivityObject,
  profilesChangedEvent,
  ruleEvaluationEvent,
  rulesChangedEvent,
  sensitiveValues,
  successResponse,
  topicsChangedEvent,
  translateFacadeFailure,
  type ActivityInput,
  type FailureContext,
} from "./facade-support";
import { ruleActivityObject, ruleOperation, type RuleHostCommand } from "./rule-activity";
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
  executeTrustAcquisitionCommand,
  isTrustAcquisitionCommand,
} from "./trust-acquisition-facade";
import { executeOperationalPreferenceCommand } from "./operational-preference-facade";
import { executeTrustRecipeCommand, isTrustRecipeCommand } from "./trust-recipe-facade";
import { executeTopicListCommand } from "./topic-list-facade";
import { ClusterServiceFacadeController } from "./cluster-service-facades";
import { PluginFacadeController } from "./plugin-facade";

export class KafkaBackendFacade implements StreamSkopeBackend {
  private readonly activity = new ActivityHistory();
  private activitySequence = 0;
  private available = true;
  private readonly commandAdmission = new KafkaCommandAdmission();
  private connectionLifetime = new AbortController();
  private connectionIntent = 0;
  private connectionPluginId: string | undefined;
  private readonly consumption: ConsumptionFacadeController;
  private consumptionCommandIntent = 0;
  private readonly consumerGroups;
  private readonly createCorrelationId;
  private readonly clusterDiagnostics;
  private readonly clusterServices;
  private readonly connectService: ConnectFacade;
  private readonly relationships: RelationshipFacade;
  private readonly observations: ObservationFacade;
  private readonly environments: EnvironmentFacade;
  private readonly plugins;
  private readonly listeners = new Set<HostEventListener>();
  private readonly latencyProbe;
  private readonly now;
  private readonly aclReviews: AclReviewFacade;
  private readonly offsetResets: OffsetResetFacade;
  private readonly recordReplay: RecordReplayFacade;
  private readonly writes: KafkaReviewedWriteService;
  private readonly schemaSamples: SchemaSamplesFacade;
  private readonly correlationTrace: CorrelationTraceFacade;
  private readonly schemaInspection: SchemaInspectionFacade;
  private readonly recordCodecs: RecordCodecFacade;
  private readonly queries: KafkaQueryLibrary;
  private readonly preferences: KafkaOperationalPreferenceService;
  private readonly protection: KafkaCommandProtection;
  private authorizationIntent = 0;
  private sequence = 0;
  private shutdownPromise: Promise<void> | undefined;
  private readonly trustAcquisitions: KafkaTrustAcquisitionServicePort | undefined;

  constructor(
    private readonly session: KafkaApplicationSession,
    private readonly profiles: KafkaProfileService,
    private readonly recipes: KafkaTrustRecipeLibrary,
    private readonly rules: KafkaRuleService,
    private readonly liveRules: KafkaLiveRuleRuntime,
    private readonly topicConfigurations: KafkaTopicConfigurationServicePort,
    options: KafkaBackendFacadeOptions = {},
  ) {
    this.correlationTrace = new CorrelationTraceFacade(
      session,
      options.recordCodec,
      options.schemaLookup,
    );
    this.aclReviews = new AclReviewFacade(session, this.recordActivity.bind(this));
    this.offsetResets = new OffsetResetFacade(session, this.recordActivity.bind(this));
    this.recordReplay = new RecordReplayFacade(
      session,
      profiles,
      options.replayConnections,
      this.recordActivity.bind(this),
    );
    this.writes = new KafkaReviewedWriteService(() => session.writeContext());
    this.schemaSamples = new SchemaSamplesFacade(
      session,
      options.recordCodec,
      options.schemaLookup,
      options.sampleGenerator,
      this.recordActivity.bind(this),
    );
    this.schemaInspection = new SchemaInspectionFacade(session, options.schemaLookup);
    this.recordCodecs = new RecordCodecFacade(session, options.recordCodec, options.schemaLookup);
    this.queries = options.queries ?? new KafkaQueryLibrary();
    this.clusterDiagnostics = createClusterDetailsService(session, options);
    this.latencyProbe = createLatencyService(session, options);
    this.createCorrelationId = options.createCorrelationId ?? defaultCorrelationId;
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
    this.protection = new KafkaCommandProtection({
      preferences: this.preferences,
      disconnected: (): boolean =>
        ["disconnected", "failed"].includes(this.session.snapshot().state),
      pendingPluginWork: (): Promise<boolean> => this.plugins.hasPendingWork(),
      managedProfile: async (command): Promise<boolean> => {
        if (command.command === "profiles.test" && command.payload.mode === "create")
          return command.payload.profile.source !== undefined;
        const profileId = "profileId" in command.payload ? command.payload.profileId : undefined;
        const snapshot = await this.profiles.list();
        return snapshot.profiles.find((entry) => entry.id === profileId)?.source !== undefined;
      },
      rejected: (command, error): void =>
        this.recordFailureActivity(
          "Record protection",
          command.command,
          error.correlationId,
          `${error.summary} ${error.recovery}`,
        ),
    });
    const publish = this.publish.bind(this);
    const nextSequence = this.nextSequence.bind(this);
    this.relationships = new RelationshipFacade(session, options.connect, options.schemaRegistry);
    this.observations = new ObservationFacade(
      session,
      options.observationStore,
      this.recordActivity.bind(this),
    );
    this.connectService = new ConnectFacade(
      session,
      options.connect,
      this.recordActivity.bind(this),
    );
    this.environments = new EnvironmentFacade(
      session,
      options.replayConnections
        ? new SavedReplayDestinations(profiles, options.replayConnections)
        : undefined,
      this.recordActivity.bind(this),
    );
    this.clusterServices = new ClusterServiceFacadeController({
      ...options,
      nextSequence,
      now: this.now,
      publish,
      recordActivity: this.recordActivity.bind(this),
      session,
    });
    this.plugins = new PluginFacadeController({
      ...(options.plugins === undefined ? {} : { runtime: options.plugins }),
      session,
      profiles,
      publish,
      nextSequence,
      execute: this.executeInternal.bind(this),
      disconnectPluginConnection: this.disconnectPluginConnection.bind(this),
      assertRemoteWriteAllowed: (): void => {
        if (this.preferences.currentSnapshot().preferences.protection.readOnly)
          throw Object.assign(new Error("Read-only mode blocks managed plugin connections."), {
            code: "AUTHORIZATION_DENIED",
            stage: "authorization",
            retryable: false,
            recovery:
              "Use an ordinary Kafka profile or deliberately disable read-only before invoking plugin lifecycle hooks.",
          });
      },
      recordActivity: this.recordActivity.bind(this),
    });
    this.consumption = new ConsumptionFacadeController({
      session,
      liveRules,
      preferences: this.preferences,
      monotonicNow: options.monotonicNow ?? ((): number => globalThis.performance.now()),
      now: this.now,
      nextSequence,
      publish,
      recordActivity: this.recordActivity.bind(this),
      recordFailureActivity: this.recordFailureActivity.bind(this),
      translateFailure: this.translateFailure.bind(this),
      ...(options.scheduleMessageFlush === undefined
        ? {}
        : { scheduleMessageFlush: options.scheduleMessageFlush }),
    });
    this.trustAcquisitions = options.trustAcquisitions;
  }

  connectionSnapshot(): KafkaConnectionSnapshot {
    return this.session.snapshot();
  }

  execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    return this.commandAdmission.track(() => this.executeCommand(command, false));
  }

  private executeInternal<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  private executeInternal(command: HostCommand): Promise<HostCommandResponse> {
    return this.commandAdmission.track(() => this.executeCommand(command, true));
  }

  private async executeCommand(
    command: HostCommand,
    internal: boolean,
  ): Promise<HostCommandResponse> {
    const correlationId = this.createCorrelationId();
    if (!this.commandAdmission.accepts(internal))
      return this.unavailableResponse(command, correlationId);
    if (
      ["connection.connect", "profiles.connect", "connection.disconnect"].includes(command.command)
    )
      this.authorizationIntent += 1;
    const intent = this.authorizationIntent;
    if (command.command === "messages.start" || command.command === "messages.stop") {
      this.consumptionCommandIntent += 1;
    }
    const consumptionIntent = this.consumptionCommandIntent;
    return this.protection.execute(command, correlationId, () => {
      if (!this.commandAdmission.accepts(internal))
        return Promise.resolve(this.unavailableResponse(command, correlationId));
      return intent === this.authorizationIntent &&
        (command.command !== "messages.start" ||
          consumptionIntent === this.consumptionCommandIntent)
        ? this.dispatch(command, correlationId)
        : Promise.resolve(
            failureResponse(
              command,
              this.translateFailure(new ConnectionAttemptSupersededError(), {
                activeStateChanged: false,
                connection: undefined,
                correlationId,
              }).error,
            ),
          );
    });
  }

  private unavailableResponse(command: HostCommand, correlationId: string): HostCommandResponse {
    return failureResponse(
      command,
      translateFacadeFailure(
        new Error("Kafka application session is unavailable."),
        {
          activeStateChanged: false,
          connection: connectionFromCommand(command),
          correlationId,
        },
        false,
      ).error,
    );
  }

  private async dispatch(
    command: HostCommand,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    if (!this.available) {
      return this.unavailableResponse(command, correlationId);
    }

    const recipeBindings = {
      available: (): boolean => this.available,
      nextSequence: this.nextSequence.bind(this),
      publish: this.publish.bind(this),
      recordActivity: this.recordActivity.bind(this),
      recipes: this.recipes,
    };
    if (isTrustRecipeCommand(command)) {
      return executeTrustRecipeCommand(command, correlationId, {
        ...recipeBindings,
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
      case "relationships.capture":
      case "relationships.cancel":
        return this.relationships.execute(command, correlationId);
      case "observations.capture":
      case "observations.history":
      case "observations.cancel":
      case "observations.clear":
        return this.observations.execute(command, correlationId);
      case "connect.list":
      case "connect.load":
      case "connect.validate":
      case "connect.review":
      case "connect.apply":
        return this.connectService.execute(command, correlationId);
      case "environments.capture":
      case "environments.review":
      case "environments.apply":
        return this.environments.execute(command, correlationId);
      case "acls.access.explain":
      case "acls.change.review":
      case "acls.change.apply":
        return this.aclReviews.execute(command, correlationId);
      case "records.replay.review":
      case "records.replay.apply":
      case "records.replay.cancel":
        return this.recordReplay.execute(command, correlationId);
      case "records.trace":
      case "records.trace.cancel":
        return this.correlationTrace.execute(command, correlationId);
      case "schemas.client":
      case "schemas.samples":
      case "records.batch.review":
      case "records.batch.apply":
      case "records.batch.cancel":
        return this.schemaSamples.execute(command, correlationId);
      case "schemas.inspect":
        return this.schemaInspection.execute(command, correlationId);
      case "records.decode":
        return this.recordCodecs.execute(command, correlationId);
      case "writes.review":
      case "writes.apply":
        return executeWriteCommand(
          command,
          correlationId,
          this.writes,
          this.recordActivity.bind(this),
        );
      case "queries.list":
      case "queries.put":
      case "queries.delete":
        return executeQueryCommand(command, correlationId, this.queries);
      case "plugin.execute":
      case "plugins.list":
      case "plugins.catalog":
      case "plugins.change.prepare":
      case "plugins.renderer.failed":
      case "plugins.install":
      case "plugins.remove":
      case "plugins.restart":
      case "plugins.exit.prepare":
      case "plugins.exit.resolve":
        return this.plugins.execute(command, correlationId);
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
          validatePluginProfile: this.plugins.validateProfile.bind(this.plugins),
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
      case "consumerGroups.reset.review":
      case "consumerGroups.reset.apply":
        return this.offsetResets.execute(command, correlationId);
      case "consumerGroups.list":
      case "consumerGroups.load":
        return this.consumerGroups.execute(command, correlationId);
      case "topicConfiguration.load":
      case "topicConfiguration.validate":
      case "topicConfiguration.apply":
      case "topicConfiguration.history":
        return executeTopicConfigurationCommand(command, correlationId, {
          currentConnection: () => this.session.writeContext()?.connection,
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
        return this.consumption.startMessages(command, correlationId);
      case "messages.stop":
        return this.consumption.stopMessages(command, correlationId);
      default:
        return this.clusterServices.execute(command, correlationId);
    }
  }

  shutdown(): Promise<void> {
    if (this.shutdownPromise === undefined) {
      this.commandAdmission.close();
      let resolve!: () => void;
      let reject!: (reason: unknown) => void;
      this.shutdownPromise = new Promise<void>((complete, fail) => {
        resolve = complete;
        reject = fail;
      });
      this.completeShutdown().then(resolve, reject);
    }
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

  setMessagePresentationPaused(paused: boolean): void {
    this.consumption.setMessagePresentationPaused(paused);
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
      this.consumption.refreshRuleCapability();
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
    this.connectionIntent += 1;
    this.beginConnectionLifetime();
    const finishConsumption = this.consumption.prepareShutdown();
    const cleanups = [
      { owner: "Observations", run: (): void => this.observations.cancel() },
      { owner: "Relationships", run: (): void => this.relationships.cancel() },
      { owner: "Environments", run: (): void => this.environments.cancel() },
      { owner: "Correlation trace", run: (): void => this.correlationTrace.invalidate() },
      { owner: "Record codecs", run: (): void => this.recordCodecs.invalidate() },
      { owner: "Schema inspection", run: (): void => this.schemaInspection.invalidate() },
      { owner: "Schema samples", run: (): void => this.schemaSamples.invalidate() },
      { owner: "Cluster diagnostics", run: (): void => this.clusterDiagnostics.clear() },
      { owner: "Latency", run: (): void => this.invalidateLatency() },
      { owner: "Consumer groups", run: (): void => this.consumerGroups.invalidate() },
      { owner: "Cluster services", run: (): void => this.clusterServices.invalidate() },
      { owner: "Trust acquisition", run: (): void => this.trustAcquisitions?.clear() },
      { owner: "Record replay", run: (): Promise<void> => this.recordReplay.invalidate() },
      { owner: "Kafka session", run: (): Promise<void> => this.session.shutdown() },
      { owner: "Plugins", run: (): Promise<void> => this.plugins.close() },
      { owner: "Queries", run: (): Promise<void> => this.queries.idle() },
      { owner: "Observations", run: (): Promise<void> => this.observations.idle() },
      { owner: "Relationships", run: (): Promise<void> => this.relationships.idle() },
      { owner: "Environments", run: (): Promise<void> => this.environments.idle() },
    ];
    const results = await Promise.allSettled(
      cleanups.map(({ run }) => {
        try {
          return Promise.resolve(run());
        } catch {
          return Promise.reject(new Error("Application cleanup invocation failed."));
        }
      }),
    );
    // Plugin cleanup may admit owned commands while its close hook is running.
    await this.commandAdmission.idle();
    const failures = results.flatMap((result, index) =>
      result.status === "rejected"
        ? [new Error(`${cleanups[index]?.owner ?? "Application"} cleanup failed.`)]
        : [],
    );
    const shutdownFailure =
      failures.length === 0
        ? undefined
        : new AggregateError(failures, "Kafka application resources did not close cleanly.");
    finishConsumption(shutdownFailure);
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
    if (shutdownFailure !== undefined) throw shutdownFailure;
  }

  private async connect(
    command: Extract<HostCommand, { readonly command: "connection.connect" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    const intent = ++this.connectionIntent;
    let acquisitionId: string | undefined;
    let connection: SecureConnectionInput | undefined;
    let connectionStarted = false;
    try {
      const resolved = resolveHostConnection(command.payload, this.trustAcquisitions);
      acquisitionId = resolved.acquisitionId;
      connection = resolved.connection;
      resolved.lifetimeSignal?.throwIfAborted();
      const lifetime = this.beginConnectionLifetime();
      const signal =
        resolved.lifetimeSignal === undefined
          ? lifetime
          : AbortSignal.any([lifetime, resolved.lifetimeSignal]);
      this.invalidateClusterState();
      this.clearActiveProfile();
      this.assertConnectionIntent(intent);
      signal.throwIfAborted();
      const operation = this.session.connect(connection, signal);
      connectionStarted = true;
      this.publishConnection(this.session.snapshot());
      await operation;
      lifetime.throwIfAborted();
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
    const intent = ++this.connectionIntent;
    let connection: SecureConnectionInput | undefined;
    let connectionStarted = false;
    try {
      connection = await this.profiles.resolveConnection(command.payload.profileId);
      const profile = this.profiles
        .currentSnapshot()
        .profiles.find((entry) => entry.id === command.payload.profileId);
      const resolvedConnection = connection;
      const connect = async (): Promise<void> => {
        this.assertConnectionIntent(intent);
        const lifetime = this.beginConnectionLifetime(profile?.source?.pluginId);
        this.invalidateClusterState();
        this.clearActiveProfile();
        this.assertConnectionIntent(intent);
        lifetime.throwIfAborted();
        const operation = this.session.connect(resolvedConnection, lifetime);
        connectionStarted = true;
        this.publishConnection(this.session.snapshot());
        await operation;
        lifetime.throwIfAborted();
        this.publishConnection(this.session.snapshot());
        this.publish(
          latencyEvent({ evidence: null, request: null, state: "idle" }, this.nextSequence()),
        );
        const snapshot = await this.profiles.markActive(command.payload.profileId, lifetime);
        lifetime.throwIfAborted();
        this.publishProfiles(snapshot);
      };
      if (profile?.source !== undefined)
        await this.plugins.withProfileConnection(profile.source, connection.brokers, connect);
      else await connect();
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
    this.connectionIntent += 1;
    const lifetime = this.beginConnectionLifetime();
    this.invalidateClusterState();
    const connectionName = this.session.snapshot().connectionName ?? "No active connection";
    const operation = this.session.disconnect();
    this.publishConnection(this.session.snapshot());
    try {
      await operation;
      lifetime.throwIfAborted();
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
        activeStateChanged: !(error instanceof ConnectionAttemptSupersededError),
        connection: undefined,
        correlationId,
      });
      if (!(error instanceof ConnectionAttemptSupersededError))
        this.publishConnection(this.session.snapshot(), translated.error);
      this.recordFailureActivity(
        connectionName,
        "Disconnect",
        correlationId,
        translated.detail,
        [],
        error instanceof ConnectionAttemptSupersededError ? "cancelled" : "failed",
      );
      return failureResponse(command, translated.error);
    }
  }

  private beginConnectionLifetime(pluginId?: string): AbortSignal {
    this.connectionLifetime.abort(new ConnectionAttemptSupersededError());
    this.connectionLifetime = new AbortController();
    this.connectionPluginId = pluginId;
    return this.connectionLifetime.signal;
  }

  private assertConnectionIntent(intent: number): void {
    if (intent !== this.connectionIntent) throw new ConnectionAttemptSupersededError();
  }

  private async disconnectPluginConnection(pluginId: string): Promise<void> {
    if (
      this.connectionPluginId !== pluginId ||
      !["connecting", "connected"].includes(this.session.snapshot().state)
    )
      return;
    // Ownership is checked and disconnection begins in one synchronous turn.
    const response = await this.executeInternal({
      command: "connection.disconnect",
      id: crypto.randomUUID(),
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    if (!response.ok) throw new Error(response.error.summary);
  }

  private nextSequence(): number {
    this.sequence += 1;
    return this.sequence;
  }

  private invalidateLatency(): void {
    this.publish(invalidateLatencyEvent(this.latencyProbe, this.nextSequence()));
    this.publish(latencyHistoryEvent(this.latencyProbe.historySnapshot(), this.nextSequence()));
  }

  private invalidateClusterState(preserveConsumption = false): void {
    if (!preserveConsumption) this.consumption.invalidate();
    this.observations.cancel();
    this.relationships.cancel();
    this.environments.cancel();
    void this.recordReplay.invalidate().catch(() => undefined);
    this.correlationTrace.invalidate();
    this.recordCodecs.invalidate();
    this.schemaInspection.invalidate();
    this.schemaSamples.invalidate();
    this.clusterDiagnostics.clear();
    this.invalidateLatency();
    this.consumerGroups.invalidate();
    this.clusterServices.invalidate();
  }

  private publish(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  private publishConnection(snapshot: KafkaConnectionSnapshot, error?: HostError): void {
    this.publish(connectionStateEvent(snapshot, this.nextSequence(), error));
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

  private translateFailure(
    error: unknown,
    context: FailureContext,
  ): ReturnType<typeof translateFacadeFailure> {
    return translateFacadeFailure(error, context, this.available);
  }
}
