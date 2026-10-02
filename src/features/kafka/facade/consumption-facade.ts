import {
  HOST_PROTOCOL_VERSION,
  type ConsumptionState,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostEvent,
  type KafkaMessage,
} from "../contracts";
import {
  ConnectionAttemptSupersededError,
  type KafkaApplicationSession,
  type KafkaLiveRuleRuntime,
  type KafkaOperationalPreferenceService,
} from "../application";

import {
  defaultScheduleMessageFlush,
  failureResponse,
  fetchDescription,
  successResponse,
  type ActiveFacadeConsumption,
  type ActivityInput,
  type FailureContext,
  type translateFacadeFailure,
} from "./facade-support";
import {
  exploredKafkaMessage,
  internalLiveRuleFailureActivity,
  unavailableLiveRuleActivity,
} from "./live-rule-support";
import { flushFacadeMessages } from "./message-flush";
import { appendFacadeMessage } from "./message-queue";
import {
  createStreamMonitoring,
  emitConsumptionState,
  emitStreamMetrics,
  recordStreamQueueBounds,
  recordStreamQueueStart,
} from "./stream-monitor-facade";

interface ConsumptionFacadeBindings {
  readonly session: KafkaApplicationSession;
  readonly liveRules: KafkaLiveRuleRuntime;
  readonly preferences: KafkaOperationalPreferenceService;
  readonly monotonicNow: () => number;
  readonly now: () => Date;
  readonly nextSequence: () => number;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly recordFailureActivity: (
    object: string,
    operation: string,
    correlationId: string,
    detail: string,
    secrets?: readonly string[],
    outcome?: "cancelled" | "failed",
  ) => void;
  readonly translateFailure: (
    error: unknown,
    context: FailureContext,
  ) => ReturnType<typeof translateFacadeFailure>;
  readonly scheduleMessageFlush?: (flush: () => void, delayMs: number) => (() => void) | void;
}

// Owns consumption presentation, queue flushing and terminal notifications.
export class ConsumptionFacadeController {
  private activeConsumption: ActiveFacadeConsumption | undefined;
  private messagePresentationPaused = false;
  private readonly scheduleMessageFlush;

  constructor(private readonly bindings: ConsumptionFacadeBindings) {
    this.scheduleMessageFlush = bindings.scheduleMessageFlush ?? defaultScheduleMessageFlush;
  }

  refreshRuleCapability(): void {
    if (this.activeConsumption !== undefined) {
      this.publishConsumption(this.activeConsumption, this.activeConsumption.state);
    }
  }

  prepareShutdown(): () => void {
    const consumption = this.activeConsumption;
    return (): void => {
      if (consumption !== undefined && this.activeConsumption === consumption) {
        this.flushMessages(consumption, true);
        this.activeConsumption = undefined;
        this.bindings.liveRules.deactivate();
        this.publishConsumption(consumption, "stopped");
      } else {
        this.bindings.liveRules.deactivate();
      }
    };
  }

  private enqueueMessage(consumption: ActiveFacadeConsumption, message: KafkaMessage): void {
    if (this.activeConsumption !== consumption || message.topic !== consumption.request.topic) {
      return;
    }
    consumption.receivedMessages += 1;
    const exploredMessage = exploredKafkaMessage(this.bindings.liveRules, message);
    const { ruleEvaluation } = exploredMessage;
    if (
      ruleEvaluation.state === "unavailable" &&
      ruleEvaluation.reason === "internal" &&
      !consumption.ruleFailureRecorded
    ) {
      consumption.ruleFailureRecorded = true;
      this.bindings.recordActivity(internalLiveRuleFailureActivity(consumption));
    }
    recordStreamQueueStart(consumption, this.bindings.monotonicNow);
    appendFacadeMessage(consumption, {
      message: exploredMessage,
      ruleOutput: { ...this.bindings.preferences.currentSnapshot().preferences.rules },
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
      monotonicNow: this.bindings.monotonicNow,
      nextSequence: this.bindings.nextSequence,
      publish: this.bindings.publish,
      recordActivity: this.bindings.recordActivity,
    });
    this.publishStreamMetrics(consumption, consumption.state, droppedSincePrevious);
    if (!drainAll && consumption.messages.length > 0) {
      consumption.streamMonitoring.queueStartedAtMs = completedAtMs;
      this.scheduleConsumptionFlush(consumption);
    }
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
        connectionName: this.bindings.session.snapshot().connectionName,
        nextSequence: this.bindings.nextSequence,
        publish: this.bindings.publish,
        sampledAt: this.bindings.now().toISOString(),
      },
      droppedSincePrevious,
    );
  }

  private publishConsumption(
    consumption: ActiveFacadeConsumption,
    state: ConsumptionState,
    error?: HostError,
  ): void {
    emitConsumptionState(consumption, state, error, this.bindings.liveRules.capability(), {
      connectionName: this.bindings.session.snapshot().connectionName,
      nextSequence: this.bindings.nextSequence,
      publish: this.bindings.publish,
      sampledAt: this.bindings.now().toISOString(),
    });
  }

  async startMessages(
    command: Extract<HostCommand, { readonly command: "messages.start" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    const preferenceSnapshot = await this.bindings.preferences.get();
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
      streamMonitoring: createStreamMonitoring(this.bindings.monotonicNow()),
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
      await this.bindings.session.stopConsumption();
      if (this.activeConsumption !== consumption) {
        throw new ConnectionAttemptSupersededError();
      }
      const capability = await this.bindings.liveRules.prepare(command.payload.topic);
      if (this.activeConsumption !== consumption) {
        throw new ConnectionAttemptSupersededError();
      }
      this.publishConsumption(consumption, "loading");
      if (capability.state === "unavailable") {
        this.bindings.recordActivity(unavailableLiveRuleActivity(consumption, capability));
      }
      await this.bindings.session.startConsumption(command.payload, {
        onCoverage: (coverage): void => {
          if (this.activeConsumption === consumption) {
            consumption.coverage = coverage;
            if (consumption.state !== "loading")
              this.publishConsumption(consumption, consumption.state);
          }
        },
        onComplete: (): void => {
          runTerminal(() => {
            if (this.activeConsumption !== consumption) {
              return;
            }
            this.flushMessages(consumption, true);
            this.activeConsumption = undefined;
            this.bindings.liveRules.deactivate();
            const state = consumption.receivedMessages === 0 ? "empty" : "complete";
            this.publishConsumption(consumption, state);
            this.bindings.recordActivity({
              correlationId: consumption.correlationId,
              detail: `${fetchDescription(consumption.request)} ended with ${String(
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
            this.bindings.liveRules.deactivate();
            const translated = this.bindings.translateFailure(error, {
              activeStateChanged: false,
              connection: undefined,
              correlationId: consumption.correlationId,
            });
            this.publishConsumption(consumption, "failed", translated.error);
            this.bindings.recordFailureActivity(
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
      this.bindings.recordActivity({
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
      const translated = this.bindings.translateFailure(operationError, {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      });
      if (this.activeConsumption === consumption) {
        this.cancelConsumptionFlush(consumption);
        consumption.messages.length = 0;
        consumption.queuedBytes = 0;
        this.activeConsumption = undefined;
        this.bindings.liveRules.deactivate();
        this.publishConsumption(consumption, "failed", translated.error);
      }
      this.bindings.recordFailureActivity(
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

  async stopMessages(
    command: Extract<HostCommand, { readonly command: "messages.stop" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    const consumption = this.activeConsumption;
    try {
      await this.bindings.session.stopConsumption();
      if (consumption !== undefined && this.activeConsumption === consumption) {
        this.flushMessages(consumption, true);
        this.activeConsumption = undefined;
        this.bindings.liveRules.deactivate();
        this.publishConsumption(consumption, "stopped");
      } else if (consumption === undefined) {
        this.bindings.liveRules.deactivate();
        this.bindings.publish({
          event: "consumption.state",
          payload: {
            droppedMessages: 0,
            receivedMessages: 0,
            request: null,
            ruleEvaluation: this.bindings.liveRules.capability(),
            state: "stopped",
          },
          sequence: this.bindings.nextSequence(),
          version: HOST_PROTOCOL_VERSION,
        });
      }
      this.bindings.recordActivity({
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
      const translated = this.bindings.translateFailure(error, {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      });
      if (consumption !== undefined && this.activeConsumption === consumption) {
        this.flushMessages(consumption, true);
        this.publishConsumption(consumption, "failed", translated.error);
      }
      this.bindings.recordFailureActivity(
        consumption?.request.topic ?? "No active topic",
        "Stop consumption",
        correlationId,
        translated.detail,
      );
      return failureResponse(command, translated.error);
    }
  }
}
