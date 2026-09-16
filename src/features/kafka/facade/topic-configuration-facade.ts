import type {
  HostCommand,
  HostCommandResponse,
  HostError,
  HostEvent,
  KafkaTopicConfigurationOperationInput,
  KafkaTopicConfigurationSnapshot,
} from "../contracts";
import { HOST_PROTOCOL_VERSION } from "../contracts";
import {
  ConnectionAttemptSupersededError,
  type KafkaApplicationSession,
  type KafkaTopicConfigurationServicePort,
  type KafkaTopicConfigurationView,
} from "../application";

import {
  failureResponse,
  successResponse,
  type ActivityInput,
  translateFacadeFailure,
} from "./facade-support";

export type TopicConfigurationHostCommand = Extract<
  HostCommand,
  {
    readonly command:
      | "topicConfiguration.apply"
      | "topicConfiguration.history"
      | "topicConfiguration.load"
      | "topicConfiguration.validate";
  }
>;

export interface TopicConfigurationFacadeBindings {
  readonly nextSequence: () => number;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly service: KafkaTopicConfigurationServicePort;
  readonly session: Pick<KafkaApplicationSession, "activeConnectionContext">;
}

function readyConfiguration(view: KafkaTopicConfigurationView): KafkaTopicConfigurationSnapshot {
  return {
    connectionName: view.connectionName,
    entries: view.entries,
    refreshedAt: view.refreshedAt,
    state: "ready",
    topic: view.topic,
  };
}

function failureState(
  error: HostError,
): Extract<KafkaTopicConfigurationSnapshot["state"], "denied" | "failed" | "not-found"> {
  return error.code === "AUTHORIZATION_DENIED"
    ? "denied"
    : error.code === "TOPIC_NOT_FOUND"
      ? "not-found"
      : "failed";
}

function activityObject(connectionName: string, topic: string): string {
  return `${connectionName} · ${topic}`;
}

function operationName(command: TopicConfigurationHostCommand["command"]): string {
  switch (command) {
    case "topicConfiguration.apply":
      return "Apply topic configuration";
    case "topicConfiguration.history":
      return "Load topic configuration history";
    case "topicConfiguration.load":
      return "Load topic configuration";
    case "topicConfiguration.validate":
      return "Dry-run topic configuration";
  }
}

export async function executeTopicConfigurationCommand(
  command: TopicConfigurationHostCommand,
  correlationId: string,
  bindings: TopicConfigurationFacadeBindings,
): Promise<HostCommandResponse> {
  const operation = operationName(command.command);
  const context = bindings.session.activeConnectionContext();
  if (command.command === "topicConfiguration.load" && context !== null) {
    bindings.publish({
      event: "topicConfiguration.changed",
      payload: {
        connectionName: context.connectionName,
        entries: [],
        refreshedAt: null,
        state: "loading",
        topic: command.payload.topic,
      },
      sequence: bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }

  try {
    if (command.command === "topicConfiguration.history") {
      const result = await bindings.service.history(command.payload.topic);
      bindings.publish({
        event: "topicConfiguration.history",
        payload: result.snapshot,
        sequence: bindings.nextSequence(),
        version: HOST_PROTOCOL_VERSION,
      });
      const degraded =
        result.failure !== undefined || result.snapshot.store.state === "unavailable";
      bindings.recordActivity({
        correlationId,
        detail: degraded
          ? "Topic configuration history could not be recorded; Kafka administration remains available."
          : `Loaded ${String(result.snapshot.entries.length)} matching topic configuration history record${
              result.snapshot.entries.length === 1 ? "" : "s"
            }.`,
        object: activityObject(result.snapshot.connectionName, result.snapshot.topic),
        operation,
        outcome: "succeeded",
        severity: degraded ? "warning" : "info",
      });
      return successResponse(command, correlationId);
    }

    if (command.command === "topicConfiguration.load") {
      const result = await bindings.service.load(command.payload.topic);
      bindings.publish({
        event: "topicConfiguration.changed",
        payload: readyConfiguration(result),
        sequence: bindings.nextSequence(),
        version: HOST_PROTOCOL_VERSION,
      });
      bindings.recordActivity({
        correlationId,
        detail: `Loaded ${String(result.entries.length)} broker-confirmed topic configuration entr${
          result.entries.length === 1 ? "y" : "ies"
        }.`,
        object: activityObject(result.connectionName, result.topic),
        operation,
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    }

    const mutationPayload = command.payload as KafkaTopicConfigurationOperationInput;
    const result =
      command.command === "topicConfiguration.validate"
        ? await bindings.service.validate(mutationPayload)
        : await bindings.service.apply(mutationPayload);
    const refreshError =
      result.refreshFailure === undefined
        ? undefined
        : translateFacadeFailure(
            result.refreshFailure,
            {
              activeStateChanged: false,
              connection: undefined,
              correlationId,
            },
            true,
          ).error;
    bindings.publish({
      event: "topicConfiguration.changed",
      payload:
        refreshError === undefined
          ? readyConfiguration(result.configuration)
          : {
              ...readyConfiguration(result.configuration),
              error: refreshError,
              state: "stale",
            },
      sequence: bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
    bindings.publish({
      event: "topicConfiguration.history",
      payload: result.history,
      sequence: bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
    const historyDegraded =
      result.historyFailure !== undefined || result.history.store.state === "unavailable";
    const degraded = refreshError !== undefined || historyDegraded;
    bindings.recordActivity({
      correlationId,
      detail:
        refreshError !== undefined
          ? "Kafka confirmed the named changes, but refreshed configuration is unavailable."
          : historyDegraded
            ? `${operation} completed, but topic configuration history could not be recorded.`
            : `${operation} completed for ${String(mutationPayload.changes.length)} named configuration${
                mutationPayload.changes.length === 1 ? "" : "s"
              }.`,
      object: activityObject(result.configuration.connectionName, result.configuration.topic),
      operation,
      outcome: "succeeded",
      severity: degraded ? "warning" : "info",
    });
    return successResponse(command, correlationId);
  } catch (error) {
    const translated = translateFacadeFailure(
      error,
      {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      },
      true,
    );
    const cancelled =
      error instanceof ConnectionAttemptSupersededError ||
      (error instanceof Error && error.name === "AbortError");
    if (!cancelled && context !== null) {
      bindings.publish({
        event: "topicConfiguration.changed",
        payload: {
          connectionName: context.connectionName,
          entries: [],
          error: translated.error,
          refreshedAt: null,
          state: failureState(translated.error),
          topic: command.payload.topic,
        },
        sequence: bindings.nextSequence(),
        version: HOST_PROTOCOL_VERSION,
      });
    }
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object: activityObject(
        context?.connectionName ?? "No active connection",
        command.payload.topic,
      ),
      operation,
      outcome: cancelled ? "cancelled" : "failed",
      severity: cancelled ? "warning" : "error",
    });
    return failureResponse(command, translated.error);
  }
}
