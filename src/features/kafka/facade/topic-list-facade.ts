import type { HostCommand, HostCommandResponse, HostError, HostEvent } from "../contracts";
import { ConnectionAttemptSupersededError, type KafkaApplicationSession } from "../application";

import { failureResponse, successResponse, type ActivityInput } from "./facade-support";

interface TopicListFacadeOptions {
  readonly now: () => Date;
  readonly publishTopics: (
    payload: Extract<HostEvent, { readonly event: "topics.changed" }>["payload"],
  ) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly recordFailureActivity: (
    object: string,
    operation: string,
    correlationId: string,
    detail: string,
    secrets: readonly string[],
    outcome: "cancelled" | "failed",
  ) => void;
  readonly session: KafkaApplicationSession;
  readonly translateFailure: (
    error: unknown,
    correlationId: string,
  ) => { readonly detail: string; readonly error: HostError };
}

export async function executeTopicListCommand(
  command: Extract<HostCommand, { readonly command: "topics.list" }>,
  correlationId: string,
  options: TopicListFacadeOptions,
): Promise<HostCommandResponse> {
  const connectionName = options.session.snapshot().connectionName ?? "No active connection";
  options.publishTopics({ refreshedAt: null, state: "loading", topics: [] });
  try {
    const topics = await options.session.listTopics();
    options.publishTopics({
      refreshedAt: options.now().toISOString(),
      state: "ready",
      topics,
    });
    options.recordActivity({
      correlationId,
      detail: `Loaded ${topics.length} authorized Kafka topic${topics.length === 1 ? "" : "s"}.`,
      object: connectionName,
      operation: "Refresh topics",
      outcome: "succeeded",
      severity: "info",
    });
    return successResponse(command, correlationId);
  } catch (error) {
    const translated = options.translateFailure(error, correlationId);
    if (!(error instanceof ConnectionAttemptSupersededError)) {
      options.publishTopics({
        error: translated.error,
        refreshedAt: null,
        state: translated.error.code === "AUTHORIZATION_DENIED" ? "denied" : "failed",
        topics: [],
      });
    }
    options.recordFailureActivity(
      connectionName,
      "Refresh topics",
      correlationId,
      translated.detail,
      [],
      error instanceof ConnectionAttemptSupersededError ? "cancelled" : "failed",
    );
    return failureResponse(command, translated.error);
  }
}
