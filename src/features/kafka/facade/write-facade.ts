import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import type { KafkaReviewedWriteService } from "../application/reviewed-write-service";

import { failureResponse, type ActivityInput } from "./facade-support";

export async function executeWriteCommand(
  command: Extract<HostCommand, { command: "writes.review" | "writes.apply" }>,
  correlationId: string,
  service: KafkaReviewedWriteService,
  recordActivity: (input: ActivityInput) => void,
): Promise<HostCommandResponse> {
  try {
    if (command.command === "writes.review")
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, review: await service.review(command.payload) },
      };
    const outcome = await service.apply(command.payload.planId);
    recordActivity({
      correlationId,
      operation: "Apply reviewed Kafka write",
      object: "Reviewed destination",
      outcome: outcome.state === "acknowledged" ? "succeeded" : "failed",
      severity: outcome.state === "acknowledged" ? "info" : "warning",
      detail: outcome.detail,
    });
    return {
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId, outcome },
    };
  } catch {
    return failureResponse(command, {
      code: "VALIDATION",
      stage: "kafka",
      correlationId,
      retryable: false,
      activeStateChanged: false,
      summary: "The write review could not be accepted.",
      recovery:
        "Check the topic, partition or replica count and permissions. Reconnect and review again if the connection or review expired. Never repeat an uncertain write without inspecting the destination.",
    });
  }
}
