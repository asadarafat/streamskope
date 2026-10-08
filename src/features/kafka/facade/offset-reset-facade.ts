import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { OffsetResetService } from "../application/offset-reset-service";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, type ActivityInput } from "./facade-support";

export class OffsetResetFacade {
  private readonly service: OffsetResetService;
  constructor(
    session: KafkaApplicationSession,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    this.service = new OffsetResetService(() => session.offsetResetScope());
  }
  async execute(
    command: Extract<
      HostCommand,
      { command: "consumerGroups.reset.review" | "consumerGroups.reset.apply" }
    >,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    try {
      if (command.command === "consumerGroups.reset.review")
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, review: await this.service.review(command.payload) },
        };
      const outcome = await this.service.apply(
        command.payload.planId,
        command.payload.confirmation,
      );
      const complete = outcome.partitions.every((p) => p.state === "acknowledged" && p.verified);
      this.activity({
        correlationId,
        operation: "Apply reviewed offset reset",
        object: outcome.groupId,
        outcome: complete ? "succeeded" : "failed",
        severity: complete ? "info" : "warning",
        detail: `${outcome.detail} ${outcome.partitions.map((p) => `${p.topic}/${p.partition}: ${p.state}, requested ${p.offset}, observed ${p.observed ?? "unknown"}`).join("; ")}`,
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
        summary: "Offset reset review could not be accepted.",
        recovery:
          "Check group/topic permissions and partition positions, stop consumers, then preview again. Expired reviews or a changed connection require another preview. Never retry an uncertain reset without inspecting committed offsets.",
      });
    }
  }
}
