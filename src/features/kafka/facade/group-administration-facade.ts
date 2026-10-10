import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { GroupAdministrationService } from "../application/group-administration-service";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, type ActivityInput } from "./facade-support";

export class GroupAdministrationFacade {
  private readonly service: GroupAdministrationService;
  constructor(
    session: KafkaApplicationSession,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    this.service = new GroupAdministrationService(() =>
      session.administrationScopes.groupAdministration(),
    );
  }
  async execute(
    command: Extract<
      HostCommand,
      { command: "consumerGroups.delete.review" | "consumerGroups.delete.apply" }
    >,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    try {
      if (command.command === "consumerGroups.delete.review")
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
      this.activity({
        correlationId,
        operation: "Apply reviewed consumer group deletion",
        object: outcome.groupId,
        detail: outcome.detail,
        outcome: outcome.state === "acknowledged" ? "succeeded" : "failed",
        severity:
          outcome.state === "acknowledged" &&
          outcome.verification === "verified" &&
          outcome.cleanup === "confirmed"
            ? "info"
            : "warning",
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
        summary: "The consumer group deletion review could not be accepted.",
        recovery:
          "Stop all consumers. Check group DESCRIBE/DELETE permission and consumer coordination support, then review again. Changed offsets, expired reviews and changed connections are refused. Inspect an uncertain outcome before another attempt.",
      });
    }
  }
}
