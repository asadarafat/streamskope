import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { ClientQuotaService } from "../application/client-quota-service";
import { clientQuotaLabel } from "../contracts/client-quotas";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, type ActivityInput } from "./facade-support";

export class ClientQuotaFacade {
  private readonly service: ClientQuotaService;
  constructor(
    session: KafkaApplicationSession,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    this.service = new ClientQuotaService(() => session.administrationScopes.clientQuotas());
  }
  async execute(
    command: Extract<
      HostCommand,
      { command: "quotas.inspect" | "quotas.change.review" | "quotas.change.apply" }
    >,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    try {
      if (command.command === "quotas.inspect")
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId, snapshot: await this.service.inspect(command.payload.entity) },
        };
      if (command.command === "quotas.change.review")
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
        operation: "Apply reviewed client quota change",
        object: clientQuotaLabel(outcome.input.entity),
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
        summary: "The exact quota inspection or review could not be accepted.",
        recovery:
          "Check DescribeClientQuotas/AlterClientQuotas support and cluster DESCRIBE_CONFIGS/ALTER_CONFIGS permission. Select exact named/default entities and finite positive values (safe integers for byte rates) or explicit removal. Changed quotas, expired reviews and changed connections are refused; inspect an uncertain outcome before another attempt.",
      });
    }
  }
}
