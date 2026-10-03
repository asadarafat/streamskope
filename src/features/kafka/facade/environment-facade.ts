import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import type { EnvironmentHostCommand } from "../contracts/environment-protocol";
import { EnvironmentService } from "../application/environment-service";
import type { KafkaApplicationSession } from "../application";
import type { ReplayDestinationPort } from "../application/replay-destination";

import { failureResponse, type ActivityInput } from "./facade-support";
export class EnvironmentFacade {
  private readonly service: EnvironmentService;
  constructor(
    session: KafkaApplicationSession,
    destinations: ReplayDestinationPort | undefined,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    this.service = new EnvironmentService(() => session.writeContext(), destinations);
  }
  async execute(
    command: EnvironmentHostCommand,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    try {
      const base = { id: command.id, version: HOST_PROTOCOL_VERSION, ok: true as const };
      switch (command.command) {
        case "environments.capture":
          return {
            ...base,
            command: command.command,
            result: {
              correlationId,
              snapshot: await this.service.capture(command.payload.topics, command.payload.profile),
            },
          };
        case "environments.review":
          return {
            ...base,
            command: command.command,
            result: { correlationId, review: await this.service.review(command.payload) },
          };
        case "environments.apply": {
          const outcome = await this.service.apply(
            command.payload.planId,
            command.payload.confirmation,
          );
          const success = outcome.results.every((r) => r.state === "acknowledged" && r.verified);
          this.activity({
            correlationId,
            operation: "Promote reviewed topic settings",
            object: command.payload.confirmation,
            detail: outcome.detail,
            outcome: success ? "succeeded" : "failed",
            severity: success ? "info" : "warning",
          });
          return { ...base, command: command.command, result: { correlationId, outcome } };
        }
      }
    } catch {
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "authorization",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: "Environment comparison could not be accepted.",
        recovery:
          "Select existing topics, a configured destination and supported mutable settings. Refresh the target snapshot after any change; expired plans or changed profiles require a new review.",
      });
    }
  }
}
