import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import type { ConnectHostCommand } from "../contracts/connect";
import { ConnectService, type ConnectPort } from "../application/connect-service";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, type ActivityInput } from "./facade-support";
export class ConnectFacade {
  private readonly service: ConnectService | undefined;
  constructor(
    session: KafkaApplicationSession,
    port: ConnectPort | undefined,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    if (port) this.service = new ConnectService(() => session.writeContext(), port);
  }
  async execute(command: ConnectHostCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      if (!this.service) throw new Error("Connect unavailable.");
      const base = { id: command.id, version: HOST_PROTOCOL_VERSION, ok: true as const };
      switch (command.command) {
        case "connect.list":
          return {
            ...base,
            command: command.command,
            result: { correlationId, inventory: await this.service.list() },
          };
        case "connect.load":
          return {
            ...base,
            command: command.command,
            result: { correlationId, detail: await this.service.load(command.payload.name) },
          };
        case "connect.validate":
          return {
            ...base,
            command: command.command,
            result: { correlationId, validation: await this.service.validate(command.payload) },
          };
        case "connect.review":
          return {
            ...base,
            command: command.command,
            result: { correlationId, review: await this.service.review(command.payload) },
          };
        case "connect.apply": {
          const outcome = await this.service.apply(
            command.payload.planId,
            command.payload.confirmation,
          );
          this.activity({
            correlationId,
            operation: "Apply reviewed Connect action",
            object: command.payload.confirmation,
            detail: outcome.detail,
            outcome: outcome.state === "acknowledged" ? "succeeded" : "failed",
            severity: outcome.state === "acknowledged" ? "info" : "warning",
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
        summary: "Connect request could not be completed.",
        recovery:
          "Configure the Connect endpoint and its OAuth/TLS trust in this profile. Check API permissions, refresh connector state, validate configuration and review again. No credentials or worker traces are included here.",
      });
    }
  }
}
