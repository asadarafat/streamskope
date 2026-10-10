import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import type { ConnectHostCommand } from "../contracts/connect";
import type { ConnectOffsetsCommand } from "../contracts/connect-offsets";
import { ConnectService, type ConnectPort } from "../application/connect-service";
import { ConnectOffsetsService } from "../application/connect-offset-service";
import { ConnectWriteAdmission } from "../application/connect-write-admission";
import type { ConnectOffsetsPort } from "../application/connect-offset-types";
import type { KafkaApplicationSession } from "../application";
import { serviceConnectionDiagnostic } from "../application/connection-diagnostics";

import { failureResponse, type ActivityInput } from "./facade-support";
export class ConnectFacade {
  private readonly service: ConnectService | undefined;
  private readonly offsets: ConnectOffsetsService | undefined;
  constructor(
    session: KafkaApplicationSession,
    port: ConnectPort | undefined,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    if (port) {
      const admission = new ConnectWriteAdmission(),
        scope = (): ReturnType<KafkaApplicationSession["administrationScopes"]["connect"]> =>
          session.administrationScopes.connect();
      this.service = new ConnectService(scope, port, Date.now, admission);
      if (
        "inspectOffsets" in port &&
        typeof port.inspectOffsets === "function" &&
        "applyOffsets" in port &&
        typeof port.applyOffsets === "function"
      )
        this.offsets = new ConnectOffsetsService(
          scope,
          port as ConnectPort & ConnectOffsetsPort,
          Date.now,
          admission,
        );
    }
  }
  async execute(
    command: ConnectHostCommand | ConnectOffsetsCommand,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    try {
      if (!this.service) throw new Error("Connect unavailable.");
      const base = { id: command.id, version: HOST_PROTOCOL_VERSION, ok: true as const };
      switch (command.command) {
        case "connect.offsets.inspect":
          if (!this.offsets) throw new Error("Offset API unavailable.");
          return {
            ...base,
            command: command.command,
            result: { correlationId, snapshot: await this.offsets.inspect(command.payload.name) },
          };
        case "connect.offsets.review":
          if (!this.offsets) throw new Error("Offset API unavailable.");
          return {
            ...base,
            command: command.command,
            result: { correlationId, review: await this.offsets.review(command.payload) },
          };
        case "connect.offsets.apply": {
          if (!this.offsets) throw new Error("Offset API unavailable.");
          const outcome = await this.offsets.apply(
            command.payload.planId,
            command.payload.confirmation,
          );
          this.activity({
            correlationId,
            operation: "Apply reviewed connector offsets",
            object: command.payload.confirmation,
            detail: outcome.detail,
            outcome: outcome.state === "acknowledged" ? "succeeded" : "failed",
            severity:
              outcome.state === "acknowledged" &&
              outcome.verification === "verified" &&
              outcome.cleanup === "confirmed"
                ? "info"
                : "warning",
          });
          return { ...base, command: command.command, result: { correlationId, outcome } };
        }
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
            severity:
              outcome.state === "acknowledged" &&
              outcome.cleanup === "confirmed" &&
              outcome.verification === "verified"
                ? "info"
                : "warning",
          });
          return { ...base, command: command.command, result: { correlationId, outcome } };
        }
      }
    } catch (error) {
      const diagnostic = serviceConnectionDiagnostic(error, "Kafka Connect");
      if (diagnostic !== undefined)
        return failureResponse(command, {
          ...diagnostic,
          correlationId,
          activeStateChanged: false,
        });
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "authorization",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: command.command.startsWith("connect.offsets.")
          ? "Connector offsets could not be verified."
          : "Connect request could not be completed.",
        recovery: command.command.startsWith("connect.offsets.")
          ? "Verify the profile's Connect endpoint and matching Kafka cluster, API permissions and supported connector mapping. Stop the connector, inspect offsets and review again. Preserve an uncertain result; no automatic retry is sent."
          : "Configure the Connect endpoint and its authentication and TLS trust in this profile. Check API permissions, refresh connector state, validate configuration and review again. No credentials or worker traces are included here.",
      });
    }
  }
}
