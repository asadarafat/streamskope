import type { HostCommand, HostCommandResponse } from "../contracts";
import type { KafkaApplicationSession, KafkaTrustAcquisitionResolver } from "../application";

import {
  failureResponse,
  sensitiveValues,
  successResponse,
  translateFacadeFailure,
  type ActivityInput,
} from "./facade-support";
import { resolveHostConnection } from "./host-connection";

type ConnectionTestCommand = Extract<HostCommand, { readonly command: "connection.test" }>;

export interface ConnectionTestFacadeBindings {
  readonly acquisitions: KafkaTrustAcquisitionResolver | undefined;
  readonly available: boolean;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly session: Pick<KafkaApplicationSession, "testConnection">;
}

export async function executeConnectionTestCommand(
  command: ConnectionTestCommand,
  correlationId: string,
  bindings: ConnectionTestFacadeBindings,
): Promise<HostCommandResponse> {
  let connection;
  try {
    const resolved = resolveHostConnection(command.payload, bindings.acquisitions);
    connection = resolved.connection;
    resolved.lifetimeSignal?.throwIfAborted();
    const result = await bindings.session.testConnection(connection, resolved.lifetimeSignal);
    resolved.lifetimeSignal?.throwIfAborted();
    bindings.recordActivity({
      correlationId,
      detail: `Confirmed checks: ${result.checks.join(", ")}. Visible topics: ${result.topicCount}.`,
      object: command.payload.name,
      operation: "Connection test",
      outcome: "succeeded",
      severity: "info",
    });
    return successResponse(command, correlationId);
  } catch (error) {
    const translated = translateFacadeFailure(
      error,
      {
        activeStateChanged: false,
        connection,
        correlationId,
      },
      bindings.available,
    );
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object: command.payload.name,
      operation: "Connection test",
      outcome: "failed",
      sensitiveValues: sensitiveValues(connection ?? command.payload),
      severity: "error",
    });
    return failureResponse(command, translated.error);
  }
}
