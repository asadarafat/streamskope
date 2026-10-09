import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import type { KafkaQueryLibrary } from "../application";

import { failureResponse, translateFacadeFailure } from "./facade-support";

export async function executeQueryCommand(
  command: Extract<
    HostCommand,
    { readonly command: "queries.list" | "queries.put" | "queries.delete" }
  >,
  correlationId: string,
  library: KafkaQueryLibrary,
): Promise<HostCommandResponse> {
  try {
    const snapshot =
      command.command === "queries.put"
        ? await library.put(command.payload.query, command.payload.expected)
        : command.command === "queries.delete"
          ? await library.delete(command.payload.id)
          : await library.list();
    return {
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId, snapshot },
    };
  } catch (error) {
    return failureResponse(
      command,
      translateFacadeFailure(
        error,
        {
          activeStateChanged: false,
          connection: undefined,
          correlationId,
        },
        true,
      ).error,
    );
  }
}
