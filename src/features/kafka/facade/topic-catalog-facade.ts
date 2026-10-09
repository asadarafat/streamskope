import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import type { TopicCatalogCommand } from "../contracts/topic-catalog-protocol";
import type { KafkaQueryLibrary, KafkaApplicationSession } from "../application";
import { TopicCatalogService } from "../application/topic-catalog-service";

import { failureResponse, translateFacadeFailure } from "./facade-support";

export async function executeTopicCatalogCommand(
  command: TopicCatalogCommand,
  correlationId: string,
  library: KafkaQueryLibrary,
  session: KafkaApplicationSession,
): Promise<HostCommandResponse> {
  try {
    const base = { id: command.id, version: HOST_PROTOCOL_VERSION, ok: true as const };
    if (command.command === "catalog.list")
      return {
        ...base,
        command: command.command,
        result: { correlationId, snapshot: await library.listTopics() },
      };
    const service = new TopicCatalogService(library, () => session.topicCatalogScope());
    const snapshot =
      command.command === "catalog.load"
        ? await service.load(command.payload.topic)
        : command.command === "catalog.put"
          ? await service.put(command.payload.annotation, command.payload.expected)
          : await library.deleteTopic(command.payload.identity, command.payload.expected);
    return { ...base, command: command.command, result: { correlationId, snapshot } };
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
