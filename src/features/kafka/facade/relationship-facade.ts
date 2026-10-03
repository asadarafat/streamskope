import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import type { RelationshipCommand } from "../contracts/relationship-protocol";
import type { KafkaApplicationSession, SchemaRegistryPort } from "../application";
import type { ConnectPort } from "../application/connect-service";
import { RelationshipService } from "../application/relationship-service";

import { failureResponse } from "./facade-support";
export class RelationshipFacade {
  private readonly service: RelationshipService;
  constructor(
    session: KafkaApplicationSession,
    connect?: ConnectPort,
    registry?: SchemaRegistryPort,
  ) {
    this.service = new RelationshipService(() => session.writeContext(), connect, registry);
  }
  cancel(): void {
    this.service.cancel();
  }
  idle(): Promise<void> {
    return this.service.idle();
  }
  async execute(command: RelationshipCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      const base = { id: command.id, version: HOST_PROTOCOL_VERSION, ok: true as const };
      if (command.command === "relationships.cancel") {
        this.cancel();
        return { ...base, command: command.command, result: { correlationId } };
      }
      const graph = await this.service.capture(command.payload);
      return { ...base, command: command.command, result: { correlationId, graph } };
    } catch {
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "kafka",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: "Relationship discovery could not complete.",
        recovery:
          "Choose one to three visible topics and optionally an exact subject/version. Check Kafka, Connect and Registry access. Discovery has a 30-second deadline; cancelled or changed connections discard results.",
      });
    }
  }
}
