import type { HostCommand, HostCommandBase, HostCommandResponse } from "./types";
import { emptyRecord, exactKeys, text } from "./validation-primitives";
import {
  parseRelationshipGraph,
  parseRelationshipInput,
  type RelationshipInput,
} from "./relationships";
export type RelationshipCommand =
  | (HostCommandBase & {
      readonly command: "relationships.capture";
      readonly payload: RelationshipInput;
    })
  | (HostCommandBase & {
      readonly command: "relationships.cancel";
      readonly payload: Readonly<Record<string, never>>;
    });
export function parseRelationshipCommand(
  command: HostCommand["command"],
  id: string,
  payload: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "relationships.capture")
    return { command, id, version, payload: parseRelationshipInput(payload) };
  if (command === "relationships.cancel")
    return { command, id, version, payload: emptyRecord(payload, "payload") };
  return undefined;
}
export function parseRelationshipResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command !== "relationships.capture") return undefined;
  exactKeys(result, ["correlationId", "graph"], "result");
  return {
    command,
    id,
    version,
    ok: true,
    result: {
      correlationId: text(result.correlationId, "correlationId", 128),
      graph: parseRelationshipGraph(result.graph),
    },
  };
}
