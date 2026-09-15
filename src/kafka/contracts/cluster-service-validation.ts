import {
  parseKafkaAclBinding,
  parseKafkaAclDeletionInput,
  parseKafkaAclSnapshot,
} from "./acl-validation";
import {
  parseSchemaCompatibilityCheckInput,
  parseSchemaCompatibilitySnapshot,
  parseSchemaDeletionInput,
  parseSchemaDetailSnapshot,
  parseSchemaIdentity,
  parseSchemaInventorySnapshot,
  parseSchemaRegistrationInput,
} from "./schema-registry-validation";
import {
  parseRedpandaTransformDeletionInput,
  parseRedpandaTransformDetailSnapshot,
  parseRedpandaTransformIdentity,
  parseRedpandaTransformInventorySnapshot,
  parseRedpandaTransformLogsSnapshot,
} from "./transform-validation";
import type { HostCommand, HostCommandName, HostError, HostEvent, HostEventName } from "./types";
import { exactKeys, record } from "./validation-primitives";

function emptyPayload(value: unknown, path: string): Readonly<Record<string, never>> {
  const payload = record(value, path);
  exactKeys(payload, [], path);
  return {};
}

export function parseClusterServiceHostCommand(
  command: HostCommandName,
  id: string,
  payload: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  switch (command) {
    case "schemas.list":
    case "acls.list":
    case "transforms.list":
      return { command, id, payload: emptyPayload(payload, "command.payload"), version };
    case "schemas.load":
      return {
        command,
        id,
        payload: parseSchemaIdentity(payload, "command.payload"),
        version,
      };
    case "schemas.compatibility.check":
      return {
        command,
        id,
        payload: parseSchemaCompatibilityCheckInput(payload, "command.payload"),
        version,
      };
    case "schemas.register":
      return {
        command,
        id,
        payload: parseSchemaRegistrationInput(payload, "command.payload"),
        version,
      };
    case "schemas.delete":
      return {
        command,
        id,
        payload: parseSchemaDeletionInput(payload, "command.payload"),
        version,
      };
    case "acls.create":
      return {
        command,
        id,
        payload: parseKafkaAclBinding(payload, "command.payload"),
        version,
      };
    case "acls.delete":
      return {
        command,
        id,
        payload: parseKafkaAclDeletionInput(payload, "command.payload"),
        version,
      };
    case "transforms.load":
    case "transforms.logs.load":
      return {
        command,
        id,
        payload: parseRedpandaTransformIdentity(payload, "command.payload"),
        version,
      };
    case "transforms.delete":
      return {
        command,
        id,
        payload: parseRedpandaTransformDeletionInput(payload, "command.payload"),
        version,
      };
    default:
      return undefined;
  }
}

export function parseClusterServiceHostEvent(
  event: HostEventName,
  payload: unknown,
  sequence: number,
  version: HostEvent["version"],
  parseError: (value: unknown, path: string) => HostError,
): HostEvent | undefined {
  switch (event) {
    case "schemas.changed":
      return {
        event,
        payload: parseSchemaInventorySnapshot(payload, "event.payload", parseError),
        sequence,
        version,
      };
    case "schema.changed":
      return {
        event,
        payload: parseSchemaDetailSnapshot(payload, "event.payload", parseError),
        sequence,
        version,
      };
    case "schemaCompatibility.changed":
      return {
        event,
        payload: parseSchemaCompatibilitySnapshot(payload, "event.payload"),
        sequence,
        version,
      };
    case "acls.changed":
      return {
        event,
        payload: parseKafkaAclSnapshot(payload, "event.payload", parseError),
        sequence,
        version,
      };
    case "transforms.changed":
      return {
        event,
        payload: parseRedpandaTransformInventorySnapshot(payload, "event.payload", parseError),
        sequence,
        version,
      };
    case "transform.changed":
      return {
        event,
        payload: parseRedpandaTransformDetailSnapshot(payload, "event.payload", parseError),
        sequence,
        version,
      };
    case "transformLogs.changed":
      return {
        event,
        payload: parseRedpandaTransformLogsSnapshot(payload, "event.payload", parseError),
        sequence,
        version,
      };
    default:
      return undefined;
  }
}
