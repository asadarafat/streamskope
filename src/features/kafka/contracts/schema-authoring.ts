import { parseSchemaInspectionInput, type SchemaInspectionInput } from "./schema-inspection";
import { SCHEMA_REGISTRY_TYPES, type SchemaRegistryType } from "./schema-registry-types";
import {
  parseKafkaOriginalRecord,
  kafkaOriginalRecordByteLength,
  type KafkaCompleteRecord,
} from "./record-bytes";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export const SCHEMA_AUTHORING_LIMITS = {
  payloadBytes: 16_384,
  issues: 16,
  pathCharacters: 512,
} as const;
export interface SchemaAuthoringInput extends SchemaInspectionInput {
  readonly schemaId: number;
  readonly messageType: string;
  readonly payload: string;
}
export interface SchemaAuthoringWriter extends SchemaInspectionInput {
  readonly id: number;
  readonly schemaType: SchemaRegistryType;
}
export const SCHEMA_AUTHORING_ISSUE_CODES = [
  "json",
  "precision",
  "schema",
  "unsupported",
  "limit",
] as const;
export interface SchemaAuthoringIssue {
  readonly path: string;
  readonly code: (typeof SCHEMA_AUTHORING_ISSUE_CODES)[number];
  readonly detail: string;
}
export type SchemaAuthoringResult =
  | {
      readonly state: "valid";
      readonly writer: SchemaAuthoringWriter;
      readonly messageType: string | null;
      readonly json: string;
      readonly encoding: string;
      readonly record: KafkaCompleteRecord;
    }
  | { readonly state: "invalid"; readonly issues: readonly SchemaAuthoringIssue[] };

export function parseSchemaAuthoringInput(value: unknown): SchemaAuthoringInput {
  const input = record(value, "authoring");
  exactKeys(input, ["subject", "version", "schemaId", "messageType", "payload"], "authoring");
  const payload = text(input.payload, "authoring.payload", SCHEMA_AUTHORING_LIMITS.payloadBytes);
  if (new TextEncoder().encode(payload).length > SCHEMA_AUTHORING_LIMITS.payloadBytes)
    throw new HostContractValidationError("authoring.payload", "exceeds 16 KiB UTF-8");
  return {
    ...parseSchemaInspectionInput({ subject: input.subject, version: input.version }),
    schemaId: positiveBoundedInteger(input.schemaId, "authoring.schemaId", 0x7fffffff),
    messageType: boundedText(input.messageType, "authoring.messageType", 512),
    payload,
  };
}
export function parseSchemaAuthoringResult(value: unknown): SchemaAuthoringResult {
  const input = record(value, "authoring");
  if (input.state === "invalid") {
    exactKeys(input, ["state", "issues"], "authoring");
    if (
      !Array.isArray(input.issues) ||
      input.issues.length < 1 ||
      input.issues.length > SCHEMA_AUTHORING_LIMITS.issues
    )
      throw new HostContractValidationError("authoring.issues", "requires one to 16 issues");
    return {
      state: "invalid",
      issues: input.issues.map((item: unknown) => {
        const issue = record(item, "issue");
        exactKeys(issue, ["path", "code", "detail"], "issue");
        return {
          path: boundedText(issue.path, "issue.path", SCHEMA_AUTHORING_LIMITS.pathCharacters),
          code: declaredValue(issue.code, SCHEMA_AUTHORING_ISSUE_CODES, "issue.code"),
          detail: text(issue.detail, "issue.detail", 256),
        };
      }),
    };
  }
  exactKeys(input, ["state", "writer", "messageType", "json", "encoding", "record"], "authoring");
  if (input.state !== "valid")
    throw new HostContractValidationError("authoring.state", "requires a validation outcome");
  const writer = record(input.writer, "writer");
  exactKeys(writer, ["subject", "version", "id", "schemaType"], "writer");
  const encoded = parseKafkaOriginalRecord(input.record, "authoring.record");
  if (
    encoded.state !== "complete" ||
    encoded.key !== null ||
    encoded.value === null ||
    encoded.headers.length !== 0 ||
    kafkaOriginalRecordByteLength(encoded) > SCHEMA_AUTHORING_LIMITS.payloadBytes
  )
    throw new HostContractValidationError(
      "authoring.record",
      "requires a bounded value-only record",
    );
  const json = text(input.json, "authoring.json", 65_536);
  JSON.parse(json);
  const schemaType = declaredValue(writer.schemaType, SCHEMA_REGISTRY_TYPES, "writer.schemaType");
  const id = positiveBoundedInteger(writer.id, "writer.id", 0x7fffffff);
  const messageType =
    input.messageType === null ? null : text(input.messageType, "authoring.messageType", 512);
  if ((schemaType === "PROTOBUF") !== (messageType !== null))
    throw new HostContractValidationError("authoring.messageType", "must match the writer format");
  const wire = Uint8Array.from(atob(encoded.value), (character) => character.charCodeAt(0));
  if (schemaType === "JSON") {
    if (new TextDecoder("utf-8", { fatal: true }).decode(wire) !== json)
      throw new HostContractValidationError("authoring.record", "must encode the JSON projection");
  } else if (
    wire.length < (schemaType === "PROTOBUF" ? 6 : 5) ||
    wire[0] !== 0 ||
    new DataView(wire.buffer).getUint32(1) !== id
  ) {
    throw new HostContractValidationError(
      "authoring.record",
      "must contain the declared writer header",
    );
  }
  return {
    state: "valid",
    writer: {
      ...parseSchemaInspectionInput({ subject: writer.subject, version: writer.version }),
      id,
      schemaType,
    },
    messageType,
    json,
    encoding: text(input.encoding, "authoring.encoding", 512),
    record: encoded,
  };
}
