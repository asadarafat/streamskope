import { RECORD_FORMATS, type RecordFormat } from "./record-codec";
import type { SchemaRegistryType } from "./schema-registry-types";
import { SCHEMA_REGISTRY_TYPES } from "./schema-registry-types";
import type { KafkaCompleteRecord } from "./record-bytes";
import { HostContractValidationError } from "./validation-error";
import {
  record,
  exactKeys,
  text,
  boundedText,
  declaredValue,
  positiveBoundedInteger,
} from "./validation-primitives";

export const STRUCTURED_REPLAY_LIMITS = {
  patches: 16,
  mappings: 50,
  path: 512,
  patchBytes: 16_384,
} as const;
export type ReplayJsonPatch =
  | { readonly op: "set"; readonly path: string; readonly json: string }
  | { readonly op: "remove"; readonly path: string };
export interface ReplayWriterSelection {
  readonly subject: string;
  readonly version: number;
  readonly messageType: string;
}
export interface ReplayWriterMapping {
  readonly format: RecordFormat;
  readonly sourceId: number | null;
  readonly target: ReplayWriterSelection | null;
}
export interface StructuredReplayField {
  readonly codec: "auto" | RecordFormat;
  readonly patches: readonly ReplayJsonPatch[];
  readonly mappings: readonly ReplayWriterMapping[];
}
export interface StructuredReplayTransform {
  readonly key: StructuredReplayField | null;
  readonly value: StructuredReplayField | null;
}
export interface ReplayFieldEncoding {
  readonly source: { readonly format: RecordFormat; readonly id: number | null };
  readonly target:
    | (ReplayWriterSelection & {
        readonly id: number;
        readonly schemaType: SchemaRegistryType;
        readonly fingerprint: string;
      })
    | null;
}
export interface ReplayRecordEncoding {
  readonly key: ReplayFieldEncoding | null;
  readonly value: ReplayFieldEncoding | null;
}

export function replayPatchSegments(path: string): readonly string[] {
  if (path === "") return [];
  if (
    !path.startsWith("/") ||
    path.length > STRUCTURED_REPLAY_LIMITS.path ||
    /~(?:[^01]|$)/u.test(path)
  )
    throw new HostContractValidationError("patch.path", "use a bounded JSON Pointer");
  const segments = path
    .slice(1)
    .split("/")
    .map((p) => p.replaceAll("~1", "/").replaceAll("~0", "~"));
  if (
    segments.length > 16 ||
    segments.some((p) => ["__proto__", "prototype", "constructor"].includes(p))
  )
    throw new HostContractValidationError("patch.path", "unsafe or excessive path");
  return segments;
}
export function parseReplayPatchJson(source: string): unknown {
  if (new TextEncoder().encode(source).length > STRUCTURED_REPLAY_LIMITS.patchBytes)
    throw new HostContractValidationError("patch.json", "exceeds 16 KiB");
  const value: unknown = JSON.parse(source);
  const pending = [{ value, depth: 0 }];
  let nodes = 0;
  while (pending.length) {
    const item = pending.pop()!;
    if (++nodes > 2048 || item.depth > 16)
      throw new HostContractValidationError("patch.json", "excessive structure");
    if (
      typeof item.value === "number" &&
      (!Number.isFinite(item.value) ||
        (Number.isInteger(item.value) && !Number.isSafeInteger(item.value)))
    )
      throw new HostContractValidationError(
        "patch.json",
        "use decimal strings for exact large integers",
      );
    if (item.value !== null && typeof item.value === "object") {
      for (const [name, child] of Object.entries(item.value)) {
        if (["__proto__", "prototype", "constructor"].includes(name))
          throw new HostContractValidationError("patch.json", "unsafe field name");
        pending.push({ value: child, depth: item.depth + 1 });
      }
    }
  }
  return value;
}
function selection(value: unknown): ReplayWriterSelection {
  const v = record(value, "writer");
  exactKeys(v, ["subject", "version", "messageType"], "writer");
  return {
    subject: text(v.subject, "writer.subject", 512),
    version: positiveBoundedInteger(v.version, "writer.version", 10_000),
    messageType: boundedText(v.messageType, "writer.messageType", 512),
  };
}
function sourceWriter(value: Record<string, unknown>): { format: RecordFormat; id: number | null } {
  const format = declaredValue(value.format, RECORD_FORMATS, "writer.format");
  const id = value.id === null ? null : positiveBoundedInteger(value.id, "writer.id", 0x7fffffff);
  if ((format === "json") !== (id === null))
    throw new HostContractValidationError("writer.id", "must match the source format");
  return { format, id };
}
function field(value: unknown): StructuredReplayField | null {
  if (value === null) return null;
  const v = record(value, "structuredField");
  exactKeys(v, ["codec", "patches", "mappings"], "structuredField");
  if (
    !Array.isArray(v.patches) ||
    v.patches.length > STRUCTURED_REPLAY_LIMITS.patches ||
    !Array.isArray(v.mappings) ||
    !v.mappings.length ||
    v.mappings.length > STRUCTURED_REPLAY_LIMITS.mappings
  )
    throw new HostContractValidationError(
      "structuredField",
      "use at most 16 patches and one to 50 explicit writer mappings",
    );
  const mappings = v.mappings.map((item: unknown): ReplayWriterMapping => {
    const m = record(item, "mapping");
    exactKeys(m, ["format", "sourceId", "target"], "mapping");
    const source = sourceWriter({ format: m.format, id: m.sourceId });
    if (source.format !== "json" && m.target === null)
      throw new HostContractValidationError(
        "mapping.target",
        "select an existing destination writer for framed bytes",
      );
    return {
      format: source.format,
      sourceId: source.id,
      target: m.target === null ? null : selection(m.target),
    };
  });
  if (new Set(mappings.map((m) => `${m.format}:${m.sourceId}`)).size !== mappings.length)
    throw new HostContractValidationError("mapping", "duplicate source writer");
  return {
    codec: declaredValue(v.codec, ["auto", ...RECORD_FORMATS] as const, "structuredField.codec"),
    mappings,
    patches: v.patches.map((item: unknown): ReplayJsonPatch => {
      const p = record(item, "patch");
      const op = declaredValue(p.op, ["set", "remove"] as const, "patch.op");
      exactKeys(p, op === "set" ? ["op", "path", "json"] : ["op", "path"], "patch");
      const path = boundedText(p.path, "patch.path", STRUCTURED_REPLAY_LIMITS.path);
      replayPatchSegments(path);
      if (op === "remove") {
        if (!path)
          throw new HostContractValidationError("patch.path", "cannot remove the entire record");
        return { op, path };
      }
      const json = text(p.json, "patch.json", STRUCTURED_REPLAY_LIMITS.patchBytes);
      parseReplayPatchJson(json);
      return { op, path, json };
    }),
  };
}
export function parseStructuredReplayTransform(value: unknown): StructuredReplayTransform {
  const v = record(value, "structuredTransform");
  exactKeys(v, ["key", "value"], "structuredTransform");
  const result = { key: field(v.key), value: field(v.value) };
  if (!result.key && !result.value)
    throw new HostContractValidationError("structuredTransform", "select at least one field");
  return result;
}
export function parseReplayRecordEncoding(value: unknown): ReplayRecordEncoding {
  const v = record(value, "recordEncoding");
  exactKeys(v, ["key", "value"], "recordEncoding");
  const encoding = (value: unknown): ReplayFieldEncoding | null => {
    if (value === null) return null;
    const f = record(value, "fieldEncoding");
    exactKeys(f, ["source", "target"], "fieldEncoding");
    const s = record(f.source, "sourceWriter");
    exactKeys(s, ["format", "id"], "sourceWriter");
    const source = sourceWriter(s);
    if (f.target === null) {
      if (source.format !== "json")
        throw new HostContractValidationError(
          "fieldEncoding.target",
          "binary fields require a destination writer",
        );
      return { source, target: null };
    }
    const t = record(f.target, "targetWriter");
    exactKeys(
      t,
      ["subject", "version", "messageType", "id", "schemaType", "fingerprint"],
      "targetWriter",
    );
    const fingerprint = text(t.fingerprint, "writer.fingerprint", 64);
    if (!/^[a-f0-9]{64}$/u.test(fingerprint))
      throw new HostContractValidationError("writer.fingerprint", "requires SHA-256");
    const schemaType = declaredValue(t.schemaType, SCHEMA_REGISTRY_TYPES, "writer.schemaType");
    const selected = selection({
      subject: t.subject,
      version: t.version,
      messageType: t.messageType,
    });
    if ((schemaType === "PROTOBUF") !== (selected.messageType !== ""))
      throw new HostContractValidationError(
        "writer.messageType",
        "must match the destination type",
      );
    return {
      source,
      target: {
        ...selected,
        id: positiveBoundedInteger(t.id, "writer.id", 0x7fffffff),
        schemaType,
        fingerprint,
      },
    };
  };
  return { key: encoding(v.key), value: encoding(v.value) };
}

function wireId(bytes: string): number | null {
  const decoded = atob(bytes);
  if (decoded.length < 5 || decoded.charCodeAt(0) !== 0) return null;
  return new DataView(
    Uint8Array.from(decoded.slice(0, 5), (c) => c.charCodeAt(0)).buffer,
  ).getUint32(1);
}
/** A stored review must describe its actual bytes and its explicitly selected writers. */
export function validateReplayEncoding(
  original: KafkaCompleteRecord,
  output: KafkaCompleteRecord,
  transform: StructuredReplayTransform,
  evidence: ReplayRecordEncoding,
): void {
  for (const name of ["key", "value"] as const) {
    const selected = transform[name],
      encoded = evidence[name],
      before = original[name],
      after = output[name];
    if (!selected || before === null) {
      if (encoded !== null || (selected && after !== null))
        throw new Error("Unexpected field writer evidence.");
      continue;
    }
    if (!encoded || after === null) throw new Error("Missing field writer evidence.");
    if (selected.codec !== "auto" && selected.codec !== encoded.source.format)
      throw new Error("Source codec differs from writer evidence.");
    if (encoded.source.id !== wireId(before))
      throw new Error("Source writer differs from original bytes.");
    const mapping = selected.mappings.find(
      (m) => m.format === encoded.source.format && m.sourceId === encoded.source.id,
    );
    if (!mapping || (mapping.target === null) !== (encoded.target === null))
      throw new Error("Writer mapping differs from evidence.");
    if (
      mapping.target &&
      encoded.target &&
      (mapping.target.subject !== encoded.target.subject ||
        mapping.target.version !== encoded.target.version ||
        (mapping.target.messageType &&
          mapping.target.messageType.replace(/^\./u, "") !==
            encoded.target.messageType.replace(/^\./u, "")))
    )
      throw new Error("Destination selection differs from writer evidence.");
    if (encoded.target && encoded.target.schemaType !== "JSON") {
      if (wireId(after) !== encoded.target.id)
        throw new Error("Destination writer differs from output bytes.");
    } else {
      if (wireId(after) !== null)
        throw new Error("JSON output must not contain a binary writer frame.");
      JSON.parse(
        new TextDecoder("utf-8", { fatal: true }).decode(
          Uint8Array.from(atob(after), (c) => c.charCodeAt(0)),
        ),
      );
    }
  }
}
