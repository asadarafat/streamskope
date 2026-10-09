import avro from "avsc";
import protobuf from "protobufjs";

import { RECORD_CODEC_LIMITS, type RecordDecodeResult } from "../contracts/record-codec";
import type { CodecSchemaBundle, RecordCodecWorkerInput } from "../application/record-codec-types";
import { boundedRecordJson as boundedJson, normalizeRecordJson } from "../application/record-json";
export { boundedJson };

// avsc's default long maps to JS numbers. Preserve all signed 64-bit values instead.
const decimalLong = avro.types.LongType.__with({
  fromBuffer: (buffer: Buffer): string => buffer.readBigInt64LE().toString(),
  toBuffer: (value: string): Buffer => {
    const buffer = Buffer.alloc(8);
    buffer.writeBigInt64LE(BigInt(value));
    return buffer;
  },
  fromJSON: (value: unknown): string => {
    if (typeof value !== "string" && (typeof value !== "number" || !Number.isSafeInteger(value)))
      throw new Error("Invalid long");
    return String(value);
  },
  toJSON: (value: string): string => value,
  isValid: (value: unknown): boolean =>
    typeof value === "string" &&
    /^-?\d+$/u.test(value) &&
    BigInt(value) >= -(1n << 63n) &&
    BigInt(value) < 1n << 63n,
  compare: (left: string, right: string): number =>
    BigInt(left) < BigInt(right) ? -1 : BigInt(left) > BigInt(right) ? 1 : 0,
});

export function avroType(bundle: CodecSchemaBundle): avro.Type {
  const registry: Record<string, avro.Type> = Object.create(null) as Record<string, avro.Type>;
  const options: Partial<avro.ForSchemaOptions> = {
    registry,
    wrapUnions: "always",
    typeHook: (schema) =>
      schema === "long" ||
      (typeof schema === "object" &&
        !Array.isArray(schema) &&
        "type" in schema &&
        schema.type === "long")
        ? decimalLong
        : undefined,
  };
  const compiled = new Set<number>();
  for (const { schema } of bundle.dependencies) {
    if (schema.schemaType !== "AVRO") throw new Error("Reference type differs");
    if (compiled.has(schema.id)) continue;
    const definition: unknown = JSON.parse(schema.schema);
    boundedJson(definition);
    avro.Type.forSchema(definition as avro.Schema, options);
    compiled.add(schema.id);
  }
  const definition: unknown = JSON.parse(bundle.root.schema);
  boundedJson(definition);
  return avro.Type.forSchema(definition as avro.Schema, options);
}

export function protobufTypes(bundle: CodecSchemaBundle): readonly protobuf.Type[] {
  const root = new protobuf.Root();
  const parsed = protobuf.parse(bundle.root.schema, root, { keepCase: true });
  const namespace = parsed.package ? root.lookup(parsed.package) : root;
  if (!(namespace instanceof protobuf.Namespace)) throw new Error("Invalid package");
  const types = namespace.nestedArray.filter(
    (entry): entry is protobuf.Type => entry instanceof protobuf.Type,
  );
  const imported = new Set<string>();
  const required = new Set([...(parsed.imports ?? []), ...(parsed.weakImports ?? [])]);
  const compiled = new Set<number>();
  for (const { name, schema } of bundle.dependencies) {
    if (schema.schemaType !== "PROTOBUF") throw new Error("Reference type differs");
    imported.add(name);
    if (compiled.has(schema.id)) continue;
    const dependency = protobuf.parse(schema.schema, root, { keepCase: true });
    for (const file of [...(dependency.imports ?? []), ...(dependency.weakImports ?? [])])
      required.add(file);
    compiled.add(schema.id);
  }
  for (const file of required) {
    if (imported.has(file)) continue;
    // Built-in Google definitions only; never let .proto imports read paths or URLs.
    const known: unknown = Object.hasOwn(protobuf.common, file)
      ? protobuf.common.get(file)
      : undefined;
    if (!known || typeof known !== "object" || !("nested" in known))
      throw new Error("Unresolved import");
    root.addJSON(known.nested as Record<string, protobuf.AnyNestedObject>);
  }
  root.resolveAll();
  return types;
}

function messageIndexes(bytes: Buffer): { indexes: number[]; offset: number } {
  let offset = 5;
  const read = (): number => {
    let raw = 0;
    for (let shift = 0; shift < 35; shift += 7) {
      if (offset >= bytes.length) throw new Error("Truncated message indexes");
      const byte = bytes[offset++]!;
      raw += (byte & 0x7f) * 2 ** shift;
      if (!(byte & 0x80)) {
        if (raw > 0xffffffff) throw new Error("Invalid index");
        return raw % 2 ? -(raw + 1) / 2 : raw / 2;
      }
    }
    throw new Error("Invalid message indexes");
  };
  const length = read();
  if (length === 0) return { indexes: [0], offset };
  if (length < 0 || length > RECORD_CODEC_LIMITS.jsonDepth)
    throw new Error("Invalid message index count");
  const indexes = Array.from({ length }, read);
  if (indexes.some((index) => index < 0)) throw new Error("Negative message index");
  return { indexes, offset };
}

export function parseStructuredRecord({
  input,
  bundle,
}: RecordCodecWorkerInput): RecordDecodeResult {
  const { format } = input;
  if (input.bytes === null) return { state: "null", format };
  try {
    const bytes = Buffer.from(input.bytes, "base64");
    if (bytes.length > RECORD_CODEC_LIMITS.bytes) throw new RangeError("Input limit");
    let value: unknown;
    let messageType: string | null = null;
    let notes: string;
    if (format === "json") {
      const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      value = JSON.parse(normalizeRecordJson(text)) as unknown;
      notes =
        "UTF-8 JSON. Integers outside JavaScript's safe range and non-finite numeric literals are displayed as their exact decimal text.";
    } else {
      if (!bundle || bytes.length < 5 || bytes[0] !== 0 || bytes.readUInt32BE(1) !== bundle.root.id)
        throw new Error("Invalid wire header");
      if (format === "avro") {
        const type = avroType(bundle);
        // Confluent's top-level bytes schema writes the bytes directly, without an Avro length.
        const decoded: unknown =
          type.typeName === "bytes" ? bytes.subarray(5) : type.fromBuffer(bytes.subarray(5));
        value = JSON.parse(type.toString(decoded)) as unknown;
        notes =
          "Avro JSON representation: long values are decimal strings; bytes/fixed use Avro JSON byte strings and unions retain their branch. Logical types retain their underlying storage values.";
      } else {
        const wire = messageIndexes(bytes);
        let types = protobufTypes(bundle);
        let selected: protobuf.Type | undefined;
        for (const index of wire.indexes) {
          selected = types[index];
          if (!selected) throw new Error("Message index does not name a type");
          types = selected.nestedArray.filter(
            (entry): entry is protobuf.Type => entry instanceof protobuf.Type,
          );
        }
        if (!selected) throw new Error("No message type");
        const decoded = selected.decode(bytes.subarray(wire.offset));
        value = selected.toObject(decoded, {
          longs: String,
          enums: String,
          bytes: String,
          defaults: false,
        });
        messageType = selected.fullName;
        notes =
          "Protobuf field names retain schema spelling; 64-bit integers are decimal strings and bytes are Base64. Absent fields stay absent. Unknown fields remain in the original bytes but are omitted from this projection.";
      }
    }
    return {
      state: "decoded",
      format,
      json: boundedJson(value),
      schemaId: bundle?.root.id ?? null,
      messageType,
      notes,
    };
  } catch (error) {
    return {
      state: "error",
      format,
      code: error instanceof RangeError ? "limit" : "malformed",
      detail:
        error instanceof RangeError
          ? "Decoded data exceeded the structure or output limit."
          : "The bytes or schema could not be decoded with the selected format. Check framing, schema references and the writer format; original bytes are unchanged.",
    };
  }
}
