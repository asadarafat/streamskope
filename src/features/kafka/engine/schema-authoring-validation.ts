import avro from "avsc";
import protobuf from "protobufjs";

import { SCHEMA_AUTHORING_LIMITS, type SchemaAuthoringIssue } from "../contracts/schema-authoring";

export class SchemaPayloadError extends Error {
  constructor(readonly issue: SchemaAuthoringIssue) {
    super(issue.code);
  }
}
export function invalidPayload(
  path: string,
  code: SchemaAuthoringIssue["code"],
  detail: string,
): never {
  throw new SchemaPayloadError({
    path: path.slice(0, SCHEMA_AUTHORING_LIMITS.pathCharacters),
    code,
    detail,
  });
}
export const childPath = (path: string, name: string): string =>
  `${path}/${name.replaceAll("~", "~0").replaceAll("/", "~1")}`;
const object = (value: unknown, path: string): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value))
    invalidPayload(path, "schema", "Expected an object.");
  return value as Record<string, unknown>;
};

/** avsc accepts extra fields while converting JSON; reject them before conversion can discard data. */
export function checkAvroFields(type: avro.Type, value: unknown, path = "", depth = 0): void {
  if (depth > 32) invalidPayload(path, "limit", "Payload structure exceeds 32 levels.");
  if (type instanceof avro.types.RecordType) {
    const fields = object(value, path);
    for (const name of Object.keys(fields))
      if (!type.fields.some((field) => field.name === name))
        invalidPayload(
          childPath(path, name),
          "schema",
          "Field is not declared in the writer schema.",
        );
    for (const field of type.fields)
      if (Object.hasOwn(fields, field.name))
        checkAvroFields(field.type, fields[field.name], childPath(path, field.name), depth + 1);
  } else if (type instanceof avro.types.ArrayType && Array.isArray(value)) {
    value.forEach((item: unknown, index) =>
      checkAvroFields(type.itemsType, item, childPath(path, String(index)), depth + 1),
    );
  } else if (type instanceof avro.types.MapType) {
    for (const [name, item] of Object.entries(object(value, path)))
      checkAvroFields(type.valuesType as avro.Type, item, childPath(path, name), depth + 1);
  } else if (type instanceof avro.types.WrappedUnionType && value !== null) {
    const wrapped = object(value, path);
    const names = Object.keys(wrapped);
    if (names.length !== 1)
      invalidPayload(path, "schema", "Avro unions require exactly one named branch, or null.");
    const name = names[0]!;
    const branch = type.types.find((candidate) => (candidate.name ?? candidate.typeName) === name);
    if (!branch)
      invalidPayload(path, "schema", "The union branch is not declared in the writer schema.");
    checkAvroFields(branch, wrapped[name], childPath(path, name), depth + 1);
  } else if (type.typeName === "bytes" || type instanceof avro.types.FixedType) {
    if (
      typeof value !== "string" ||
      [...value].some((character) => character.codePointAt(0)! > 255)
    )
      invalidPayload(
        path,
        "schema",
        "Avro bytes/fixed require an Avro JSON byte string with code points 0–255.",
      );
  }
}

function scalar(field: protobuf.Field, value: unknown, path: string, depth: number): void {
  if (field.resolvedType instanceof protobuf.Type) {
    checkProtobufPayload(field.resolvedType, value, path, depth + 1);
    return;
  }
  if (field.resolvedType instanceof protobuf.Enum) {
    if (typeof value !== "string" || !Object.hasOwn(field.resolvedType.values, value))
      invalidPayload(path, "schema", "Choose a declared enum name.");
    return;
  }
  if (field.type === "string" || field.type === "bool") {
    if (typeof value !== (field.type === "bool" ? "boolean" : "string"))
      invalidPayload(
        path,
        "schema",
        `Expected ${field.type === "bool" ? "a boolean" : "a string"}.`,
      );
  } else if (field.type === "bytes") {
    if (
      typeof value !== "string" ||
      value.length % 4 !== 0 ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u.test(value) ||
      Buffer.from(value, "base64").toString("base64") !== value
    )
      invalidPayload(path, "schema", "Expected canonical padded Base64 bytes.");
  } else if (/64$/u.test(field.type)) {
    const unsigned = field.type === "uint64" || field.type === "fixed64";
    if (typeof value !== "string" || !/^-?(?:0|[1-9]\d*)$/u.test(value) || value.length > 21)
      invalidPayload(path, "precision", "64-bit integers require an exact decimal string.");
    const integer = BigInt(value);
    if (
      integer < (unsigned ? 0n : -(1n << 63n)) ||
      integer > (unsigned ? (1n << 64n) - 1n : (1n << 63n) - 1n)
    )
      invalidPayload(path, "schema", "Integer exceeds its declared 64-bit range.");
  } else if (field.type === "double" || field.type === "float") {
    if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      (field.type === "float" && !Number.isFinite(Math.fround(value)))
    )
      invalidPayload(path, "schema", "Expected a finite number within the field's range.");
  } else {
    const unsigned = field.type === "uint32" || field.type === "fixed32";
    if (
      typeof value !== "number" ||
      !Number.isInteger(value) ||
      value < (unsigned ? 0 : -0x80000000) ||
      value > (unsigned ? 0xffffffff : 0x7fffffff)
    )
      invalidPayload(path, "schema", "Expected an integer within the declared 32-bit range.");
  }
}
export function checkProtobufPayload(
  type: protobuf.Type,
  value: unknown,
  path = "",
  depth = 0,
): void {
  if (depth > 32) invalidPayload(path, "limit", "Payload structure exceeds 32 levels.");
  const values = object(value, path);
  for (const name of Object.keys(values))
    if (!Object.hasOwn(type.fields, name))
      invalidPayload(
        childPath(path, name),
        "schema",
        "Field is not declared in the writer message.",
      );
  for (const group of type.oneofsArray)
    if (group.oneof.filter((name) => Object.hasOwn(values, name)).length > 1)
      invalidPayload(path, "schema", "Only one field in each oneof may be supplied.");
  for (const field of type.fieldsArray) {
    const next = childPath(path, field.name);
    if (!Object.hasOwn(values, field.name)) {
      if (field.required) invalidPayload(next, "schema", "A required field is missing.");
      continue;
    }
    const entry = values[field.name];
    if (field instanceof protobuf.MapField) {
      for (const [name, item] of Object.entries(object(entry, next))) {
        if (
          field.keyType === "bool"
            ? name !== "true" && name !== "false"
            : field.keyType !== "string" && !/^-?(?:0|[1-9]\d*)$/u.test(name)
        )
          invalidPayload(next, "schema", "Map key does not match its declared type.");
        if (field.keyType !== "string" && field.keyType !== "bool") {
          const unsigned = /^(?:u|fixed)/u.test(field.keyType);
          const bits = field.keyType.endsWith("64") ? 64n : 32n;
          if (
            name.length > 21 ||
            BigInt(name) < (unsigned ? 0n : -(1n << (bits - 1n))) ||
            BigInt(name) > (unsigned ? (1n << bits) - 1n : (1n << (bits - 1n)) - 1n)
          )
            invalidPayload(next, "schema", "Map key exceeds its declared integer range.");
        }
        scalar(field, item, childPath(next, name), depth);
      }
    } else if (field.repeated) {
      if (!Array.isArray(entry)) invalidPayload(next, "schema", "Expected an array.");
      entry.forEach((item: unknown, index) =>
        scalar(field, item, childPath(next, String(index)), depth),
      );
    } else scalar(field, entry, next, depth);
  }
}
