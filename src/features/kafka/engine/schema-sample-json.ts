import Ajv from "ajv";

import type { CodecSchemaBundle } from "../application/record-codec-types";

const keywords = new Set([
  "$schema",
  "$id",
  "$ref",
  "$defs",
  "definitions",
  "title",
  "description",
  "$comment",
  "examples",
  "default",
  "type",
  "properties",
  "required",
  "additionalProperties",
  "items",
  "minItems",
  "maxItems",
  "uniqueItems",
  "minLength",
  "maxLength",
  "minimum",
  "maximum",
  "exclusiveMinimum",
  "exclusiveMaximum",
  "multipleOf",
  "enum",
  "const",
  "anyOf",
  "oneOf",
  "minProperties",
  "maxProperties",
  "readOnly",
  "writeOnly",
  "deprecated",
]);
const object = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("A JSON Schema object is required.");
  return value as Record<string, unknown>;
};
const numeric = (value: unknown, fallback: number): number =>
  typeof value === "number" && Number.isFinite(value) ? value : fallback;

export function jsonSampleGenerator(
  bundle: CodecSchemaBundle,
  random: () => number,
): () => unknown {
  const root: unknown = JSON.parse(bundle.root.schema);
  const documents = new Map<string, unknown>([["", root]]);
  const validator = new Ajv({
    strict: true,
    allErrors: false,
    allowUnionTypes: true,
    logger: false,
    code: { optimize: false },
  });
  const checkKeywords = (schema: unknown, depth: number): void => {
    if (depth > 32) throw new Error("JSON Schema is too deeply nested.");
    if (typeof schema === "boolean") return;
    const value = object(schema);
    for (const key of Object.keys(value))
      if (!keywords.has(key))
        throw new Error(`Unsupported JSON Schema keyword: ${key.slice(0, 80)}`);
    if (
      value.$schema !== undefined &&
      value.$schema !== "http://json-schema.org/draft-07/schema#" &&
      value.$schema !== "https://json-schema.org/draft-07/schema#"
    )
      throw new Error("JSON sample generation supports draft-07 only.");
    for (const collection of [value.properties, value.$defs, value.definitions])
      if (collection)
        for (const child of Object.values(object(collection))) checkKeywords(child, depth + 1);
    for (const choices of [value.anyOf, value.oneOf])
      if (Array.isArray(choices)) for (const child of choices) checkKeywords(child, depth + 1);
    if (value.items !== undefined) checkKeywords(value.items, depth + 1);
    if (value.additionalProperties !== undefined && typeof value.additionalProperties !== "boolean")
      checkKeywords(value.additionalProperties, depth + 1);
  };
  checkKeywords(root, 0);
  for (const { name, schema } of bundle.dependencies) {
    if (schema.schemaType !== "JSON")
      throw new Error("JSON references require JSON Schema definitions.");
    const parsed: unknown = JSON.parse(schema.schema);
    checkKeywords(parsed, 0);
    documents.set(name, parsed);
    validator.addSchema(parsed as object, name);
  }
  const validate = validator.compile(root as object);
  const sample = (schema: unknown, currentDocument: unknown, depth: number): unknown => {
    if (depth > 8) throw new Error("JSON samples exceed the eight-level generation limit.");
    if (schema === true) return null;
    if (schema === false) throw new Error("A false schema cannot generate a valid value.");
    const value = object(schema);
    if (Object.hasOwn(value, "default")) return structuredClone(value.default);
    if (Object.hasOwn(value, "const")) return structuredClone(value.const);
    if (Array.isArray(value.enum) && value.enum.length)
      return structuredClone(value.enum[Math.floor(random() * value.enum.length)]);
    if (typeof value.$ref === "string") {
      const [file = "", fragment = ""] = value.$ref.split("#");
      const document = file === "" ? currentDocument : documents.get(file);
      if (document === undefined || (fragment !== "" && !fragment.startsWith("/")))
        throw new Error("Only declared references and JSON Pointer fragments are supported.");
      let target: unknown = document;
      for (const token of fragment.split("/").slice(1)) {
        const key = decodeURIComponent(token).replaceAll("~1", "/").replaceAll("~0", "~");
        const parent = object(target);
        if (!Object.hasOwn(parent, key)) throw new Error("A JSON reference could not be resolved.");
        target = parent[key];
      }
      return sample(target, document, depth + 1);
    }
    const alternatives = value.oneOf ?? value.anyOf;
    if (Array.isArray(alternatives) && alternatives.length)
      return sample(
        alternatives[Math.floor(random() * alternatives.length)],
        currentDocument,
        depth + 1,
      );
    const type: unknown = Array.isArray(value.type)
      ? value.type[Math.floor(random() * value.type.length)]
      : value.type;
    switch (type) {
      case "null":
        return null;
      case "boolean":
        return random() < 0.5;
      case "integer":
      case "number": {
        const step = numeric(value.multipleOf, 1);
        if (step <= 0) throw new Error("Invalid numeric step.");
        const minimum = Math.max(
          numeric(value.minimum, -1_000),
          value.exclusiveMinimum === undefined
            ? -Infinity
            : numeric(value.exclusiveMinimum, 0) + step,
        );
        const maximum = Math.min(
          numeric(value.maximum, 1_000),
          value.exclusiveMaximum === undefined
            ? Infinity
            : numeric(value.exclusiveMaximum, 0) - step,
        );
        const low = Math.ceil(minimum / step);
        const high = Math.floor(maximum / step);
        if (!Number.isSafeInteger(low) || !Number.isSafeInteger(high) || low > high)
          throw new Error("Numeric constraints exceed supported sample bounds.");
        return (low + Math.floor(random() * (Math.min(high - low, 2_000) + 1))) * step;
      }
      case "string": {
        const minimum = numeric(value.minLength, 0);
        const maximum = Math.min(numeric(value.maxLength, 64), 1_024);
        if (minimum > maximum || minimum < 0)
          throw new Error("String constraints exceed the 1,024-character sample limit.");
        return `sample-${Math.floor(random() * 1_000_000).toString(36)}`
          .padEnd(minimum, "x")
          .slice(0, maximum);
      }
      case "array": {
        const length = Math.max(
          numeric(value.minItems, 0),
          Math.min(numeric(value.maxItems, 2), 2),
        );
        if (length > 8) throw new Error("Arrays support at most eight generated items.");
        return Array.from({ length }, () =>
          sample(value.items ?? true, currentDocument, depth + 1),
        );
      }
      case "object": {
        const properties = value.properties === undefined ? {} : object(value.properties);
        if (Object.keys(properties).length > 128)
          throw new Error("Objects support at most 128 generated fields.");
        return Object.fromEntries(
          Object.entries(properties).map(([key, schema]) => [
            key,
            sample(schema, currentDocument, depth + 1),
          ]),
        );
      }
      default:
        throw new Error("Declare a supported JSON type, enum, const, union or reference.");
    }
  };
  return (): unknown => {
    for (let attempt = 0; attempt < 16; attempt++) {
      const value = sample(root, root, 0);
      if (validate(value)) return value;
    }
    throw new Error("No valid sample satisfied all constraints in 16 bounded attempts.");
  };
}
