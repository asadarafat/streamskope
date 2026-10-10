import Ajv, { type ValidateFunction } from "ajv";

import type { CodecSchemaBundle } from "../application/record-codec-types";

/** No fetch/load hook: only the Registry's bounded declared graph can resolve a reference. */
export function compileJsonSchema(bundle: CodecSchemaBundle): ValidateFunction {
  const validator = new Ajv({
    strict: true,
    allErrors: false,
    allowUnionTypes: true,
    logger: false,
    code: { optimize: false },
  });
  validator.addMetaSchema(
    { $ref: "http://json-schema.org/draft-07/schema#" },
    "https://json-schema.org/draft-07/schema#",
  );
  const parse = (source: string): object | boolean => {
    const schema: unknown = JSON.parse(source, (_key: string, value: unknown) => {
      if (
        typeof value === "number" &&
        (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value)))
      )
        throw new Error("JSON sample schemas require finite numbers and safe integer values.");
      return value;
    });
    if (
      typeof schema !== "boolean" &&
      (!schema || typeof schema !== "object" || Array.isArray(schema))
    )
      throw new Error("A JSON Schema object or boolean is required.");
    const dialect =
      typeof schema === "object" && schema !== null && "$schema" in schema
        ? schema.$schema
        : undefined;
    if (
      dialect !== undefined &&
      dialect !== "http://json-schema.org/draft-07/schema#" &&
      dialect !== "https://json-schema.org/draft-07/schema#"
    )
      throw new Error("JSON validation supports draft-07 only.");
    return schema;
  };
  for (const { name, schema } of bundle.dependencies) {
    if (schema.schemaType !== "JSON") throw new Error("JSON reference type differs.");
    validator.addSchema(parse(schema.schema), name);
  }
  return validator.compile(parse(bundle.root.schema));
}
