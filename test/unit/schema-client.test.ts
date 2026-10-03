import { mkdir, mkdtemp, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";

import { expect, it } from "vitest";

import { generateSchemaClient } from "../../src/features/kafka/engine/schema-client-generator";
import type { CodecSchemaBundle } from "../../src/features/kafka/application/record-codec-types";
import { createHostRecordCodec } from "../../src/platform/node/record-codec";
const schema = {
  type: "object",
  properties: { id: { type: "integer" }, label: { type: "string", minLength: 1 } },
  required: ["id", "label"],
  additionalProperties: false,
};
const bundle: CodecSchemaBundle = {
  root: { id: 7, schemaType: "JSON", schema: JSON.stringify(schema), references: [] },
  dependencies: [],
};
it("compiles a pinned validating client with exact Registry framing and rejects invalid records/IDs", async () => {
  const client = generateSchemaClient({
    kind: "client",
    input: { subject: "events-value", version: 1 },
    bundle,
  });
  await mkdir(".artifacts", { recursive: true });
  const folder = await mkdtemp(join(process.cwd(), ".artifacts", "streamskope-client-"));
  try {
    const path = join(folder, "client.cjs");
    await writeFile(path, client.source);
    execFileSync(process.execPath, ["--check", path]);
    const codec = createRequire(import.meta.url)(path) as {
      encode(v: unknown): Buffer;
      decode(v: Buffer): unknown;
      provenance: { sha256: string };
    };
    const value = { id: 42, label: "é" };
    const bytes = codec.encode(value);
    expect([...bytes.subarray(0, 5)]).toEqual([0, 0, 0, 0, 7]);
    expect(codec.decode(bytes)).toEqual(value);
    expect(codec.provenance.sha256).toBe(client.sha256);
    expect(() => codec.encode({ id: "wrong", label: "x" })).toThrow();
    expect(() => codec.decode(Buffer.from([0, 0, 0, 0, 8, 123, 125]))).toThrow();
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
it("uses the bounded worker and fails unsupported untrusted schemas closed", async () => {
  const worker = createHostRecordCodec();
  expect(
    (
      await worker.generateClient(
        { subject: "events-value", version: 1 },
        bundle,
        AbortSignal.timeout(15000),
      )
    ).generator,
  ).toBe("Ajv 8.20.0 standalone (MIT)");
  for (const definition of [
    { type: "string", pattern: "(a+)+$" },
    { $ref: "file:///etc/passwd" },
    { $schema: "https://json-schema.org/draft/2020-12/schema", type: "string" },
  ])
    expect(() =>
      generateSchemaClient({
        kind: "client",
        input: { subject: "x", version: 1 },
        bundle: { ...bundle, root: { ...bundle.root, schema: JSON.stringify(definition) } },
      }),
    ).toThrow();
});
