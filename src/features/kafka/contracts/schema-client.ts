import { record, exactKeys, text, nonNegativeInteger } from "./validation-primitives";
export interface SchemaClient {
  readonly generator: string;
  readonly language: "JavaScript CommonJS";
  readonly subject: string;
  readonly version: number;
  readonly schemaId: number;
  readonly sha256: string;
  readonly source: string;
}
export function parseSchemaClient(value: unknown): SchemaClient {
  const p = record(value, "client");
  exactKeys(
    p,
    ["generator", "language", "subject", "version", "schemaId", "sha256", "source"],
    "client",
  );
  if (p.language !== "JavaScript CommonJS") throw new Error("Unsupported generated language.");
  const sha256 = text(p.sha256, "sha256", 64);
  if (!/^[a-f0-9]{64}$/u.test(sha256)) throw new Error("Invalid schema fingerprint.");
  return {
    generator: text(p.generator, "generator", 256),
    language: p.language,
    subject: text(p.subject, "subject", 512),
    version: nonNegativeInteger(p.version, "version"),
    schemaId: nonNegativeInteger(p.schemaId, "schemaId"),
    sha256,
    source: text(p.source, "source", 262144),
  };
}
