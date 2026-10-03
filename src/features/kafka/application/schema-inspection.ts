import {
  SCHEMA_INSPECTION_LIMITS as limits,
  type SchemaInspection,
  type SchemaInspectionInput,
  type SchemaReferenceEdge,
} from "../contracts/schema-inspection";

import type { RegisteredSchema, SchemaLookupPort } from "./record-codec-types";
import type { KafkaClusterServiceContext } from "./types";

export async function inspectSchema(
  input: SchemaInspectionInput,
  lookup: SchemaLookupPort,
  context: KafkaClusterServiceContext,
  signal: AbortSignal,
): Promise<SchemaInspection> {
  const key = (identity: SchemaInspectionInput): string =>
    JSON.stringify([identity.subject, identity.version]);
  signal.throwIfAborted();
  const root = await lookup.byVersion(context, input.subject, input.version, signal);
  signal.throwIfAborted();
  let bytes = new TextEncoder().encode(JSON.stringify(root)).length;
  if (bytes > limits.bytes) throw new Error("Schema exceeds the inspection byte limit.");
  const seen = new Map<string, SchemaReferenceEdge["state"]>([[key(input), "resolved"]]);
  const edges: SchemaReferenceEdge[] = [];
  let limited = false;
  const visit = async (
    schema: RegisteredSchema,
    parent: SchemaInspectionInput,
    path: ReadonlySet<string>,
    depth: number,
  ): Promise<void> => {
    for (const ref of schema.references) {
      signal.throwIfAborted();
      if (edges.length >= limits.edges) {
        limited = true;
        break;
      }
      const to = { subject: ref.subject, version: ref.version };
      const identity = key(to);
      const edge = { from: parent, to, name: ref.name, depth };
      if (path.has(identity)) {
        edges.push({ ...edge, state: "cycle" });
        continue;
      }
      if (
        bytes > limits.bytes ||
        depth > limits.depth ||
        (!seen.has(identity) && seen.size >= limits.nodes)
      ) {
        edges.push({ ...edge, state: "limit" });
        limited = true;
        continue;
      }
      if (seen.has(identity)) {
        edges.push({ ...edge, state: seen.get(identity)! });
        continue;
      }
      seen.set(identity, "unavailable");
      let child: RegisteredSchema;
      try {
        child = await lookup.byVersion(context, ref.subject, ref.version, signal);
        signal.throwIfAborted();
      } catch {
        signal.throwIfAborted();
        edges.push({ ...edge, state: "unavailable" });
        continue;
      }
      bytes += new TextEncoder().encode(JSON.stringify(child)).length;
      if (bytes > limits.bytes) {
        seen.set(identity, "limit");
        edges.push({ ...edge, state: "limit" });
        limited = true;
        continue;
      }
      seen.set(identity, "resolved");
      edges.push({ ...edge, state: "resolved" });
      await visit(child, to, new Set([...path, identity]), depth + 1);
    }
  };
  await visit(root, input, new Set([key(input)]), 1);
  signal.throwIfAborted();
  return { root: { ...root, ...input }, edges, limited };
}
