import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
  truth,
} from "./validation-primitives";

export const RELATIONSHIP_LIMITS = {
  topics: 3,
  groups: 20,
  connectors: 10,
  subjects: 20,
  schemaReads: 24,
  nodes: 80,
  edges: 160,
  deadlineMs: 30_000,
  staleMs: 60_000,
  bytes: 512 * 1024,
} as const;
export interface RelationshipInput {
  readonly topics: readonly string[];
  readonly subject: string | null;
  readonly version: number | null;
  readonly sampleRecords: boolean;
}
export const RELATIONSHIP_KINDS = ["topic", "group", "connector", "schema", "schema-id"] as const;
export const RELATIONSHIP_SOURCES = [
  "Kafka metadata",
  "Kafka groups",
  "Kafka Connect",
  "Schema Registry",
  "Protected record sample",
] as const;
export type RelationshipSource = (typeof RELATIONSHIP_SOURCES)[number];
export interface RelationshipNode {
  readonly id: string;
  readonly kind: (typeof RELATIONSHIP_KINDS)[number];
  readonly label: string;
  readonly version: number | null;
}
export const RELATIONSHIPS = [
  "assigned",
  "committed",
  "writes",
  "reads",
  "configured",
  "reports-topic",
  "references",
  "registered-id",
  "framed-id",
  "naming-match",
] as const;
export interface RelationshipEdge {
  readonly from: string;
  readonly to: string;
  readonly relation: (typeof RELATIONSHIPS)[number];
  readonly evidence: "observed" | "declared" | "inferred";
  readonly source: RelationshipSource;
  readonly observedAt: number;
  readonly detail: string;
}
export interface RelationshipCoverage {
  readonly source: RelationshipSource;
  readonly state: "complete" | "limited" | "unavailable" | "not-configured" | "not-requested";
  readonly inspected: number;
  readonly omitted: number | null;
  readonly detail: string;
}
export interface RelationshipGraph {
  readonly startedAt: number;
  readonly observedAt: number;
  readonly clusterId: string;
  readonly target: string | null;
  readonly nodes: readonly RelationshipNode[];
  readonly edges: readonly RelationshipEdge[];
  readonly coverage: readonly RelationshipCoverage[];
}
export function relationshipNodeId(
  kind: RelationshipNode["kind"],
  label: string,
  version: number | null = null,
): string {
  return JSON.stringify([kind, label, version]);
}
function integer(v: unknown, maximum: number): number {
  const n = nonNegativeInteger(v, "number");
  if (n > maximum) throw new Error("Relationship value exceeds bound.");
  return n;
}
function list<T>(v: unknown, maximum: number, parse: (x: unknown) => T): T[] {
  if (!Array.isArray(v) || v.length > maximum) throw new Error("Relationship list exceeds bound.");
  return (v as unknown[]).map(parse);
}
export function parseRelationshipInput(v: unknown): RelationshipInput {
  const p = record(v, "relationship input");
  exactKeys(p, ["topics", "subject", "version", "sampleRecords"], "relationship input");
  const topics = list(p.topics, RELATIONSHIP_LIMITS.topics, (t) => {
    const value = text(t, "topic", 249);
    if (!/^[A-Za-z0-9._-]+$/.test(value) || value === "." || value === "..")
      throw new Error("Invalid topic.");
    return value;
  });
  const subject = p.subject === null ? null : text(p.subject, "subject", 512);
  const version = p.version === null ? null : integer(p.version, 2147483647);
  if (
    !topics.length ||
    new Set(topics).size !== topics.length ||
    (subject === null) !== (version === null) ||
    version === 0
  )
    throw new Error("Choose one to three distinct topics and optionally an exact subject/version.");
  return { topics, subject, version, sampleRecords: truth(p.sampleRecords, "sampleRecords") };
}
export function parseRelationshipGraph(v: unknown): RelationshipGraph {
  if (new TextEncoder().encode(JSON.stringify(v)).length > RELATIONSHIP_LIMITS.bytes)
    throw new Error("Relationship graph exceeds byte bound.");
  const p = record(v, "graph");
  exactKeys(
    p,
    ["startedAt", "observedAt", "clusterId", "target", "nodes", "edges", "coverage"],
    "graph",
  );
  const startedAt = integer(p.startedAt, 8.64e15),
    observedAt = integer(p.observedAt, 8.64e15);
  if (observedAt < startedAt || observedAt - startedAt > RELATIONSHIP_LIMITS.deadlineMs + 1000)
    throw new Error("Invalid graph observation time.");
  const nodes = list(p.nodes, RELATIONSHIP_LIMITS.nodes, (v) => {
    const n = record(v, "node");
    exactKeys(n, ["id", "kind", "label", "version"], "node");
    return {
      id: text(n.id, "id", 2048),
      kind: declaredValue(n.kind, RELATIONSHIP_KINDS, "kind"),
      label: text(n.label, "label", 512),
      version: n.version === null ? null : integer(n.version, 2147483647),
    };
  });
  const ids = new Set(nodes.map((n) => n.id));
  if (ids.size !== nodes.length) throw new Error("Duplicate graph identity.");
  const target = p.target === null ? null : text(p.target, "target", 2048);
  if (target !== null && !ids.has(target)) throw new Error("Missing impact target.");
  const edges = list(p.edges, RELATIONSHIP_LIMITS.edges, (v) => {
    const e = record(v, "edge");
    exactKeys(e, ["from", "to", "relation", "evidence", "source", "observedAt", "detail"], "edge");
    const from = text(e.from, "from", 2048),
      to = text(e.to, "to", 2048),
      at = integer(e.observedAt, 8.64e15);
    if (!ids.has(from) || !ids.has(to) || at < startedAt || at > observedAt)
      throw new Error("Invalid graph edge.");
    return {
      from,
      to,
      relation: declaredValue(e.relation, RELATIONSHIPS, "relation"),
      evidence: declaredValue(
        e.evidence,
        ["observed", "declared", "inferred"] as const,
        "evidence",
      ),
      source: declaredValue(e.source, RELATIONSHIP_SOURCES, "source"),
      observedAt: at,
      detail: text(e.detail, "detail", 1024),
    };
  });
  const coverage = list(p.coverage, 32, (v) => {
    const c = record(v, "coverage");
    exactKeys(c, ["source", "state", "inspected", "omitted", "detail"], "coverage");
    return {
      source: declaredValue(c.source, RELATIONSHIP_SOURCES, "source"),
      state: declaredValue(
        c.state,
        ["complete", "limited", "unavailable", "not-configured", "not-requested"] as const,
        "state",
      ),
      inspected: integer(c.inspected, 10000),
      omitted: c.omitted === null ? null : integer(c.omitted, 1e9),
      detail: text(c.detail, "detail", 1024),
    };
  });
  return {
    startedAt,
    observedAt,
    clusterId: text(p.clusterId, "clusterId", 512),
    target,
    nodes,
    edges,
    coverage,
  };
}
export interface SchemaImpact {
  readonly nodeId: string;
  readonly path: readonly number[];
}
/** Follow schema dependents, then topic mappings, then adjacent groups/connectors only. */
export function schemaImpact(graph: RelationshipGraph): readonly SchemaImpact[] {
  if (!graph.target) return [];
  const paths = new Map<string, readonly number[]>([[graph.target, []]]),
    queue = [graph.target];
  while (queue.length) {
    const id = queue.shift()!;
    graph.edges.forEach((e, index) => {
      if (e.relation === "references" && e.to === id && !paths.has(e.from)) {
        paths.set(e.from, [...paths.get(id)!, index]);
        queue.push(e.from);
      }
    });
  }
  for (const [id, path] of [...paths])
    graph.edges.forEach((e, i) => {
      if (e.relation === "registered-id" && e.to === id && !paths.has(e.from))
        paths.set(e.from, [...path, i]);
    });
  for (const [id, path] of [...paths])
    graph.edges.forEach((e, i) => {
      if (
        (e.relation === "framed-id" || e.relation === "naming-match") &&
        e.to === id &&
        !paths.has(e.from)
      )
        paths.set(e.from, [...path, i]);
    });
  for (const [id, path] of [...paths]) {
    if (graph.nodes.find((n) => n.id === id)?.kind !== "topic") continue;
    graph.edges.forEach((e, i) => {
      const other = e.from === id ? e.to : e.to === id ? e.from : null;
      if (
        other &&
        ["group", "connector"].includes(graph.nodes.find((n) => n.id === other)?.kind ?? "") &&
        !paths.has(other)
      )
        paths.set(other, [...path, i]);
    });
  }
  return [...paths]
    .filter(([id]) => id !== graph.target)
    .map(([nodeId, path]) => ({ nodeId, path }));
}
