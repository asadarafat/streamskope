import {
  RELATIONSHIP_LIMITS as limits,
  relationshipNodeId,
  type RelationshipNode,
  type RelationshipEdge,
  type RelationshipCoverage,
  type RelationshipSource,
} from "../contracts/relationships";

export class RelationshipBuilder {
  readonly nodes: RelationshipNode[] = [];
  readonly edges: RelationshipEdge[] = [];
  readonly coverage: RelationshipCoverage[] = [];
  truncated = false;
  constructor(readonly now: () => number) {}
  node(
    kind: RelationshipNode["kind"],
    label: string,
    version: number | null = null,
  ): string | null {
    const id = relationshipNodeId(kind, label, version);
    if (this.nodes.some((n) => n.id === id)) return id;
    if (this.nodes.length >= limits.nodes) {
      this.truncated = true;
      return null;
    }
    this.nodes.push({ id, kind, label, version });
    return id;
  }
  edge(
    from: string | null,
    to: string | null,
    relation: RelationshipEdge["relation"],
    evidence: RelationshipEdge["evidence"],
    source: RelationshipSource,
    detail: string,
  ): void {
    if (from === null || to === null) return;
    if (this.edges.some((e) => e.from === from && e.to === to && e.relation === relation)) return;
    if (this.edges.length >= limits.edges) {
      this.truncated = true;
      return;
    }
    this.edges.push({ from, to, relation, evidence, source, detail, observedAt: this.now() });
  }
}
