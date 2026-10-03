import { parseSchemaIdentity, parseSchemaVersion } from "./schema-registry-validation";
import type { SchemaVersionDetail } from "./schema-registry-types";
import { HostContractValidationError } from "./validation-error";
import {
  exactKeys,
  record,
  text,
  declaredValue,
  nonNegativeInteger,
  truth,
} from "./validation-primitives";

export const SCHEMA_INSPECTION_LIMITS = {
  edges: 64,
  nodes: 32,
  depth: 8,
  bytes: 1_048_576,
  milliseconds: 15_000,
} as const;
export interface SchemaInspectionInput {
  readonly subject: string;
  readonly version: number;
}
export interface SchemaReferenceEdge {
  readonly from: SchemaInspectionInput;
  readonly to: SchemaInspectionInput;
  readonly name: string;
  readonly depth: number;
  readonly state: "resolved" | "unavailable" | "cycle" | "limit";
}
export interface SchemaInspection {
  readonly root: SchemaVersionDetail;
  readonly edges: readonly SchemaReferenceEdge[];
  readonly limited: boolean;
}
export function parseSchemaInspectionInput(value: unknown): SchemaInspectionInput {
  const identity = parseSchemaIdentity(value, "inspection");
  if (identity.version === "latest")
    throw new HostContractValidationError("inspection.version", "requires an exact version");
  return identity as SchemaInspectionInput;
}
export function parseSchemaInspection(value: unknown): SchemaInspection {
  const input = record(value, "inspection");
  exactKeys(input, ["root", "edges", "limited"], "inspection");
  if (!Array.isArray(input.edges) || input.edges.length > SCHEMA_INSPECTION_LIMITS.edges)
    throw new HostContractValidationError("inspection.edges", "exceeds the graph bound");
  return {
    root: parseSchemaVersion(input.root, "inspection.root"),
    limited: truth(input.limited, "inspection.limited"),
    edges: input.edges.map((value: unknown) => {
      const edge = record(value, "edge");
      exactKeys(edge, ["from", "to", "name", "depth", "state"], "edge");
      const depth = nonNegativeInteger(edge.depth, "edge.depth");
      if (depth > SCHEMA_INSPECTION_LIMITS.depth + 1)
        throw new HostContractValidationError("edge.depth", "exceeds the graph depth");
      return {
        from: parseSchemaInspectionInput(edge.from),
        to: parseSchemaInspectionInput(edge.to),
        name: text(edge.name, "edge.name", 512),
        depth,
        state: declaredValue(
          edge.state,
          ["resolved", "unavailable", "cycle", "limit"] as const,
          "edge.state",
        ),
      };
    }),
  };
}
