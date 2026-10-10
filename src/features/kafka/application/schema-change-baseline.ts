import {
  SCHEMA_CHANGE_LIMITS,
  type SchemaChangeInput,
  type SchemaCompatibilityPolicy,
} from "../contracts/schema-changes";
import type { SchemaVersionDetail } from "../contracts/schema-registry-types";
import { utf8ByteLength } from "../contracts/message-limits";

import type { SchemaRegistryPort, SchemaRegistryReviewPort } from "./schema-registry-types";
import type { SchemaRegistryReviewScope } from "./connection-scope";
import { SchemaChangeReviewError } from "./schema-change-errors";

export interface SchemaChangeBaseline {
  readonly before: SchemaVersionDetail | null;
  readonly policy: SchemaCompatibilityPolicy;
  readonly dependencies: readonly SchemaVersionDetail[];
}
export type ReviewedSchemaRegistryPort = SchemaRegistryPort & SchemaRegistryReviewPort;
export function boundedSchemaSnapshot(value: unknown, reserveBytes = 0): void {
  if (utf8ByteLength(JSON.stringify(value)) > SCHEMA_CHANGE_LIMITS.snapshotBytes - reserveBytes)
    throw new SchemaChangeReviewError(
      "The Registry snapshot exceeds the 512 KiB review limit. Use a smaller schema or manage this subject outside StreamSkope.",
    );
}
export async function readSchemaChangeBaseline(
  scope: SchemaRegistryReviewScope,
  port: ReviewedSchemaRegistryPort,
  input: SchemaChangeInput,
  signal: AbortSignal,
): Promise<SchemaChangeBaseline> {
  return scope.read(async (context, combined) => {
    const subject = input.draft.subject;
    const before = await port.loadReviewSchema(context, { subject, version: "latest" }, combined);
    const policy = await port.loadCompatibilityPolicy(context, subject, combined);
    const dependencies = new Map<string, SchemaVersionDetail>();
    const baseline = (): SchemaChangeBaseline => ({
      before,
      policy,
      dependencies: [...dependencies.values()].sort(
        (a, b) => a.subject.localeCompare(b.subject, "en-US") || a.version - b.version,
      ),
    });
    const visit = async (schema: SchemaVersionDetail, depth: number): Promise<void> => {
      if (depth > SCHEMA_CHANGE_LIMITS.referenceDepth)
        throw new SchemaChangeReviewError("Reference graph exceeds the review depth.");
      const key = JSON.stringify([schema.subject, schema.version]);
      if (dependencies.has(key)) return;
      if (dependencies.size >= SCHEMA_CHANGE_LIMITS.referenceNodes)
        throw new SchemaChangeReviewError(
          "Registry history and references exceed the review node bound.",
        );
      dependencies.set(key, schema);
      boundedSchemaSnapshot(baseline());
      for (const reference of schema.references) {
        const resolved = await port.loadReviewSchema(context, reference, combined);
        if (!resolved) throw new SchemaChangeReviewError("A pinned reference is unavailable.");
        await visit(resolved, depth + 1);
      }
    };
    if (before) {
      await visit(before, 0);
      if (policy.effectiveLevel.endsWith("_TRANSITIVE")) {
        const history = await port.loadLatestSubject(context, subject, combined);
        if (history.versions.length > SCHEMA_CHANGE_LIMITS.referenceNodes)
          throw new SchemaChangeReviewError("Transitive history exceeds the review bound.");
        for (const version of history.versions) {
          const schema = await port.loadReviewSchema(context, { subject, version }, combined);
          if (!schema)
            throw new SchemaChangeReviewError("A transitive history version is unavailable.");
          await visit(schema, 0);
        }
      }
    }
    for (const reference of input.draft.references) {
      const schema = await port.loadReviewSchema(context, reference, combined);
      if (!schema) throw new SchemaChangeReviewError("A proposed reference is unavailable.");
      await visit(schema, 0);
    }
    boundedSchemaSnapshot(baseline());
    return baseline();
  }, signal);
}
