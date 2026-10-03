import type { HostCommand, HostCommandResponse } from "./types";
import { exactKeys, text } from "./validation-primitives";
import {
  parseSchemaSamples,
  parseRecordBatchReview,
  parseRecordBatchOutcome,
} from "./schema-samples";

export function parseSampleResponse(
  command: HostCommand["command"],
  id: string,
  version: HostCommand["version"],
  result: Record<string, unknown>,
): HostCommandResponse | undefined {
  if (command === "schemas.samples") {
    exactKeys(result, ["correlationId", "samples"], "response.result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        samples: parseSchemaSamples(result.samples),
      },
    };
  }
  if (command === "records.batch.review") {
    exactKeys(result, ["correlationId", "review"], "response.result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        review: parseRecordBatchReview(result.review),
      },
    };
  }
  if (command === "records.batch.apply") {
    exactKeys(result, ["correlationId", "outcome"], "response.result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        outcome: parseRecordBatchOutcome(result.outcome),
      },
    };
  }
  return undefined;
}
