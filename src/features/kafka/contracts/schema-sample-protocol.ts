import type { HostCommand, HostCommandResponse } from "./types";
import { exactKeys, text, record } from "./validation-primitives";
import {
  parseSchemaSampleInput,
  parseRecordBatchInput,
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

export function parseSampleCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  switch (command) {
    case "schemas.samples":
      return { command, id, version, payload: parseSchemaSampleInput(value) };
    case "records.batch.review":
      return { command, id, version, payload: parseRecordBatchInput(value) };
    case "records.batch.apply":
    case "records.batch.cancel": {
      const payload = record(value, "command.payload");
      exactKeys(payload, ["planId"], "command.payload");
      return {
        command,
        id,
        version,
        payload: { planId: text(payload.planId, "command.payload.planId", 128) },
      };
    }
  }
  return undefined;
}
