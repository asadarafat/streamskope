import type { HostCommand, HostCommandResponse } from "./types";
import { record, exactKeys, text } from "./validation-primitives";
import { parseCorrelationTraceInput, parseCorrelationTraceResult } from "./correlation-trace";

export function parseCorrelationCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "records.trace")
    return { command, id, version, payload: parseCorrelationTraceInput(value) };
  if (command !== "records.trace.cancel") return undefined;
  const payload = record(value, "traceCancel");
  exactKeys(payload, ["traceId"], "traceCancel");
  return {
    command,
    id,
    version,
    payload: { traceId: text(payload.traceId, "traceCancel.traceId", 128) },
  };
}
export function parseCorrelationResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (command !== "records.trace") return undefined;
  exactKeys(result, ["correlationId", "trace"], "traceResult");
  return {
    command,
    id,
    version,
    ok: true,
    result: {
      correlationId: text(result.correlationId, "traceResult.correlationId", 128),
      trace: parseCorrelationTraceResult(result.trace),
    },
  };
}
