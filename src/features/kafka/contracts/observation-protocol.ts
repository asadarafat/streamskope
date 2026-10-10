import type { HostCommand, HostCommandBase, HostCommandResponse } from "./types";
import { declaredValue, emptyRecord, exactKeys, record, text } from "./validation-primitives";
import {
  parseObservationHistory,
  parseObservationInput,
  parseObservationSeries,
} from "./observation-validation";
import { parseObservationWatch } from "./observation-watch";
import type { ObservationInput } from "./observations";
export type ObservationCommand =
  | (HostCommandBase & {
      readonly command: "observations.capture" | "observations.watch.start";
      readonly payload: ObservationInput;
    })
  | (HostCommandBase & {
      readonly command:
        | "observations.history"
        | "observations.cancel"
        | "observations.watch.status"
        | "observations.watch.stop";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "observations.clear";
      readonly payload: { readonly confirmation: "CLEAR HISTORY" };
    });
export function parseObservationCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "observations.capture" || command === "observations.watch.start")
    return { command, id, version, payload: parseObservationInput(value) };
  if (
    command === "observations.history" ||
    command === "observations.cancel" ||
    command === "observations.watch.status" ||
    command === "observations.watch.stop"
  )
    return { command, id, version, payload: emptyRecord(value, "payload") };
  if (command === "observations.clear") {
    const p = record(value, "payload");
    exactKeys(p, ["confirmation"], "payload");
    return {
      command,
      id,
      version,
      payload: {
        confirmation: declaredValue(p.confirmation, ["CLEAR HISTORY"] as const, "confirmation"),
      },
    };
  }
  return undefined;
}
export function parseObservationResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (
    command === "observations.watch.start" ||
    command === "observations.watch.status" ||
    command === "observations.watch.stop"
  ) {
    exactKeys(result, ["correlationId", "watch"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        watch: parseObservationWatch(result.watch),
      },
    };
  }
  if (command === "observations.capture") {
    exactKeys(result, ["correlationId", "capture"], "result");
    const c = record(result.capture, "capture");
    exactKeys(c, ["series", "durability"], "capture");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        capture: {
          series: parseObservationSeries(c.series),
          durability: declaredValue(c.durability, ["session", "durable"] as const, "durability"),
        },
      },
    };
  }
  if (command === "observations.history" || command === "observations.clear") {
    exactKeys(result, ["correlationId", "snapshot"], "result");
    const s = record(result.snapshot, "snapshot");
    exactKeys(s, ["schemaVersion", "series", "durability"], "snapshot");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "correlationId", 128),
        snapshot: {
          ...parseObservationHistory({ schemaVersion: s.schemaVersion, series: s.series }),
          durability: declaredValue(s.durability, ["session", "durable"] as const, "durability"),
        },
      },
    };
  }
  return undefined;
}
