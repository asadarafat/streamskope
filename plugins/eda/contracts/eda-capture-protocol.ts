import {
  exactKeys,
  record,
  text,
} from "../../../src/features/kafka/contracts/validation-primitives";

import {
  parseEdaCaptureDeployInput,
  parseEdaCaptureApplicationInput,
  parseEdaCaptureApplicationStatus,
  parseEdaCaptureDeployment,
  parseEdaCaptureInspectInput,
  parseEdaCaptureInspection,
  parseEdaCaptureProgress,
} from "./eda-capture-validation";
import type {
  HostCommand,
  HostCommandName,
  HostCommandResponse,
  HostEvent,
  HostEventName,
} from "./types";
import { parseEdaCaptureHostStatus, parseEdaCaptureSessionStatus } from "./eda-capture-lifecycle";
import { parseProfileSource } from "./profile-validation";

export function parseEdaCaptureHostEvent(
  event: HostEventName,
  payload: unknown,
  sequence: number,
  version: number,
): HostEvent | undefined {
  return event === "edaCapture.progress"
    ? {
        event,
        payload: parseEdaCaptureProgress(payload, "event.payload"),
        sequence,
        version,
      }
    : undefined;
}

export function parseEdaCaptureHostCommand(
  command: HostCommandName,
  id: string,
  payload: unknown,
  version: number,
): HostCommand | undefined {
  if (command === "edaCapture.application.status" || command === "edaCapture.application.install") {
    return {
      command,
      id,
      payload: parseEdaCaptureApplicationInput(payload, "command.payload"),
      version,
    };
  }
  if (command === "edaCapture.preflight" || command === "edaCapture.status") {
    exactKeys(record(payload, "command.payload"), [], "command.payload");
    return { command, id, payload: {}, version };
  }
  if (command === "edaCapture.cancel") {
    const input = record(payload, "command.payload");
    exactKeys(input, ["requestId"], "command.payload");
    return {
      command,
      id,
      payload: { requestId: text(input.requestId, "command.payload.requestId", 128) },
      version,
    };
  }
  if (command === "edaCapture.stop" || command === "edaCapture.remove") {
    const input = record(payload, "command.payload");
    exactKeys(input, ["source"], "command.payload");
    return {
      command,
      id,
      payload: { source: parseProfileSource(input.source, "command.payload.source") },
      version,
    };
  }
  if (command === "edaCapture.inspect") {
    return {
      command,
      id,
      payload: parseEdaCaptureInspectInput(payload, "command.payload"),
      version,
    };
  }
  if (command === "edaCapture.deploy") {
    return {
      command,
      id,
      payload: parseEdaCaptureDeployInput(payload, "command.payload"),
      version,
    };
  }
  return undefined;
}

export function parseEdaCaptureHostResponse(
  command: HostCommandName,
  id: string,
  value: unknown,
  version: number,
): HostCommandResponse | undefined {
  if (command === "edaCapture.application.status" || command === "edaCapture.application.install") {
    const result = record(value, "response.result");
    exactKeys(result, ["application", "correlationId"], "response.result");
    return {
      command,
      id,
      ok: true,
      result: {
        application: parseEdaCaptureApplicationStatus(
          result.application,
          "response.result.application",
        ),
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
      },
      version,
    };
  }
  if (
    command === "edaCapture.preflight" ||
    command === "edaCapture.status" ||
    command === "edaCapture.stop" ||
    command === "edaCapture.remove"
  ) {
    const result = record(value, "response.result");
    const preflight = command === "edaCapture.preflight";
    exactKeys(
      result,
      ["correlationId", preflight ? "captureHost" : "captureSession"],
      "response.result",
    );
    const correlationId = text(result.correlationId, "response.result.correlationId", 128);
    if (command === "edaCapture.preflight")
      return {
        command,
        id,
        ok: true,
        version,
        result: { correlationId, captureHost: parseEdaCaptureHostStatus(result.captureHost) },
      };
    return {
      command,
      id,
      ok: true,
      version,
      result: {
        correlationId,
        captureSession: parseEdaCaptureSessionStatus(result.captureSession),
      },
    };
  }

  if (command !== "edaCapture.inspect" && command !== "edaCapture.deploy") return undefined;
  const result = record(value, "response.result");
  exactKeys(
    result,
    command === "edaCapture.inspect"
      ? ["correlationId", "inspection"]
      : ["correlationId", "deployment"],
    "response.result",
  );
  const correlationId = text(result.correlationId, "response.result.correlationId", 128);
  return command === "edaCapture.inspect"
    ? {
        command,
        id,
        ok: true,
        result: {
          correlationId,
          inspection: parseEdaCaptureInspection(result.inspection, "response.result.inspection"),
        },
        version,
      }
    : {
        command,
        id,
        ok: true,
        result: {
          correlationId,
          deployment: parseEdaCaptureDeployment(result.deployment, "response.result.deployment"),
        },
        version,
      };
}
