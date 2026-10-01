import {
  declaredValue,
  exactKeys,
  optionalText,
  record,
  text,
} from "../../../src/features/kafka/contracts/validation-primitives";

import type { ProfileEdaCaptureSource } from "./profile-types";
import { parseProfileSource } from "./profile-validation";

export interface EdaCaptureHostStatus {
  readonly state: "configured" | "unavailable";
  readonly detail: string;
  readonly context?: string;
  readonly edaApiUrl?: string;
}

export interface EdaCaptureSessionStatus {
  readonly state: "idle" | "starting" | "ready" | "stopped" | "failed" | "cancelled";
  readonly tunnel: "open" | "closed";
  readonly detail: string;
  readonly source?: ProfileEdaCaptureSource;
  readonly verifiedAt?: string;
}

export function sameEdaCaptureSource(
  left: ProfileEdaCaptureSource,
  right: ProfileEdaCaptureSource,
): boolean {
  return (
    left.edaApiUrl !== undefined &&
    left.context !== undefined &&
    left.edaApiUrl === right.edaApiUrl &&
    left.context === right.context &&
    left.source.apiVersion === right.source.apiVersion &&
    left.source.kind === right.source.kind &&
    left.source.namespace === right.source.namespace &&
    left.source.name === right.source.name
  );
}

export function parseEdaCaptureHostStatus(value: unknown): EdaCaptureHostStatus {
  const input = record(value, "captureHost");
  exactKeys(input, ["state", "detail", "context", "edaApiUrl"], "captureHost");
  return {
    state: declaredValue(input.state, ["configured", "unavailable"], "captureHost.state"),
    detail: text(input.detail, "captureHost.detail", 4096),
    ...(input.context === undefined
      ? {}
      : { context: text(input.context, "captureHost.context", 253) }),
    ...(input.edaApiUrl === undefined
      ? {}
      : { edaApiUrl: text(input.edaApiUrl, "captureHost.edaApiUrl", 2048) }),
  };
}

export function parseEdaCaptureSessionStatus(value: unknown): EdaCaptureSessionStatus {
  const input = record(value, "captureSession");
  exactKeys(input, ["state", "tunnel", "detail", "source", "verifiedAt"], "captureSession");
  const verifiedAt = optionalText(input, "verifiedAt", "captureSession", 64);
  return {
    state: declaredValue(
      input.state,
      ["idle", "starting", "ready", "stopped", "failed", "cancelled"],
      "captureSession.state",
    ),
    tunnel: declaredValue(input.tunnel, ["open", "closed"], "captureSession.tunnel"),
    detail: text(input.detail, "captureSession.detail", 4096),
    ...(input.source === undefined
      ? {}
      : { source: parseProfileSource(input.source, "captureSession.source") }),
    ...(verifiedAt === undefined ? {} : { verifiedAt }),
  };
}
