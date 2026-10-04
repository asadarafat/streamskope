import type { KafkaStreamMonitorSnapshot } from "../contracts";
import type { RendererStreamMonitorSnapshot } from "./stream-monitor-observer";
import type { StatusIndicatorTone } from "./StatusIndicator";

export const MONITOR_STALE_AFTER_MS = 5_000;
export const MONITOR_TIME_WINDOWS = [30, 60, 300] as const;

export function monitorNumber(value: number): string {
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export function monitorBytes(value: number): string {
  if (value >= 1_048_576) return `${monitorNumber(value / 1_048_576)} MiB`;
  if (value >= 1_024) return `${monitorNumber(value / 1_024)} KiB`;
  return `${monitorNumber(value)} B`;
}

export function monitorValue(value: number | null | undefined, unit = ""): string {
  return value === null || value === undefined
    ? "Unavailable"
    : `${monitorNumber(value)}${unit ? ` ${unit}` : ""}`;
}

export function measurementAge(sampledAt: string | null, now: number): string {
  if (sampledAt === null) return "Not measured";
  const elapsed = Math.max(0, now - Date.parse(sampledAt));
  if (!Number.isFinite(elapsed)) return "Unknown measurement time";
  if (elapsed < 1_000) return "Measured just now";
  if (elapsed < 60_000) return `Measured ${Math.floor(elapsed / 1_000)}s ago`;
  if (elapsed < 3_600_000) return `Measured ${Math.floor(elapsed / 60_000)}m ago`;
  return `Measured ${Math.floor(elapsed / 3_600_000)}h ago`;
}

export function monitorIsActive(snapshot: KafkaStreamMonitorSnapshot): boolean {
  return (
    ["loading", "fetching", "streaming"].includes(snapshot.state) ||
    (snapshot.state === "empty" && snapshot.request?.mode === "tail")
  );
}

export function monitorStatus(
  snapshot: KafkaStreamMonitorSnapshot,
  now: number,
): {
  readonly label: string;
  readonly explanation: string;
  readonly tone: StatusIndicatorTone;
  readonly stale: boolean;
} {
  const stale =
    snapshot.state === "stale" ||
    snapshot.status === "stale" ||
    (monitorIsActive(snapshot) &&
      snapshot.sampledAt !== null &&
      now - Date.parse(snapshot.sampledAt) > MONITOR_STALE_AFTER_MS);
  if (stale)
    return {
      label: "Stale evidence",
      explanation:
        "Current delivery cannot be confirmed. The measurements below are retained evidence.",
      tone: "warning",
      stale: true,
    };
  if (snapshot.state === "unavailable")
    return {
      label: "Unavailable",
      explanation: "Start a message request to collect delivery measurements.",
      tone: "neutral",
      stale: false,
    };
  if (snapshot.state === "failed")
    return {
      label: "Failed",
      explanation: "The message request failed. Measurements show its last observed state.",
      tone: "error",
      stale: false,
    };
  if (!monitorIsActive(snapshot))
    return {
      label:
        snapshot.state === "stopped"
          ? "Stopped"
          : snapshot.state === "empty"
            ? "Empty"
            : "Complete",
      explanation: "This request has ended. Charts and totals show historical evidence.",
      tone: "neutral",
      stale: false,
    };
  const reasons = snapshot.queue?.pressureReasons ?? [];
  if (reasons.length > 0)
    return {
      label: "Buffer pressure",
      explanation: reasons
        .map((reason) =>
          reason === "transport"
            ? "Display transport is paused"
            : reason === "count-capacity"
              ? "Message capacity reached"
              : "Byte capacity nearly reached",
        )
        .join(" · "),
      tone: "warning",
      stale: false,
    };
  if (snapshot.state === "loading" || snapshot.state === "fetching")
    return {
      label: "Fetching",
      explanation: "The message request is active; measurements appear as records arrive.",
      tone: "info",
      stale: false,
    };
  return {
    label: snapshot.status === "idle" ? "Waiting for records" : "Delivering",
    explanation: "No current host buffer pressure observed.",
    tone: "success",
    stale: false,
  };
}

export function monitorWindow(
  snapshot: KafkaStreamMonitorSnapshot,
  now: number,
  seconds: number,
): readonly [string, string] {
  const end =
    monitorIsActive(snapshot) && !monitorStatus(snapshot, now).stale
      ? now
      : Date.parse(snapshot.sampledAt ?? "") || now;
  return [new Date(end - seconds * 1_000).toISOString(), new Date(end).toISOString()];
}

export function withinMonitorWindow(
  sampledAt: string | null,
  window: readonly [string, string],
): boolean {
  if (sampledAt === null) return false;
  const time = Date.parse(sampledAt);
  return time >= Date.parse(window[0]) && time <= Date.parse(window[1]);
}

export function scopedHostHistory(
  snapshot: KafkaStreamMonitorSnapshot,
  history: readonly KafkaStreamMonitorSnapshot[],
): readonly KafkaStreamMonitorSnapshot[] {
  const scoped = history.filter(
    (sample) =>
      sample.operationId === snapshot.operationId &&
      sample.connectionName === snapshot.connectionName &&
      sample.request?.topic === snapshot.request?.topic,
  );
  return scoped.includes(snapshot) ? scoped : [...scoped, snapshot];
}

export function monitorRateSamples(
  history: readonly KafkaStreamMonitorSnapshot[],
  window: readonly [string, string],
): readonly { readonly sampledAt: string; readonly value: number | null }[] {
  const samples = new Map<string, number | null>();
  for (const sample of history) {
    const timestamp = sample.delivery?.rateSampledAt ?? null;
    if (timestamp !== null && withinMonitorWindow(timestamp, window))
      samples.set(timestamp, sample.delivery?.messagesPerSecond ?? null);
  }
  return [...samples]
    .map(([sampledAt, value]) => ({ sampledAt, value }))
    .sort((a, b) => a.sampledAt.localeCompare(b.sampledAt));
}

export function monitorLossRows(
  snapshot: KafkaStreamMonitorSnapshot,
  renderer: RendererStreamMonitorSnapshot,
): readonly { readonly label: string; readonly value: string }[] {
  const reasons = snapshot.queue?.dropReasons;
  return [
    { label: "Message capacity omissions", value: monitorValue(reasons?.countCapacity) },
    { label: "Byte capacity omissions", value: monitorValue(reasons?.byteCapacity) },
    { label: "Oversized records", value: monitorValue(reasons?.oversized) },
    { label: "Discarded when request ended", value: monitorValue(reasons?.terminalDiscarded) },
    {
      label: "Renderer overload omissions",
      value:
        renderer.operationId === snapshot.operationId
          ? monitorValue(renderer.rendererDroppedMessages)
          : "Unavailable",
    },
    {
      label: "Display retention evictions",
      value:
        renderer.operationId === snapshot.operationId
          ? monitorValue(renderer.rendererWindowEvictions)
          : "Unavailable",
    },
  ];
}
