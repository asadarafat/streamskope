import {
  EXTERNAL_METRICS,
  EXTERNAL_METRIC_LIMITS as limits,
  type ExternalMetric,
  type ExternalMetricsInput,
  type ExternalMetricsSnapshot,
} from "../contracts/external-metrics";
import { parseExternalMetricsInput } from "../contracts/external-metrics-validation";
import { serviceConnectionDiagnostic } from "../application/connection-diagnostics";
import type { ExternalMetricsPort } from "../application/external-metrics-service";
import type { KafkaClusterServiceContext } from "../application/types";

import type { BoundedJsonHttpPort } from "./bounded-json-http";
import { KafkaEngineFailure } from "./failure";
import { readServiceHttp } from "./service-http-request";

class PrometheusHttpError extends Error {
  constructor(readonly status: number) {
    super(`Prometheus HTTP ${status}.`);
  }
}
interface GaugeRow {
  readonly labels: Readonly<Record<string, string>>;
  readonly value: number;
}
type Vector =
  | { readonly state: "usable"; readonly rows: readonly GaugeRow[] }
  | { readonly state: "annotated" | "invalid"; readonly rows: readonly [] };
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid Prometheus response.");
  return value as Record<string, unknown>;
}
function numeric(value: unknown): number {
  if (typeof value !== "string" || !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/u.test(value))
    throw new Error("Invalid Prometheus number.");
  const number = Number(value);
  if (!Number.isFinite(number)) throw new Error("Non-finite Prometheus number.");
  return number;
}
function vector(
  body: unknown,
  input: ExternalMetricsInput,
  evaluatedAt: number,
  expectedName?: string,
): Vector {
  try {
    const envelope = object(body);
    if (envelope.status !== "success") return { state: "invalid", rows: [] };
    for (const key of ["warnings", "infos"] as const) {
      const annotations = envelope[key];
      if (annotations !== undefined && (!Array.isArray(annotations) || annotations.length > 0))
        return { state: "annotated", rows: [] };
    }
    const data = object(envelope.data);
    if (
      data.resultType !== "vector" ||
      !Array.isArray(data.result) ||
      data.result.length > limits.queryRows
    )
      return { state: "invalid", rows: [] };
    const rows = data.result.map((raw: unknown): GaugeRow => {
      const row = object(raw),
        labels = object(row.metric);
      if (
        expectedName === undefined
          ? labels.__name__ !== undefined
          : labels.__name__ !== expectedName
      )
        throw new Error("Prometheus metric name differs.");
      if (
        Object.keys(labels).length > limits.labels ||
        labels.job !== input.job ||
        labels.instance !== input.instance ||
        Object.entries(labels).some(
          ([key, value]) =>
            key.length > 128 ||
            typeof value !== "string" ||
            value.length > limits.selectorCharacters,
        )
      )
        throw new Error("Invalid Prometheus labels.");
      if (Object.keys(row).some((key) => key !== "metric" && key !== "value"))
        throw new Error("Unsupported Prometheus row fields.");
      if (
        !Array.isArray(row.value) ||
        row.value.length !== 2 ||
        typeof row.value[0] !== "number" ||
        !Number.isFinite(row.value[0]) ||
        Math.abs(row.value[0] * 1000 - evaluatedAt) > 1
      )
        throw new Error("Prometheus evaluation time differs.");
      const value = numeric(row.value[1]);
      // timestamp() removes __name__; no arbitrary remote labels leave the host.
      const normalized = Object.fromEntries(
        Object.entries(labels).filter(([key]) => key !== "__name__"),
      ) as Record<string, string>;
      return { labels: normalized, value };
    });
    return { state: "usable", rows };
  } catch {
    return { state: "invalid", rows: [] };
  }
}
function sameLabels(left: GaugeRow, right: GaugeRow): boolean {
  const keys = Object.keys(left.labels);
  return (
    keys.length === Object.keys(right.labels).length &&
    keys.every((key) => left.labels[key] === right.labels[key])
  );
}
function measurement(
  name: ExternalMetric["name"],
  raw: Vector,
  times: Vector,
  collectedAt: number,
): ExternalMetric {
  const absent = (
    state: ExternalMetric["state"],
    scrapedAt: number | null = null,
  ): ExternalMetric => ({ name, state, value: null, scrapedAt });
  if (raw.state !== "usable" || times.state !== "usable")
    return absent(
      raw.state === "annotated" || times.state === "annotated" ? "annotated" : "invalid",
    );
  if (!raw.rows.length || !times.rows.length) return absent("missing");
  if (raw.rows.length !== 1 || times.rows.length !== 1) return absent("ambiguous");
  const sample = raw.rows[0]!,
    stamp = times.rows[0]!;
  if (
    !sameLabels(sample, stamp) ||
    !Number.isSafeInteger(sample.value) ||
    sample.value < 0 ||
    stamp.value < 0
  )
    return absent("invalid");
  const scrapedAt = Math.round(stamp.value * 1000);
  if (!Number.isSafeInteger(scrapedAt) || scrapedAt > 8_640_000_000_000_000)
    return absent("invalid");
  if (scrapedAt > collectedAt) return absent("future", scrapedAt);
  if (collectedAt - scrapedAt > limits.staleMs) return absent("stale", scrapedAt);
  return { name, state: "ready", value: sample.value, scrapedAt };
}

/** Declared instant gauges paired with actual scrape timestamps, through original owned HTTP. */
export class PrometheusMetricsAdapter implements ExternalMetricsPort {
  constructor(
    private readonly http: BoundedJsonHttpPort,
    private readonly now = Date.now,
  ) {}
  async capture(
    context: KafkaClusterServiceContext,
    input: ExternalMetricsInput,
    signal: AbortSignal,
  ): Promise<ExternalMetricsSnapshot> {
    input = parseExternalMetricsInput(input);
    const evaluatedAt = this.now();
    try {
      if (context.baseUrl !== input.endpoint)
        throw new KafkaEngineFailure({
          code: "VALIDATION",
          stage: "validation",
          summary: "The profile metrics URL differs from the connected service.",
          recovery: "Reconnect the edited profile before reading its selected exporter.",
          retryable: false,
          target: "metrics",
        });
      if (!context.requestOwner) throw new Error("Original HTTP request ownership is unavailable.");
      const selectors = `{job=${JSON.stringify(input.job)},instance=${JSON.stringify(input.instance)}}`;
      const read = async (query: string, expectedName?: string): Promise<Vector> => {
        const params = new URLSearchParams({
          query,
          time: String(evaluatedAt / 1000),
          timeout: "4s",
          limit: String(limits.queryRows),
        });
        const response = await readServiceHttp(
          this.http,
          context,
          signal,
          "GET",
          `/api/v1/query?${params.toString()}`,
        );
        if (response.status < 200 || response.status >= 300)
          throw new PrometheusHttpError(response.status);
        return vector(response.body, input, evaluatedAt, expectedName);
      };
      // Keep all admitted reads under the original authorization/socket owner and join every close.
      const result = await context.requestOwner.runWork(() =>
        Promise.allSettled(
          EXTERNAL_METRICS.flatMap((metric) => [
            read(metric.name + selectors, metric.name),
            read(`timestamp(${metric.name + selectors})`),
          ]),
        ),
      );
      const failures = result.flatMap((entry) =>
        entry.status === "rejected" ? [entry.reason as unknown] : [],
      );
      if (failures.length) throw new AggregateError(failures, "Prometheus read failed.");
      signal.throwIfAborted();
      const values = result.map((entry) => {
        if (entry.status === "rejected") throw new Error("Prometheus result unavailable.");
        return entry.value;
      });
      const collectedAt = this.now();
      if (collectedAt < evaluatedAt)
        throw new Error("Local metrics clock changed during collection.");
      return {
        source: "prometheus",
        identity: "not-established",
        ...input,
        evaluatedAt,
        collectedAt,
        metrics: EXTERNAL_METRICS.map((metric, index) =>
          measurement(metric.name, values[index * 2]!, values[index * 2 + 1]!, collectedAt),
        ),
      };
    } catch (cause) {
      if (cause instanceof KafkaEngineFailure) throw cause;
      throw new KafkaEngineFailure({
        ...(serviceConnectionDiagnostic(cause, "Prometheus") ?? {
          code: "HTTPS_RESPONSE",
          stage: "backend",
          retryable: false,
          summary: "Prometheus did not complete the bounded metrics read.",
          recovery:
            "Verify its API URL and declared exporter configuration. If original HTTP cleanup remains unresolved, reconnect only after cleanup is confirmed.",
        }),
        cause,
        target: "metrics",
      });
    }
  }
}
