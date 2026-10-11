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
  readonly value: number;
  readonly sampledAt: number;
}
type Samples =
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
function samples(
  body: unknown,
  input: ExternalMetricsInput,
  evaluatedAt: number,
  expectedName: string,
): Samples {
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
      data.resultType !== "matrix" ||
      !Array.isArray(data.result) ||
      data.result.length > limits.queryRows
    )
      return { state: "invalid", rows: [] };
    const rows = data.result.map((raw: unknown): GaugeRow => {
      const row = object(raw),
        labels = object(row.metric);
      if (labels.__name__ !== expectedName) throw new Error("Prometheus metric name differs.");
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
      if (Object.keys(row).some((key) => key !== "metric" && key !== "values"))
        throw new Error("Unsupported Prometheus row fields.");
      if (
        !Array.isArray(row.values) ||
        row.values.length < 1 ||
        row.values.length > limits.samplesPerSeries
      )
        throw new Error("Prometheus sample inventory is unsupported.");
      let previous = -1;
      const points = row.values.map((point: unknown): GaugeRow => {
        if (
          !Array.isArray(point) ||
          point.length !== 2 ||
          typeof point[0] !== "number" ||
          !Number.isFinite(point[0])
        )
          throw new Error("Invalid Prometheus sample pair.");
        const sampledAt = Math.round(point[0] * 1000);
        if (
          !Number.isSafeInteger(sampledAt) ||
          sampledAt < 0 ||
          sampledAt > 8_640_000_000_000_000 ||
          sampledAt <= previous ||
          sampledAt <= evaluatedAt - limits.sourceWindowMs ||
          sampledAt > evaluatedAt
        )
          throw new Error("Prometheus source samples differ from the requested window.");
        previous = sampledAt;
        return { value: numeric(point[1]), sampledAt };
      });
      // Value and source time belong to one original sample, never separate evaluations.
      return points.at(-1)!;
    });
    return { state: "usable", rows };
  } catch {
    return { state: "invalid", rows: [] };
  }
}
function measurement(
  name: ExternalMetric["name"],
  raw: Samples,
  collectedAt: number,
): ExternalMetric {
  const absent = (
    state: ExternalMetric["state"],
    sampledAt: number | null = null,
  ): ExternalMetric => ({ name, state, value: null, sampledAt });
  if (raw.state !== "usable") return absent(raw.state);
  if (!raw.rows.length) return absent("missing");
  if (raw.rows.length !== 1) return absent("ambiguous");
  const sample = raw.rows[0]!;
  if (!Number.isSafeInteger(sample.value) || sample.value < 0) return absent("invalid");
  const sampledAt = sample.sampledAt;
  if (sampledAt > collectedAt) return absent("future", sampledAt);
  if (collectedAt - sampledAt > limits.staleMs) return absent("stale", sampledAt);
  return { name, state: "ready", value: sample.value, sampledAt };
}

/** Latest original gauge samples from bounded range selectors, through original owned HTTP. */
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
      const read = async (query: string, expectedName: string): Promise<Samples> => {
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
        return samples(response.body, input, evaluatedAt, expectedName);
      };
      // Keep all admitted reads under the original authorization/socket owner and join every close.
      const result = await context.requestOwner.runWork(() =>
        Promise.allSettled(
          EXTERNAL_METRICS.map((metric) =>
            read(`${metric.name}${selectors}[${limits.sourceWindowMs / 1000}s]`, metric.name),
          ),
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
          measurement(metric.name, values[index]!, collectedAt),
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
