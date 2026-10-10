import {
  EXTERNAL_METRICS,
  EXTERNAL_METRIC_LIMITS,
  EXTERNAL_METRIC_STATES,
  type ExternalMetricsInput,
  type ExternalMetricsSnapshot,
} from "./external-metrics";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";

function endpointUrl(value: unknown): string {
  const endpoint = text(value, "metrics.endpoint", 2048);
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    throw new HostContractValidationError(
      "metrics.endpoint",
      "must be an absolute HTTP(S) service URL",
    );
  }
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    throw new HostContractValidationError(
      "metrics.endpoint",
      "must be an HTTP(S) service URL without credentials, query or fragment",
    );
  return endpoint;
}

export function parseExternalMetricsInput(value: unknown): ExternalMetricsInput {
  const input = record(value, "metrics");
  exactKeys(input, ["endpoint", "job", "instance"], "metrics");
  const label = (value: unknown, path: string): string => {
    const result = text(value, path, EXTERNAL_METRIC_LIMITS.selectorCharacters);
    if (
      !result.trim() ||
      result !== result.trim() ||
      // eslint-disable-next-line no-control-regex -- Literal selectors cannot contain control or direction bytes.
      /[\u0000-\u001f\u007f\u200e\u200f\u202a-\u202e\u2066-\u2069]/u.test(result)
    )
      throw new HostContractValidationError(
        path,
        "use a non-empty literal label without surrounding whitespace or control characters",
      );
    return result;
  };
  return {
    endpoint: endpointUrl(input.endpoint),
    job: label(input.job, "metrics.job"),
    instance: label(input.instance, "metrics.instance"),
  };
}

function timestamp(value: unknown, path: string): number {
  const result = nonNegativeInteger(value, path);
  if (result > 8_640_000_000_000_000)
    throw new HostContractValidationError(path, "is outside the supported date range");
  return result;
}

export function parseExternalMetricsSnapshot(value: unknown): ExternalMetricsSnapshot {
  const s = record(value, "metrics");
  exactKeys(
    s,
    ["source", "endpoint", "identity", "job", "instance", "evaluatedAt", "collectedAt", "metrics"],
    "metrics",
  );
  const selectors = parseExternalMetricsInput({
    endpoint: s.endpoint,
    job: s.job,
    instance: s.instance,
  });
  const evaluatedAt = timestamp(s.evaluatedAt, "metrics.evaluatedAt");
  const collectedAt = timestamp(s.collectedAt, "metrics.collectedAt");
  if (
    collectedAt < evaluatedAt ||
    !Array.isArray(s.metrics) ||
    s.metrics.length !== EXTERNAL_METRICS.length
  )
    throw new HostContractValidationError(
      "metrics",
      "requires a complete declared gauge inventory and valid collection interval",
    );
  const metrics = s.metrics.map((raw: unknown, index) => {
    const m = record(raw, "metric");
    exactKeys(m, ["name", "state", "value", "scrapedAt"], "metric");
    const name = declaredValue(m.name, [EXTERNAL_METRICS[index]!.name], "metric.name");
    const state = declaredValue(m.state, EXTERNAL_METRIC_STATES, "metric.state");
    const value = m.value === null ? null : nonNegativeInteger(m.value, "metric.value");
    const scrapedAt = m.scrapedAt === null ? null : timestamp(m.scrapedAt, "metric.scrapedAt");
    if (
      (state === "ready") !== (value !== null) ||
      (value !== null && scrapedAt === null) ||
      ((state === "stale" || state === "future") && scrapedAt === null) ||
      (state === "ready" &&
        (scrapedAt! > collectedAt || collectedAt - scrapedAt! > EXTERNAL_METRIC_LIMITS.staleMs)) ||
      (state === "stale" && collectedAt - scrapedAt! <= EXTERNAL_METRIC_LIMITS.staleMs) ||
      (state === "future" && scrapedAt! <= collectedAt)
    )
      throw new HostContractValidationError(
        "metric",
        "value, source time and coverage state must agree",
      );
    return { name, state, value, scrapedAt };
  });
  return {
    source: declaredValue(s.source, ["prometheus"], "metrics.source"),
    identity: declaredValue(s.identity, ["not-established"], "metrics.identity"),
    ...selectors,
    evaluatedAt,
    collectedAt,
    metrics,
  };
}
