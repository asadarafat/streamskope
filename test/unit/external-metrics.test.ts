import { expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import {
  EXTERNAL_METRICS,
  type ExternalMetricsSnapshot,
} from "../../src/features/kafka/contracts/external-metrics";
import {
  parseExternalMetricsInput,
  parseExternalMetricsSnapshot,
} from "../../src/features/kafka/contracts/external-metrics-validation";
import { ExternalMetricsService } from "../../src/features/kafka/application/external-metrics-service";
import type { MetricsReadScope } from "../../src/features/kafka/application/connection-scope";
import type { KafkaClusterServiceContext } from "../../src/features/kafka/application/types";
import { PrometheusMetricsAdapter } from "../../src/features/kafka/engine/prometheus-metrics";
import type {
  BoundedJsonHttpRequest,
  OwnedJsonHttpPort,
} from "../../src/features/kafka/engine/bounded-json-http";
import { OwnedKafkaResources } from "../../src/features/kafka/engine/owned-kafka-resources";

const now = 1_800_000_000_000,
  input = { endpoint: "https://metrics.test", job: 'kafka"\\literal', instance: "broker:9404" };
const snapshot: ExternalMetricsSnapshot = {
  source: "prometheus",
  identity: "not-established",
  ...input,
  evaluatedAt: now,
  collectedAt: now,
  metrics: EXTERNAL_METRICS.map(({ name }) => ({
    name,
    state: "ready",
    value: 0,
    sampledAt: now - 1000,
  })),
};
function context(): KafkaClusterServiceContext {
  const signal = new AbortController().signal;
  return {
    baseUrl: snapshot.endpoint,
    signal,
    requestOwner: new OwnedKafkaResources(signal),
    authorization: () => Promise.resolve("Bearer fixture-private"),
  };
}
function http(body: (request: BoundedJsonHttpRequest) => unknown): OwnedJsonHttpPort {
  return {
    request: vi.fn(() => Promise.reject(new Error("Unowned HTTP must not be used"))),
    open: vi.fn((request: BoundedJsonHttpRequest) => ({
      response: Promise.resolve({ status: 200, body: body(request) }),
      closed: Promise.resolve(),
      close: () => Promise.resolve(),
      dispatched: () => true,
    })),
  };
}
function samples(
  request: BoundedJsonHttpRequest,
  value = "0",
  time = now - 1000,
  extra: Record<string, unknown> = {},
): unknown {
  const url = new URL(request.url),
    query = url.searchParams.get("query")!;
  const name = EXTERNAL_METRICS.find((m) => query.includes(m.name))!.name;
  return {
    status: "success",
    data: {
      resultType: "matrix",
      result: [
        {
          metric: {
            __name__: name,
            job: input.job,
            instance: input.instance,
          },
          values: [[time / 1000, value]],
        },
      ],
    },
    ...extra,
  };
}
it("closes the protocol inventory, source identity, metric units, selectors and coverage states", () => {
  const command = {
    command: "observations.metrics.capture",
    id: "metrics",
    version: HOST_PROTOCOL_VERSION,
    payload: input,
  };
  expect(parseHostCommand(command)).toEqual(command);
  expect(
    parseHostCommandResponse({
      command: command.command,
      id: command.id,
      version: command.version,
      ok: true,
      result: { correlationId: "metrics", snapshot },
    }),
  ).toMatchObject({ result: { snapshot } });
  for (const job of ["", " padded", "x\n", "x\u202ey", "a".repeat(257)])
    expect(() => parseExternalMetricsInput({ ...input, job })).toThrow();
  expect(() => parseExternalMetricsInput({ ...input, query: "up" })).toThrow();
  for (const patch of [
    { identity: "verified" },
    { collectedAt: now - 1 },
    { endpoint: "https://user:secret@metrics.test" },
    { metrics: snapshot.metrics.slice(0, 1) },
  ])
    expect(() => parseExternalMetricsSnapshot({ ...snapshot, ...patch })).toThrow();
  expect(() =>
    parseExternalMetricsSnapshot({
      ...snapshot,
      metrics: [{ ...snapshot.metrics[0], state: "missing", value: 0 }, snapshot.metrics[1]],
    }),
  ).toThrow();
});
it("escapes literal selectors and keeps each value with its original scrape time using only two owned bounded queries", async () => {
  const requests: BoundedJsonHttpRequest[] = [];
  const port = http((r) => {
    requests.push(r);
    return samples(r, "3");
  });
  const result = await new PrometheusMetricsAdapter(port, () => now).capture(
    context(),
    input,
    AbortSignal.timeout(1000),
  );
  expect(result.metrics.map((m) => [m.state, m.value, m.sampledAt])).toEqual([
    ["ready", 3, now - 1000],
    ["ready", 3, now - 1000],
  ]);
  expect(requests).toHaveLength(2);
  for (const request of requests) {
    const url = new URL(request.url);
    expect(url.pathname).toBe("/api/v1/query");
    expect(url.searchParams.get("query")).toContain(
      `{job=${JSON.stringify(input.job)},instance=${JSON.stringify(input.instance)}}`,
    );
    expect(url.searchParams.get("query")).toMatch(/\[300s\]$/u);
    expect(url.searchParams.get("query")).not.toContain("timestamp(");
    expect(url.searchParams.get("limit")).toBe("2");
    expect(url.searchParams.get("time")).toBe(String(now / 1000));
    expect(request.authorization).toBe("Bearer fixture-private");
  }
  expect(JSON.stringify(result)).not.toContain("fixture-private");
});
it("selects the latest original sample without attaching a newer timestamp to an older gauge value", async () => {
  const port = http((request) => {
    const body = samples(request, "0", now - 2000) as {
      data: { result: { values: [number, string][] }[] };
    };
    body.data.result[0]!.values.push([(now - 1000) / 1000, "7"]);
    return body;
  });
  const result = await new PrometheusMetricsAdapter(port, () => now).capture(
    context(),
    input,
    AbortSignal.timeout(1000),
  );
  expect(result.metrics.map((row) => [row.value, row.sampledAt])).toEqual([
    [7, now - 1000],
    [7, now - 1000],
  ]);
});
it.each([
  ["annotated", "0", now - 1000, { warnings: ["remote secret"] }],
  ["annotated", "0", now - 1000, { infos: ["incomplete response"] }],
  ["invalid", "NaN", now - 1000, {}],
  ["invalid", "Inf", now - 1000, {}],
  ["invalid", "0.5", now - 1000, {}],
  ["invalid", "-1", now - 1000, {}],
  ["invalid", "9007199254740992", now - 1000, {}],
  ["stale", "0", now - 45_001, {}],
  ["invalid", "0", now + 1000, {}],
] as const)(
  "keeps %s coverage explicit rather than manufacturing a zero",
  async (state, value, time, extra) => {
    const result = await new PrometheusMetricsAdapter(
      http((r) => samples(r, value, time, extra)),
      () => now,
    ).capture(context(), input, AbortSignal.timeout(1000));
    expect(result.metrics.every((m) => m.state === state && m.value === null)).toBe(true);
    expect(JSON.stringify(result)).not.toContain("remote secret");
    expect(parseExternalMetricsSnapshot(result)).toEqual(result);
  },
);
it.each([
  "missing",
  "ambiguous",
  "wrong labels",
  "wrong name",
  "out of window",
  "scalar",
  "instant vector",
  "histogram",
  "mixed histogram",
  "NaN source time",
  "unordered samples",
  "duplicate source times",
  "empty samples",
  "too many samples",
])("rejects %s instead of treating it as complete evidence", async (kind) => {
  const port = http((r) => {
    const body = samples(r) as {
      data: {
        resultType: string;
        result: { metric: Record<string, string>; values?: unknown; histograms?: unknown }[];
      };
    };
    const row = body.data.result[0]!;
    if (kind === "missing") body.data.result = [];
    if (kind === "ambiguous")
      body.data.result.push({ ...row, metric: { ...row.metric, broker: "another" } });
    if (kind === "wrong labels") row.metric.instance = "another";
    if (kind === "wrong name") row.metric.__name__ = "arbitrary_metric";
    if (kind === "out of window") row.values = [[(now - 300_000) / 1000, "0"]];
    if (kind === "scalar") body.data.resultType = "scalar";
    if (kind === "instant vector") body.data.resultType = "vector";
    if (kind === "mixed histogram") row.histograms = [[now / 1000, {}]];
    if (kind === "NaN source time") row.values = [[NaN, "0"]];
    if (kind === "histogram") {
      delete row.values;
      row.histograms = [[now / 1000, {}]];
    }
    if (kind === "unordered samples")
      row.values = [
        [now / 1000, "0"],
        [(now - 1000) / 1000, "1"],
      ];
    if (kind === "duplicate source times")
      row.values = [
        [now / 1000, "0"],
        [now / 1000, "1"],
      ];
    if (kind === "empty samples") row.values = [];
    if (kind === "too many samples")
      row.values = Array.from({ length: 501 }, (_, i) => [(now - 1000 + i) / 1000, "0"]);
    return body;
  });
  const result = await new PrometheusMetricsAdapter(port, () => now).capture(
    context(),
    input,
    AbortSignal.timeout(1000),
  );
  expect(
    result.metrics.every(
      (m) =>
        m.value === null &&
        m.state === (kind === "missing" || kind === "ambiguous" ? kind : "invalid"),
    ),
  ).toBe(true);
});
it("retains cancellation authority until original settlement and refuses replacement work and late publication", async () => {
  let settle!: (value: ExternalMetricsSnapshot) => void;
  let current = true;
  const scope: MetricsReadScope = {
    connectionName: "original",
    cleanupUnresolved: () => false,
    isCurrent: () => current,
    read: (run, signal) => run(context(), signal),
  };
  const capture = vi.fn(
    () =>
      new Promise<ExternalMetricsSnapshot>((resolve) => {
        settle = resolve;
      }),
  );
  const service = new ExternalMetricsService(() => scope, { capture });
  const operation = service.capture(input);
  const failure = expect(operation).rejects.toThrow();
  service.cancel();
  current = false;
  let idle = false;
  const cleanup = service.idle().then(() => {
    idle = true;
  });
  await expect(service.capture(input)).rejects.toMatchObject({ code: "OBSERVATION_BUSY" });
  await Promise.resolve();
  expect(idle).toBe(false);
  expect(capture).toHaveBeenCalledTimes(1);
  settle(snapshot);
  await failure;
  await cleanup;
  expect(idle).toBe(true);
  await expect(service.capture(input)).rejects.toMatchObject({ code: "UNSUPPORTED_OPERATION" });
});

it("refuses a cancellation acknowledgement and replacement admission while original HTTP cleanup remains unresolved", async () => {
  let unresolved = false;
  const original: MetricsReadScope = {
    connectionName: "original",
    isCurrent: () => true,
    cleanupUnresolved: () => unresolved,
    read: (run, signal) => run(context(), signal),
  };
  const replacement: MetricsReadScope = { ...original, cleanupUnresolved: () => false };
  let owner = original;
  const service = new ExternalMetricsService(() => owner, {
    capture: (): Promise<ExternalMetricsSnapshot> => {
      unresolved = true;
      return Promise.reject(new Error("Original close did not complete"));
    },
  });
  await expect(service.capture(input)).rejects.toThrow("Original close did not complete");
  service.cancel();
  await expect(service.idle()).rejects.toMatchObject({ code: "OBSERVATION_BUSY" });
  owner = replacement;
  await expect(service.capture(input)).rejects.toMatchObject({ code: "OBSERVATION_BUSY" });
  unresolved = false;
  await expect(service.idle()).resolves.toBeUndefined();
});

it("refuses an edited profile endpoint before authorization or HTTP admission on the old connection", async () => {
  const original = context(),
    authorization = vi.fn(() => Promise.resolve("Bearer never-sent"));
  const body = vi.fn((request: BoundedJsonHttpRequest): unknown => samples(request));
  const port = http(body);
  await expect(
    new PrometheusMetricsAdapter(port, () => now).capture(
      { ...original, authorization },
      { ...input, endpoint: "https://replacement.test" },
      AbortSignal.timeout(1000),
    ),
  ).rejects.toMatchObject({ code: "VALIDATION" });
  expect(authorization).not.toHaveBeenCalled();
  expect(body).not.toHaveBeenCalled();
});
