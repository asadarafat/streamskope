import {
  REDPANDA_TRANSFORM_LIMITS,
  REDPANDA_TRANSFORM_STATUSES,
  summarizeRedpandaTransformStatuses,
  type RedpandaTransformSummary,
} from "../contracts";
import type { KafkaClusterServiceContext } from "../application";

import type { BoundedJsonHttpPort, BoundedJsonHttpResponse } from "./bounded-json-http";

type UnknownRecord = Record<string, unknown>;

export class RedpandaTransformResponseError extends Error {
  constructor(
    readonly status: number | null,
    message = "Redpanda Admin API returned invalid transform data.",
  ) {
    super(message);
    this.name = "RedpandaTransformResponseError";
  }
}

function record(value: unknown): UnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new RedpandaTransformResponseError(null);
  return value as UnknownRecord;
}

function boundedString(value: unknown): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > REDPANDA_TRANSFORM_LIMITS.fieldCharacters
  )
    throw new RedpandaTransformResponseError(null);
  return value;
}

function nonNegativeInteger(value: unknown): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0)
    throw new RedpandaTransformResponseError(null);
  return Number(value);
}

function successful(response: BoundedJsonHttpResponse): unknown {
  if (response.status < 200 || response.status >= 300)
    throw new RedpandaTransformResponseError(
      response.status,
      `Redpanda Admin API returned HTTP ${String(response.status)}.`,
    );
  return response.body;
}

function stringArray(value: unknown, maximum: number): readonly string[] {
  if (!Array.isArray(value) || value.length > maximum)
    throw new RedpandaTransformResponseError(null);
  return value.map(boundedString);
}

function offsetString(value: unknown): string {
  if (typeof value === "string") return boundedString(value);
  if (Number.isSafeInteger(value)) return String(value);
  throw new RedpandaTransformResponseError(null);
}

function transformOffset(value: unknown): RedpandaTransformSummary["offset"] {
  if (value === undefined || value === null) return null;
  const parsed = record(value);
  return { format: boundedString(parsed.format), value: offsetString(parsed.value) };
}

function transformSummary(value: unknown): RedpandaTransformSummary {
  const item = record(value);
  const statusValues = item.status ?? [];
  const environmentValues = item.environment ?? [];
  if (!Array.isArray(statusValues) || !Array.isArray(environmentValues))
    throw new RedpandaTransformResponseError(null);
  const environment = environmentValues.map((entry) => {
    const variable = record(entry);
    return {
      name: boundedString(variable.key),
      valuePresent: typeof variable.value === "string" && variable.value.length > 0,
    };
  });
  if (environment.length > REDPANDA_TRANSFORM_LIMITS.environmentNames)
    throw new RedpandaTransformResponseError(null);
  const statuses: RedpandaTransformSummary["statuses"] = statusValues.map((entry) => {
    const status = record(entry);
    const statusName = boundedString(status.status);
    if (!REDPANDA_TRANSFORM_STATUSES.includes(statusName as never))
      throw new RedpandaTransformResponseError(null);
    return {
      lag: nonNegativeInteger(status.lag),
      nodeId: nonNegativeInteger(status.node_id),
      partition: nonNegativeInteger(status.partition),
      status: statusName as RedpandaTransformSummary["statuses"][number]["status"],
    };
  });
  return {
    ...summarizeRedpandaTransformStatuses(statuses),
    compression: item.compression === undefined ? "none" : boundedString(item.compression),
    environment: [...environment].sort((left, right) =>
      left.name.localeCompare(right.name, "en-US"),
    ),
    inputTopic: boundedString(item.input_topic),
    name: boundedString(item.name),
    offset: transformOffset(item.offset),
    outputTopics: stringArray(item.output_topics, REDPANDA_TRANSFORM_LIMITS.outputTopics),
    statuses,
  };
}

function serviceUrl(context: KafkaClusterServiceContext, path: string): string {
  return `${context.baseUrl.replace(/\/+$/u, "")}${path}`;
}

export class RedpandaTransformHttpAdapter {
  constructor(private readonly http: BoundedJsonHttpPort) {}

  async list(
    context: KafkaClusterServiceContext,
    signal: AbortSignal,
  ): Promise<readonly RedpandaTransformSummary[]> {
    const body = successful(await this.request(context, signal, "GET", "/v1/transform/"));
    if (!Array.isArray(body) || body.length > REDPANDA_TRANSFORM_LIMITS.transforms)
      throw new RedpandaTransformResponseError(null);
    return body
      .map(transformSummary)
      .sort((left, right) => left.name.localeCompare(right.name, "en-US"));
  }

  async delete(
    context: KafkaClusterServiceContext,
    name: string,
    signal: AbortSignal,
  ): Promise<void> {
    successful(
      await this.request(context, signal, "DELETE", `/v1/transform/${encodeURIComponent(name)}`),
    );
  }

  private async request(
    context: KafkaClusterServiceContext,
    signal: AbortSignal,
    method: "DELETE" | "GET",
    path: string,
  ): Promise<BoundedJsonHttpResponse> {
    const authorization = await context.authorization();
    return this.http.request({
      ...(authorization === undefined ? {} : { authorization }),
      caPem: context.caPem,
      method,
      signal,
      url: serviceUrl(context, path),
    });
  }
}
