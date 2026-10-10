import type { SchemaPolicyChange } from "../contracts/schema-policy";
import {
  SCHEMA_REGISTRY_LIMITS,
  SCHEMA_REGISTRY_TYPES,
  type SchemaCompatibilityCheckInput,
  type SchemaDeletionInput,
  type SchemaReference,
  type SchemaRegistrationInput,
  type SchemaSubjectVersionIdentity,
  type SchemaVersionDetail,
} from "../contracts";
import type { KafkaClusterServiceContext } from "../application";
import type {
  SchemaRegistryCompatibilityResult,
  SchemaRegistrySubjectDetail,
  SchemaRegistrySubjectInventory,
} from "../application/schema-registry-types";
import type { RegisteredSchema } from "../application/record-codec-types";
import {
  SCHEMA_COMPATIBILITY_LEVELS,
  type SchemaCompatibilityLevel,
  type SchemaCompatibilityPolicy,
} from "../contracts/schema-changes";

import type { BoundedJsonHttpPort, BoundedJsonHttpResponse } from "./bounded-json-http";

type UnknownRecord = Record<string, unknown>;

export class SchemaRegistryResponseError extends Error {
  constructor(
    readonly status: number | null,
    message = "Schema Registry returned invalid data.",
  ) {
    super(message);
    this.name = "SchemaRegistryResponseError";
  }
}

function record(value: unknown): UnknownRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new SchemaRegistryResponseError(null);
  }
  return value as UnknownRecord;
}

function boundedString(
  value: unknown,
  maximum: number = SCHEMA_REGISTRY_LIMITS.subjectCharacters,
): string {
  if (typeof value !== "string" || value.length === 0 || value.length > maximum) {
    throw new SchemaRegistryResponseError(null);
  }
  return value;
}

function positiveInteger(value: unknown): number {
  if (!Number.isSafeInteger(value) || Number(value) < 1) {
    throw new SchemaRegistryResponseError(null);
  }
  return Number(value);
}

function schemaReference(value: unknown): SchemaReference {
  const item = record(value);
  return {
    name: boundedString(item.name),
    subject: boundedString(item.subject),
    version: positiveInteger(item.version),
  };
}

function schemaVersion(value: unknown): SchemaVersionDetail {
  const item = record(value);
  const references = item.references ?? [];
  if (!Array.isArray(references) || references.length > SCHEMA_REGISTRY_LIMITS.references) {
    throw new SchemaRegistryResponseError(null);
  }
  const schemaType = item.schemaType ?? "AVRO";
  if (typeof schemaType !== "string" || !SCHEMA_REGISTRY_TYPES.includes(schemaType as never)) {
    throw new SchemaRegistryResponseError(null);
  }
  return {
    id: positiveInteger(item.id),
    references: references.map(schemaReference),
    schema: boundedString(item.schema, SCHEMA_REGISTRY_LIMITS.schemaCharacters),
    schemaType: schemaType as SchemaVersionDetail["schemaType"],
    subject: boundedString(item.subject),
    version: positiveInteger(item.version),
  };
}

function successful(response: BoundedJsonHttpResponse): unknown {
  if (response.status < 200 || response.status >= 300) {
    throw new SchemaRegistryResponseError(
      response.status,
      `Schema Registry returned HTTP ${String(response.status)}.`,
    );
  }
  return response.body;
}

function serviceUrl(context: KafkaClusterServiceContext, path: string): string {
  return `${context.baseUrl.replace(/\/+$/u, "")}${path}`;
}

function basicConfiguration(value: unknown, allowed: readonly string[]): UnknownRecord {
  const body = record(value);
  for (const [key, entry] of Object.entries(body)) {
    if (
      !allowed.includes(key) &&
      entry !== null &&
      entry !== false &&
      JSON.stringify(entry) !== "{}" &&
      JSON.stringify(entry) !== "[]"
    )
      throw new SchemaRegistryResponseError(
        null,
        "Advanced Registry configuration is outside reviewed change support.",
      );
  }
  return body;
}
function compatibilityLevel(value: unknown): SchemaCompatibilityLevel {
  if (
    typeof value !== "string" ||
    !SCHEMA_COMPATIBILITY_LEVELS.includes(value as SchemaCompatibilityLevel)
  )
    throw new SchemaRegistryResponseError(null);
  return value as SchemaCompatibilityLevel;
}

export class SchemaRegistryHttpAdapter {
  constructor(private readonly http: BoundedJsonHttpPort) {}

  async loadReviewSchema(
    context: KafkaClusterServiceContext,
    identity: SchemaSubjectVersionIdentity,
    signal: AbortSignal,
  ): Promise<SchemaVersionDetail | null> {
    const response = await this.request(
      context,
      signal,
      "GET",
      `/subjects/${encodeURIComponent(identity.subject)}/versions/${String(identity.version)}`,
    );
    if (
      response.status === 404 &&
      [40401, 40402].includes(Number(record(response.body).error_code))
    )
      return null;
    const body = basicConfiguration(successful(response), [
      "subject",
      "version",
      "id",
      "schema",
      "schemaType",
      "references",
    ]);
    const parsed = schemaVersion(body);
    if (
      parsed.subject !== identity.subject ||
      (identity.version !== "latest" && parsed.version !== identity.version)
    )
      throw new SchemaRegistryResponseError(null);
    return parsed;
  }

  async loadCompatibilityPolicy(
    context: KafkaClusterServiceContext,
    subject: string,
    signal: AbortSignal,
  ): Promise<SchemaCompatibilityPolicy> {
    const global = basicConfiguration(
      successful(await this.request(context, signal, "GET", "/config")),
      ["compatibilityLevel"],
    );
    const response = await this.request(
      context,
      signal,
      "GET",
      `/config/${encodeURIComponent(subject)}?defaultToGlobal=false`,
    );
    const subjectLevel =
      response.status === 404 && [40401, 40408].includes(Number(record(response.body).error_code))
        ? null
        : compatibilityLevel(
            basicConfiguration(successful(response), ["compatibilityLevel"]).compatibilityLevel,
          );
    const globalLevel = compatibilityLevel(global.compatibilityLevel);
    return { globalLevel, subjectLevel, effectiveLevel: subjectLevel ?? globalLevel };
  }

  async changeSubjectCompatibility(
    context: KafkaClusterServiceContext,
    subject: string,
    change: SchemaPolicyChange,
    signal: AbortSignal,
  ): Promise<{ readonly level: SchemaCompatibilityLevel }> {
    const response = await this.request(
      context,
      signal,
      change.mode === "set" ? "PUT" : "DELETE",
      `/config/${encodeURIComponent(subject)}`,
      change.mode === "set" ? { compatibility: change.level } : undefined,
    );
    return {
      level: compatibilityLevel(
        basicConfiguration(successful(response), ["compatibility"]).compatibility,
      ),
    };
  }

  async checkProposedCompatibility(
    context: KafkaClusterServiceContext,
    input: SchemaRegistrationInput,
    versions: readonly number[],
    signal: AbortSignal,
  ): Promise<SchemaRegistryCompatibilityResult> {
    if (
      !versions.length ||
      versions.length > 32 ||
      versions.some((version) => !Number.isSafeInteger(version) || version < 1 || version > 10000)
    )
      throw new SchemaRegistryResponseError(null);
    for (const version of versions) {
      const payload = record(
        successful(
          await this.request(
            context,
            signal,
            "POST",
            `/compatibility/subjects/${encodeURIComponent(input.subject)}/versions/${String(version)}?verbose=true&normalize=${String(input.normalize)}`,
            { references: input.references, schema: input.schema, schemaType: input.schemaType },
          ),
        ),
      );
      if (typeof payload.is_compatible !== "boolean") throw new SchemaRegistryResponseError(null);
      if (!payload.is_compatible) return { compatible: false, messages: [] };
    }
    return { compatible: true, messages: [] };
  }

  async byId(
    context: KafkaClusterServiceContext,
    id: number,
    signal: AbortSignal,
  ): Promise<RegisteredSchema> {
    const body = record(
      successful(await this.request(context, signal, "GET", `/schemas/ids/${String(id)}`)),
    );
    const parsed = schemaVersion({ ...body, id, subject: "schema-id", version: 1 });
    return {
      id: parsed.id,
      schema: parsed.schema,
      schemaType: parsed.schemaType,
      references: parsed.references,
    };
  }

  async byVersion(
    context: KafkaClusterServiceContext,
    subject: string,
    version: number,
    signal: AbortSignal,
  ): Promise<RegisteredSchema> {
    const parsed = schemaVersion(
      successful(
        await this.request(
          context,
          signal,
          "GET",
          `/subjects/${encodeURIComponent(subject)}/versions/${String(version)}`,
        ),
      ),
    );
    if (parsed.subject !== subject || parsed.version !== version)
      throw new SchemaRegistryResponseError(null);
    return {
      id: parsed.id,
      schema: parsed.schema,
      schemaType: parsed.schemaType,
      references: parsed.references,
    };
  }

  async listSubjects(
    context: KafkaClusterServiceContext,
    signal: AbortSignal,
  ): Promise<SchemaRegistrySubjectInventory> {
    const body = successful(await this.request(context, signal, "GET", "/subjects"));
    if (!Array.isArray(body)) {
      throw new SchemaRegistryResponseError(null);
    }
    const ordered = body
      .map((subject: unknown) => boundedString(subject))
      .sort((left, right) => left.localeCompare(right, "en-US"));
    return {
      omittedSubjects: Math.max(0, ordered.length - SCHEMA_REGISTRY_LIMITS.subjects),
      subjects: ordered.slice(0, SCHEMA_REGISTRY_LIMITS.subjects),
    };
  }

  async loadSubject(
    context: KafkaClusterServiceContext,
    identity: SchemaSubjectVersionIdentity,
    signal: AbortSignal,
  ): Promise<SchemaRegistrySubjectDetail> {
    return this.loadSubjectVersion(context, identity.subject, String(identity.version), signal);
  }

  async loadLatestSubject(
    context: KafkaClusterServiceContext,
    subject: string,
    signal: AbortSignal,
  ): Promise<SchemaRegistrySubjectDetail> {
    return this.loadSubjectVersion(context, subject, "latest", signal);
  }

  private async loadSubjectVersion(
    context: KafkaClusterServiceContext,
    subjectValue: string,
    version: string,
    signal: AbortSignal,
  ): Promise<SchemaRegistrySubjectDetail> {
    const subject = encodeURIComponent(subjectValue);
    const [versionsBody, schemaBody, compatibilityBody] = await Promise.all([
      this.request(context, signal, "GET", `/subjects/${subject}/versions`).then(successful),
      this.request(context, signal, "GET", `/subjects/${subject}/versions/${version}`).then(
        successful,
      ),
      this.loadCompatibilityConfiguration(context, subject, signal),
    ]);
    if (!Array.isArray(versionsBody) || versionsBody.length > SCHEMA_REGISTRY_LIMITS.versions) {
      throw new SchemaRegistryResponseError(null);
    }
    const compatibility = record(compatibilityBody);
    return {
      compatibilityLevel:
        typeof compatibility.compatibilityLevel === "string"
          ? boundedString(compatibility.compatibilityLevel, 128)
          : null,
      schema: schemaVersion(schemaBody),
      versions: versionsBody.map(positiveInteger).sort((left, right) => left - right),
    };
  }

  async checkCompatibility(
    context: KafkaClusterServiceContext,
    input: SchemaCompatibilityCheckInput,
    signal: AbortSignal,
  ): Promise<SchemaRegistryCompatibilityResult> {
    const response = await this.request(
      context,
      signal,
      "POST",
      `/compatibility/subjects/${encodeURIComponent(input.subject)}/versions/${String(input.version)}`,
      {
        references: input.references,
        schema: input.schema,
        schemaType: input.schemaType,
      },
    );
    if (response.status === 404 && input.version === "latest") {
      const error = record(response.body);
      if (error.error_code === 40_401 || error.error_code === 40_402) {
        return { compatible: true, messages: ["The subject has no registered version."] };
      }
    }
    const body = successful(response);
    const payload = record(body);
    if (typeof payload.is_compatible !== "boolean") {
      throw new SchemaRegistryResponseError(null);
    }
    const messages = payload.messages ?? [];
    if (
      !Array.isArray(messages) ||
      messages.length > SCHEMA_REGISTRY_LIMITS.compatibilityMessages
    ) {
      throw new SchemaRegistryResponseError(null);
    }
    return {
      compatible: payload.is_compatible,
      messages: messages.map((message) => boundedString(message, 2_048)),
    };
  }

  async register(
    context: KafkaClusterServiceContext,
    input: SchemaRegistrationInput,
    signal: AbortSignal,
  ): Promise<{ readonly id: number }> {
    const body = successful(
      await this.request(
        context,
        signal,
        "POST",
        `/subjects/${encodeURIComponent(input.subject)}/versions?normalize=${String(input.normalize)}`,
        { references: input.references, schema: input.schema, schemaType: input.schemaType },
      ),
    );
    return { id: positiveInteger(record(body).id) };
  }

  async delete(
    context: KafkaClusterServiceContext,
    input: SchemaDeletionInput,
    signal: AbortSignal,
  ): Promise<readonly number[]> {
    const target = input.target;
    const path =
      target.kind === "subject"
        ? `/subjects/${encodeURIComponent(target.subject)}`
        : `/subjects/${encodeURIComponent(target.subject)}/versions/${String(target.version)}`;
    if (input.mode === "permanent") {
      const soft = await this.request(context, signal, "DELETE", `${path}?permanent=false`);
      const alreadyAbsent =
        soft.status === 404 &&
        soft.body !== null &&
        typeof soft.body === "object" &&
        "error_code" in soft.body &&
        [40_401, 40_402, 40_404, 40_406].includes(Number(soft.body.error_code));
      if (!alreadyAbsent) successful(soft);
    }
    const body = successful(
      await this.request(
        context,
        signal,
        "DELETE",
        `${path}?permanent=${String(input.mode === "permanent")}`,
      ),
    );
    const versions = Array.isArray(body) ? body : [body];
    return versions.map(positiveInteger);
  }

  private async request(
    context: KafkaClusterServiceContext,
    signal: AbortSignal,
    method: "DELETE" | "GET" | "POST" | "PUT",
    path: string,
    body?: unknown,
  ): Promise<BoundedJsonHttpResponse> {
    signal = context.signal === undefined ? signal : AbortSignal.any([signal, context.signal]);
    signal.throwIfAborted();
    const authorization = await context.authorization(signal);
    signal.throwIfAborted();
    return this.http.request({
      ...(authorization === undefined ? {} : { authorization }),
      ...(body === undefined ? {} : { body }),
      ...(context.caPem === undefined ? {} : { caPem: context.caPem }),
      ...(context.clientIdentity === undefined ? {} : { clientIdentity: context.clientIdentity }),
      method,
      signal,
      url: serviceUrl(context, path),
    });
  }

  private async loadCompatibilityConfiguration(
    context: KafkaClusterServiceContext,
    encodedSubject: string,
    signal: AbortSignal,
  ): Promise<unknown> {
    const subjectResponse = await this.request(context, signal, "GET", `/config/${encodedSubject}`);
    if (subjectResponse.status !== 404) return successful(subjectResponse);
    return successful(await this.request(context, signal, "GET", "/config"));
  }
}
