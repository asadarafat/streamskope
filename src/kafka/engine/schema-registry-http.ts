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

export class SchemaRegistryHttpAdapter {
  constructor(private readonly http: BoundedJsonHttpPort) {}

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
      successful(await this.request(context, signal, "DELETE", `${path}?permanent=false`));
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
    method: "DELETE" | "GET" | "POST",
    path: string,
    body?: unknown,
  ): Promise<BoundedJsonHttpResponse> {
    const authorization = await context.authorization();
    return this.http.request({
      ...(authorization === undefined ? {} : { authorization }),
      ...(body === undefined ? {} : { body }),
      caPem: context.caPem,
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
