import {
  RECORD_CODEC_LIMITS,
  type RecordDecodeInput,
  type RecordDecodeResult,
} from "../contracts/record-codec";

import type { KafkaClusterServiceContext } from "./types";
import type {
  CodecSchemaBundle,
  RecordCodecPort,
  RegisteredSchema,
  SchemaLookupPort,
} from "./record-codec-types";

export class SchemaResolutionError extends Error {
  constructor(readonly code: "reference" | "limit" | "schema-unavailable") {
    super(code);
  }
}

export class RecordCodecService {
  private readonly cache = new Map<string, { schema: RegisteredSchema; bytes: number }>();
  private cacheBytes = 0;
  private readonly decoded = new Map<string, { result: RecordDecodeResult; bytes: number }>();
  private decodedBytes = 0;
  private readonly scopes = new WeakMap<KafkaClusterServiceContext, number>();
  private nextScope = 0;
  private scope(context: KafkaClusterServiceContext): string {
    let scope = this.scopes.get(context);
    if (scope === undefined) {
      scope = ++this.nextScope;
      this.scopes.set(context, scope);
      const prefix = `${scope}:`;
      context.signal?.addEventListener(
        "abort",
        () => {
          for (const [key, entry] of this.cache)
            if (key.startsWith(prefix)) {
              this.cacheBytes -= entry.bytes;
              this.cache.delete(key);
            }
          for (const [key, entry] of this.decoded)
            if (key.startsWith(prefix)) {
              this.decodedBytes -= entry.bytes;
              this.decoded.delete(key);
            }
        },
        { once: true },
      );
    }
    return String(scope);
  }
  constructor(
    private readonly lookup: SchemaLookupPort,
    private readonly codec: RecordCodecPort,
  ) {}

  clear(): void {
    this.cache.clear();
    this.cacheBytes = 0;
    this.decoded.clear();
    this.decodedBytes = 0;
  }

  private async cached(
    key: string,
    load: () => Promise<RegisteredSchema>,
    signal: AbortSignal,
  ): Promise<RegisteredSchema> {
    signal.throwIfAborted();
    const cached = this.cache.get(key);
    if (cached) {
      this.cache.delete(key);
      this.cache.set(key, cached);
      return cached.schema;
    }
    let schema: RegisteredSchema;
    try {
      schema = await load();
    } catch {
      signal.throwIfAborted();
      throw new SchemaResolutionError("schema-unavailable");
    }
    signal.throwIfAborted();
    const bytes = new TextEncoder().encode(JSON.stringify(schema)).length;
    if (bytes > RECORD_CODEC_LIMITS.schemaBytes) throw new SchemaResolutionError("limit");
    while (
      this.cache.size >= RECORD_CODEC_LIMITS.cacheEntries ||
      this.cacheBytes + bytes > RECORD_CODEC_LIMITS.cacheBytes
    ) {
      const first = this.cache.keys().next().value;
      if (first === undefined) break;
      this.cacheBytes -= this.cache.get(first)!.bytes;
      this.cache.delete(first);
    }
    // Simultaneous reads of the same key must not count the entry twice.
    this.cacheBytes -= this.cache.get(key)?.bytes ?? 0;
    this.cache.set(key, { schema, bytes });
    this.cacheBytes += bytes;
    return schema;
  }

  async resolve(
    context: KafkaClusterServiceContext,
    id: number,
    signal: AbortSignal,
  ): Promise<CodecSchemaBundle> {
    if (context.signal) signal = AbortSignal.any([signal, context.signal]);
    signal.throwIfAborted();
    const root = await this.cached(
      `${this.scope(context)}:id:${id}`,
      () => this.lookup.byId(context, id, signal),
      signal,
    );
    if (root.id !== id) throw new SchemaResolutionError("schema-unavailable");
    const dependencies: { name: string; schema: RegisteredSchema }[] = [];
    const visited = new Map<string, RegisteredSchema>();
    const names = new Map<string, number>();
    let bytes = new TextEncoder().encode(root.schema).length;
    const visit = async (
      schema: RegisteredSchema,
      path: Set<string>,
      depth: number,
    ): Promise<void> => {
      if (depth > RECORD_CODEC_LIMITS.referenceDepth) throw new SchemaResolutionError("limit");
      for (const ref of schema.references) {
        signal.throwIfAborted();
        const key = JSON.stringify([ref.subject, ref.version]);
        if (path.has(key)) throw new SchemaResolutionError("reference");
        let child = visited.get(key);
        if (!child) {
          if (visited.size >= RECORD_CODEC_LIMITS.schemaNodes - 1)
            throw new SchemaResolutionError("limit");
          child = await this.cached(
            `${this.scope(context)}:version:${key}`,
            () => this.lookup.byVersion(context, ref.subject, ref.version, signal),
            signal,
          );
          visited.set(key, child);
          bytes += new TextEncoder().encode(child.schema).length;
          if (bytes > RECORD_CODEC_LIMITS.graphBytes) throw new SchemaResolutionError("limit");
          await visit(child, new Set([...path, key]), depth + 1);
        }
        if (names.has(ref.name) && names.get(ref.name) !== child.id)
          throw new SchemaResolutionError("reference");
        if (!names.has(ref.name)) {
          if (dependencies.length >= RECORD_CODEC_LIMITS.schemaNodes - 1)
            throw new SchemaResolutionError("limit");
          names.set(ref.name, child.id);
          dependencies.push({ name: ref.name, schema: child });
        }
      }
    };
    await visit(root, new Set(), 0);
    const bundle = { root, dependencies };
    if (new TextEncoder().encode(JSON.stringify(bundle)).length > RECORD_CODEC_LIMITS.graphBytes)
      throw new SchemaResolutionError("limit");
    return bundle;
  }

  async resolveVersion(
    context: KafkaClusterServiceContext,
    subject: string,
    version: number,
    signal: AbortSignal,
  ): Promise<CodecSchemaBundle> {
    if (context.signal) signal = AbortSignal.any([signal, context.signal]);
    signal.throwIfAborted();
    const schema = await this.cached(
      `${this.scope(context)}:version:${JSON.stringify([subject, version])}`,
      () => this.lookup.byVersion(context, subject, version, signal),
      signal,
    );
    const bundle = await this.resolve(context, schema.id, signal);
    if (
      bundle.root.schemaType !== schema.schemaType ||
      bundle.root.schema !== schema.schema ||
      JSON.stringify(bundle.root.references) !== JSON.stringify(schema.references)
    )
      throw new SchemaResolutionError("schema-unavailable");
    return bundle;
  }

  async decode(
    input: RecordDecodeInput,
    context: KafkaClusterServiceContext | null,
    signal: AbortSignal,
  ): Promise<RecordDecodeResult> {
    try {
      if (context?.signal) signal = AbortSignal.any([signal, context.signal]);
      signal.throwIfAborted();
      if (input.bytes === null) return { state: "null", format: input.format };
      const cacheKey = `${context ? this.scope(context) : "none"}:${input.format}:${input.bytes}`;
      const retained = this.decoded.get(cacheKey);
      if (retained) {
        this.decoded.delete(cacheKey);
        this.decoded.set(cacheKey, retained);
        return retained.result;
      }
      let bundle: CodecSchemaBundle | null = null;
      if (input.format !== "json") {
        const bytes = Uint8Array.from(atob(input.bytes), (c) => c.charCodeAt(0));
        if (bytes.length < 5 || bytes[0] !== 0)
          return {
            state: "error",
            format: input.format,
            code: "malformed",
            detail:
              "Expected Confluent magic byte 0 and a four-byte schema ID. Raw binary and GUID-header framing are not supported.",
          };
        const id = new DataView(bytes.buffer).getUint32(1);
        if (id === 0 || id > 0x7fffffff) throw new SchemaResolutionError("schema-unavailable");
        if (!context) throw new SchemaResolutionError("schema-unavailable");
        bundle = await this.resolve(context, id, signal);
        if (bundle.root.schemaType !== (input.format === "avro" ? "AVRO" : "PROTOBUF"))
          return {
            state: "error",
            format: input.format,
            code: "schema-type",
            detail:
              "The Registry schema type differs from the selected encoding. Choose the declared writer format.",
          };
      }
      const result = await this.codec.decode(input, bundle, signal);
      signal.throwIfAborted();
      if (result.state === "decoded") {
        const bytes = cacheKey.length + new TextEncoder().encode(JSON.stringify(result)).length;
        if (bytes <= RECORD_CODEC_LIMITS.cacheBytes) {
          while (
            this.decoded.size >= RECORD_CODEC_LIMITS.cacheEntries ||
            this.decodedBytes + bytes > RECORD_CODEC_LIMITS.cacheBytes
          ) {
            const first = this.decoded.keys().next().value;
            if (first === undefined) break;
            this.decodedBytes -= this.decoded.get(first)!.bytes;
            this.decoded.delete(first);
          }
          this.decodedBytes -= this.decoded.get(cacheKey)?.bytes ?? 0;
          this.decoded.set(cacheKey, { result: Object.freeze({ ...result }), bytes });
          this.decodedBytes += bytes;
        }
      }
      return result;
    } catch (error) {
      const code = signal.aborted
        ? "cancelled"
        : error instanceof SchemaResolutionError
          ? error.code
          : "unavailable";
      return {
        state: "error",
        format: input.format,
        code,
        detail:
          code === "cancelled"
            ? "Decoding cancelled because the request or connection ended."
            : code === "schema-unavailable"
              ? "The writer schema could not be read. Configure the active profile's Schema Registry endpoint and credentials, then check its schema ID and permissions."
              : code === "reference"
                ? "Schema references are cyclic or ambiguous; decoding was stopped."
                : code === "limit"
                  ? "Schema resolution exceeded its bounded size, depth or count."
                  : "The decoder is unavailable or exceeded its execution limit. Original bytes remain unchanged.",
      };
    }
  }
}
