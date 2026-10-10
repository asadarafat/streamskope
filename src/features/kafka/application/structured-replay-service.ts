import {
  replayBatch,
  transformReplayRecord,
  type RecordReplayInput,
  type RecordReplayReview,
} from "../contracts/record-replay";
import { parseRecordBatchInput, type RecordBatchInput } from "../contracts/schema-samples";
import {
  type ReplayFieldEncoding,
  type ReplayRecordEncoding,
  type StructuredReplayField,
} from "../contracts/structured-replay";
import type { KafkaCompleteRecord } from "../contracts/record-bytes";

import type { SchemaRegistryReviewScope } from "./connection-scope";
import { RecordCodecService } from "./record-codec-service";
import { StructuredRecordService } from "./structured-record-service";
import { boundedRecordJson } from "./record-json";
import { applyReplayJsonPatches } from "./replay-json-patches";
import type {
  CodecSchemaBundle,
  RecordCodecPort,
  SchemaLookupPort,
  SchemaAuthoringPort,
} from "./record-codec-types";

export interface PreparedReplayEncoding {
  readonly batch: RecordBatchInput;
  readonly encoding?: readonly ReplayRecordEncoding[];
  revalidate(index: number): Promise<boolean>;
}
export interface ReplayEncodingPort {
  prepare(
    input: RecordReplayInput,
    target: SchemaRegistryReviewScope | null,
    signal: AbortSignal,
    frozen?: { readonly review: RecordReplayReview; readonly startIndex: number },
  ): Promise<PreparedReplayEncoding>;
}
function framed(bytes: string | null): boolean {
  if (bytes === null) return false;
  const value = atob(bytes);
  return value.length >= 5 && value.charCodeAt(0) === 0;
}
async function fingerprint(bundle: CodecSchemaBundle): Promise<string> {
  const bytes = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(bundle)),
  );
  return Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("");
}
const encodeJson = (json: string): string =>
  btoa(Array.from(new TextEncoder().encode(json), (b) => String.fromCharCode(b)).join(""));

/** One canonical decoder and authoring worker; Registry IDs are never copied as target identity. */
export class StructuredReplayService implements ReplayEncodingPort {
  constructor(
    private readonly source: () => SchemaRegistryReviewScope | null,
    private readonly codec?: RecordCodecPort,
    private readonly lookup?: SchemaLookupPort,
    private readonly authoring?: SchemaAuthoringPort,
  ) {}
  async prepare(
    input: RecordReplayInput,
    target: SchemaRegistryReviewScope | null,
    signal: AbortSignal,
    frozen?: { readonly review: RecordReplayReview; readonly startIndex: number },
  ): Promise<PreparedReplayEncoding> {
    signal.throwIfAborted();
    const structured = input.transform.structured;
    for (const record of input.records) {
      if (
        input.targetProfile &&
        ((framed(record.original.key) && !structured?.key) ||
          (framed(record.original.value) && !structured?.value))
      )
        throw new Error(
          "Framed records require structured destination writer mapping before cross-profile replay.",
        );
    }
    if (frozen) {
      const batch = parseRecordBatchInput({
        ...frozen.review.batch,
        records: frozen.review.batch.records.slice(frozen.startIndex),
        ...(frozen.review.batch.timestamps
          ? { timestamps: frozen.review.batch.timestamps.slice(frozen.startIndex) }
          : {}),
      });
      const encoding = frozen.review.encoding?.slice(frozen.startIndex);
      return {
        batch,
        ...(encoding ? { encoding: structuredClone(encoding) } : {}),
        revalidate: (index): Promise<boolean> =>
          encoding && !encoding[index]
            ? Promise.resolve(false)
            : this.verify(
                encoding?.[index],
                target,
                AbortSignal.any([signal, AbortSignal.timeout(15_000)]),
              ),
      };
    }
    if (!structured)
      return {
        batch: replayBatch(input),
        revalidate: (): Promise<boolean> => Promise.resolve(!signal.aborted),
      };
    if (!this.codec || !this.lookup) throw new Error("Structured record encoding is unavailable.");
    const admission = AbortSignal.any([signal, AbortSignal.timeout(30_000)]);
    const source = this.source(),
      resolver = new RecordCodecService(this.lookup, this.codec),
      decoder = new StructuredRecordService(resolver);
    const records: KafkaCompleteRecord[] = [],
      encoding: ReplayRecordEncoding[] = [];
    for (const record of input.records) {
      admission.throwIfAborted();
      const key = await this.translate(
        record.original.key,
        structured.key,
        source,
        target,
        decoder,
        admission,
      );
      const value = await this.translate(
        record.original.value,
        structured.value,
        source,
        target,
        decoder,
        admission,
      );
      const transformed = transformReplayRecord(record.original, {
        key: input.transform.key,
        valueText: input.transform.valueText,
        removeHeaders: input.transform.removeHeaders,
        appendHeaders: input.transform.appendHeaders,
      });
      records.push({
        ...transformed,
        key: structured.key ? key.bytes : transformed.key,
        value: structured.value ? value.bytes : transformed.value,
      });
      encoding.push({ key: key.evidence, value: value.evidence });
    }
    const batch = parseRecordBatchInput({
      topic: input.topic,
      partition: input.partition,
      ratePerSecond: input.ratePerSecond,
      records,
      timestamps: input.records.map((r) => r.timestampMs),
    });
    admission.throwIfAborted();
    return {
      batch,
      encoding,
      revalidate: (index): Promise<boolean> =>
        encoding[index]
          ? this.verify(
              encoding[index],
              target,
              AbortSignal.any([signal, AbortSignal.timeout(15_000)]),
            )
          : Promise.resolve(false),
    };
  }
  private async translate(
    bytes: string | null,
    field: StructuredReplayField | null,
    source: SchemaRegistryReviewScope | null,
    target: SchemaRegistryReviewScope | null,
    decoder: StructuredRecordService,
    signal: AbortSignal,
  ): Promise<{ bytes: string | null; evidence: ReplayFieldEncoding | null }> {
    if (!field || bytes === null) return { bytes, evidence: null };
    const decoded = source
      ? await source.read(
          (context, combined) => decoder.decodeField(bytes, field.codec, context, combined),
          signal,
        )
      : await decoder.decodeField(bytes, field.codec, null, signal);
    if (
      decoded.state !== "decoded" ||
      decoded.json === null ||
      !["json", "avro", "protobuf"].includes(decoded.codec)
    )
      throw new Error("A structured field could not be decoded from its complete original bytes.");
    const format = decoded.codec as "json" | "avro" | "protobuf",
      id = decoded.writerSchema?.id ?? null;
    const mapping = field.mappings.find((m) => m.format === format && m.sourceId === id);
    if (!mapping)
      throw new Error("Every decoded source writer requires an explicit destination mapping.");
    const payload = boundedRecordJson(applyReplayJsonPatches(decoded.json, field.patches));
    if (new TextEncoder().encode(payload).length > 16_384)
      throw new Error("Transformed projection exceeds 16 KiB.");
    if (!mapping.target)
      return { bytes: encodeJson(payload), evidence: { source: { format, id }, target: null } };
    if (!target || !this.authoring)
      throw new Error(
        "Configure the destination Registry and select an existing registered writer.",
      );
    const selection = mapping.target;
    return target.read(async (context, combined) => {
      // Fresh destination authority and graph; source caches must never alias this Registry.
      const fresh = new RecordCodecService(this.lookup!, this.codec!);
      const bundle = await fresh.resolveVersion(
        context,
        selection.subject,
        selection.version,
        combined,
      );
      if (bundle.root.schemaType !== "PROTOBUF" && selection.messageType !== "")
        throw new Error("Only Protobuf writers accept a message type selection.");
      const authored = await this.authoring!.author(
        { ...selection, schemaId: bundle.root.id, payload },
        bundle,
        combined,
      );
      if (authored.state !== "valid")
        throw new Error(
          "Transformed projection does not validate against the selected destination writer.",
        );
      const evidence: ReplayFieldEncoding = {
        source: { format, id },
        target: {
          ...selection,
          id: bundle.root.id,
          schemaType: bundle.root.schemaType,
          messageType: authored.messageType ?? "",
          fingerprint: await fingerprint(bundle),
        },
      };
      combined.throwIfAborted();
      return { bytes: authored.record.value, evidence };
    }, signal);
  }
  private async verify(
    encoding: ReplayRecordEncoding | undefined,
    target: SchemaRegistryReviewScope | null,
    signal: AbortSignal,
  ): Promise<boolean> {
    if (signal.aborted) return false;
    if (!encoding) return true;
    try {
      for (const field of [encoding.key, encoding.value]) {
        if (!field?.target) continue;
        if (!target || !this.lookup || !this.codec) return false;
        const expected = field.target;
        const valid = await target.read(async (context, combined) => {
          const fresh = new RecordCodecService(this.lookup!, this.codec!);
          const bundle = await fresh.resolveVersion(
            context,
            expected.subject,
            expected.version,
            combined,
          );
          return (
            bundle.root.id === expected.id &&
            bundle.root.schemaType === expected.schemaType &&
            (await fingerprint(bundle)) === expected.fingerprint
          );
        }, signal);
        if (!valid) return false;
      }
      return !signal.aborted;
    } catch {
      return false;
    }
  }
}
