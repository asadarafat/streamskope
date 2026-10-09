import { KAFKA_RECORD_PROTECTION_DEFAULTS } from "../../src/features/kafka/contracts/operational-preference-types";
import {
  RECORD_EXPORT_LIMITS,
  type RecordExportReceiptDetails,
  type RecordExportArtifact,
} from "../../src/features/kafka/contracts/record-export";
import type { RecordExportSink } from "../../src/features/kafka/application/record-export-artifacts";
import { RECORD_CODEC_DEFAULTS } from "../../src/features/kafka/contracts/structured-record";
import type {
  NodeRecordExportArtifacts,
  RecordExportDelivery,
} from "../../src/platform/node/record-export-artifacts";
import type { ArtifactReference } from "../../src/platform/desktop";

export function artifactReceipt(bytes: number): RecordExportReceiptDetails {
  return {
    limits: RECORD_EXPORT_LIMITS,
    jobId: "artifact-fixture",
    input: {
      requestId: "artifact-fixture",
      topic: "records",
      range: { mode: "earliest" },
      search: { key: "", value: "", offset: "", timestamp: "", partition: null },
      format: "jsonl",
      maxRecords: 1000,
    },
    source: { connectionName: "Test", clusterId: "cluster", topicId: "topic" },
    settings: { codecs: RECORD_CODEC_DEFAULTS, protection: KAFKA_RECORD_PROTECTION_DEFAULTS },
    startedAt: "2026-10-09T12:00:00.000Z",
    completedAt: "2026-10-09T12:00:01.000Z",
    outcome: "complete",
    reason: "range-complete",
    coverage: null,
    counts: {
      passes: 1,
      scannedRecords: 1,
      scannedBytes: bytes,
      writtenRecords: 1,
      writtenBytes: bytes,
      unavailableRecords: 0,
      decodeErrorRecords: 0,
      originalUnavailableRecords: 0,
    },
  };
}
export async function sealedArtifact(
  store: NodeRecordExportArtifacts,
  text = '{"value":"private-export-sentinel"}\n',
  lifetimeMs = 60000,
): Promise<{
  sink: RecordExportSink;
  artifact: RecordExportArtifact;
  signal: AbortController;
  text: string;
}> {
  const signal = new AbortController();
  const sink = await store.create({
    jobId: "artifact-fixture",
    format: "jsonl",
    maximumBytes: RECORD_EXPORT_LIMITS.bytes,
    lifetimeMs,
    signal: signal.signal,
    assertCurrent: () => signal.signal.throwIfAborted(),
  });
  await sink.write(Buffer.from(text));
  const artifact = await sink.seal(artifactReceipt(Buffer.byteLength(text)));
  return { sink, artifact, signal, text };
}
export function readArtifact(
  delivery: RecordExportDelivery,
  reference: ArtifactReference,
): Promise<Buffer> {
  return delivery.withDownload(
    reference,
    { signal: new AbortController().signal, assertCurrent: () => undefined },
    async (chunks) => {
      const bytes: Uint8Array[] = [];
      for await (const chunk of chunks) bytes.push(chunk);
      return Buffer.concat(bytes);
    },
  );
}
