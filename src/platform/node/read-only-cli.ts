import { constants } from "node:fs";
import { open } from "node:fs/promises";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseKafkaFetchRequest,
  type KafkaRecordProtection,
  type SecureConnectionInput,
} from "../../features/kafka/contracts";
import { parseKafkaRecordProtection } from "../../features/kafka/contracts/operational-preference-validation";
import { record, exactKeys } from "../../features/kafka/contracts/validation-primitives";
import {
  RECORD_CODEC_DEFAULTS,
  parseRecordCodecPreferences,
  type RecordCodecPreferences,
} from "../../features/kafka/contracts/structured-record";
import { StreamSkopeKafkaEngine } from "../../features/kafka/engine";

import { createHostRecordPipeline } from "./record-pipeline";

export interface CliIo {
  write(value: unknown): Promise<void>;
}
/** Only fixed, host-owned input guidance may be disclosed by the CLI boundary. */
export class CliInputError extends Error {}
export async function readCliJson(path: string, secret = false): Promise<unknown> {
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const info = await file.stat();
    if (
      !info.isFile() ||
      info.size > 1048576 ||
      (secret && process.platform !== "win32" && (info.mode & 0o077) !== 0)
    )
      throw new CliInputError("Use a bounded private regular configuration file (chmod 600).");
    // Bound the read itself as well as the initial stat: a file may grow after stat.
    const bytes = Buffer.alloc(1048577);
    let length = 0;
    while (length < bytes.length) {
      const part = await file.read(bytes, length, bytes.length - length, null);
      if (part.bytesRead === 0) break;
      length += part.bytesRead;
    }
    if (length > 1048576) throw new CliInputError("Configuration exceeds 1 MiB.");
    return JSON.parse(bytes.subarray(0, length).toString("utf8")) as unknown;
  } finally {
    await file.close();
  }
}
export function parseCliConfiguration(value: unknown): {
  connection: SecureConnectionInput;
  protection: KafkaRecordProtection;
  codecs: RecordCodecPreferences;
} {
  const p = record(value, "CLI config");
  exactKeys(p, ["connection", "protection", "codecs"], "CLI config");
  const command = parseHostCommand({
    command: "connection.connect",
    id: "cli",
    version: HOST_PROTOCOL_VERSION,
    payload: p.connection,
  });
  if (command.command !== "connection.connect" || "acquisitionId" in command.payload.tls)
    throw new CliInputError(
      "CLI requires explicit protected credentials and PEM trust; desktop acquisition handles are unavailable.",
    );
  return {
    connection: command.payload as SecureConnectionInput,
    codecs:
      p.codecs === undefined ? { ...RECORD_CODEC_DEFAULTS } : parseRecordCodecPreferences(p.codecs),
    protection: { ...parseKafkaRecordProtection(p.protection), readOnly: true },
  };
}
export async function runReadOnlyCli(
  operation: "inspect" | "query" | "export",
  configuration: unknown,
  query: unknown,
  io: CliIo,
  signal: AbortSignal,
): Promise<void> {
  const { connection, protection, codecs } = parseCliConfiguration(configuration);
  const request = parseCliQuery(operation, query);
  const engine = new StreamSkopeKafkaEngine(
    createHostRecordPipeline(() => ({ codecs, protection })),
  );
  const active = await engine.openConnection(connection, signal);
  try {
    if (!request) {
      const metadata = await active.describeClusterMetadata(signal),
        topics = await active.listTopics(signal);
      await io.write({
        format: "streamskope.cli/v1",
        kind: "inspection",
        clusterId: metadata.clusterId,
        brokerCount: metadata.brokers.length,
        topics: [...topics].sort(),
      });
      return;
    }
    const stream = await active.openMessageStream(request, signal);
    let count = 0,
      bytes = 0,
      limited = false;
    try {
      for await (const message of stream) {
        signal.throwIfAborted();
        const result = { format: "streamskope.cli/v1", kind: "record", record: message };
        const size = Buffer.byteLength(JSON.stringify(result)) + 1;
        if (bytes + size > 8388608) {
          limited = true;
          break;
        }
        bytes += size;
        count++;
        await io.write(result);
      }
    } finally {
      await stream.close();
    }
    await io.write({
      format: "streamskope.cli/v1",
      kind: "summary",
      operation,
      count,
      bytes,
      complete: !limited && stream.coverage?.()?.reason === "range-complete",
      stopReason: limited ? "output-limit" : (stream.coverage?.()?.reason ?? "read-complete"),
      coverage: stream.coverage?.() ?? null,
    });
  } finally {
    await active.close();
  }
}

export function parseCliQuery(
  operation: "inspect" | "query" | "export",
  query: unknown,
): ReturnType<typeof parseKafkaFetchRequest> | null {
  const request = operation === "inspect" ? null : parseKafkaFetchRequest(query);
  if (request && (request.mode === "tail" || request.maxMessages > 1000))
    throw new CliInputError("CLI queries require a finite read of at most 1,000 records.");
  return request;
}
