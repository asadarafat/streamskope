import type { KafkaExploredMessage } from "../contracts";
import type { RecordFormat } from "../contracts/record-codec";
import type { StructuredReplayField } from "../contracts/structured-replay";

export interface ReplayMappingDraft {
  readonly format: RecordFormat;
  readonly sourceId: number | null;
  readonly target: "json" | "registered";
  readonly subject: string;
  readonly version: string;
  readonly messageType: string;
}
export interface StructuredReplayDraft {
  readonly codec: StructuredReplayField["codec"];
  readonly patches: string;
  readonly mappings: readonly ReplayMappingDraft[];
}
export function replayWriterDrafts(
  messages: readonly KafkaExploredMessage[],
  field: "key" | "value",
  previous: readonly ReplayMappingDraft[] = [],
): readonly ReplayMappingDraft[] {
  const writers = new Map<string, ReplayMappingDraft>();
  for (const message of messages) {
    if (message.original?.state !== "complete" || message.original[field] === null) continue;
    const canonical = message.structured?.[field];
    const bytes = Uint8Array.from(atob(message.original[field]), (c) => c.charCodeAt(0));
    const framed = bytes.length >= 5 && bytes[0] === 0;
    const id = framed ? new DataView(bytes.buffer).getUint32(1) : null;
    const format: RecordFormat =
      canonical?.codec === "protobuf" ? "protobuf" : framed ? "avro" : "json";
    const retained = previous.find((m) => m.format === format && m.sourceId === id);
    writers.set(
      `${format}:${id}`,
      retained ?? {
        format,
        sourceId: id,
        target: framed ? "registered" : "json",
        subject: "",
        version: "1",
        messageType: "",
      },
    );
  }
  // Null-only fields need no encoding; keep a harmless explicit JSON mapping for the closed contract.
  return [...writers.values()].length
    ? [...writers.values()]
    : [
        {
          format: "json",
          sourceId: null,
          target: "json",
          subject: "",
          version: "1",
          messageType: "",
        },
      ];
}
export function structuredReplayField(
  draft: StructuredReplayDraft | null,
): StructuredReplayField | null {
  if (!draft) return null;
  const patches: unknown = JSON.parse(draft.patches);
  if (!Array.isArray(patches)) throw new Error("Use an array of JSON Pointer edits.");
  // The same closed host parser validates this draft before dispatch.
  return {
    codec: draft.codec,
    patches: patches as StructuredReplayField["patches"],
    mappings: draft.mappings.map((m) => ({
      format: m.format,
      sourceId: m.sourceId,
      target:
        m.target === "json"
          ? null
          : { subject: m.subject, version: Number(m.version), messageType: m.messageType },
    })),
  };
}
