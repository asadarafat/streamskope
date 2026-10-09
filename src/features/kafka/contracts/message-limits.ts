import type { StructuredRecord, RecordField } from "./structured-record";
import type { KafkaLiveRuleEvaluation } from "./live-rule-types";
import type { KafkaOriginalRecord } from "./record-bytes";
import type { KafkaExploredMessage, KafkaMessage } from "./types";

const UTF8_ENCODER = new TextEncoder();
const LIVE_RULE_RESULT_FIXED_BYTES = 256;
const LIVE_RULE_ENTRY_FIXED_BYTES = 64;

export function utf8ByteLength(value: string | null): number {
  return value === null ? 0 : UTF8_ENCODER.encode(value).byteLength;
}

export function kafkaLiveRuleEvidenceBytes(evaluation: KafkaLiveRuleEvaluation): number {
  const matchBytes = [...evaluation.activeMatches, ...evaluation.suppressedMatches].reduce(
    (bytes, match) =>
      bytes + LIVE_RULE_ENTRY_FIXED_BYTES + utf8ByteLength(match.name) + match.level.length,
    0,
  );
  const errorBytes = evaluation.errors.reduce(
    (bytes, error) =>
      bytes +
      LIVE_RULE_ENTRY_FIXED_BYTES +
      utf8ByteLength(error.name) +
      utf8ByteLength(error.diagnostic),
    0,
  );
  return LIVE_RULE_RESULT_FIXED_BYTES + matchBytes + errorBytes;
}

function isExploredMessage(message: KafkaMessage): message is KafkaExploredMessage {
  return "ruleEvaluation" in message;
}

// Base64 is ASCII without JSON escapes. Count its canonical envelope without
// repeatedly allocating another copy of the record at every queue boundary.
function originalRetainedBytes(original: KafkaOriginalRecord | undefined): number {
  if (original === undefined) return 0;
  if (original.state === "unavailable") return 35 + original.reason.length;
  const field = (value: string | null): number => (value === null ? 4 : value.length + 2);
  return (
    69 +
    field(original.key) +
    field(original.value) +
    original.headers.reduce(
      (bytes, header, index) =>
        bytes + 17 + field(header.key) + field(header.value) + (index === 0 ? 0 : 1),
      0,
    )
  );
}

function structuredRetainedBytes(value: StructuredRecord | undefined): number {
  if (!value) return 0;
  const field = (f: RecordField): number =>
    160 +
    (f.writerSchema
      ? 64 + utf8ByteLength(f.writerSchema.registry) + utf8ByteLength(f.writerSchema.messageType)
      : 0) +
    (f.state === "decoded"
      ? utf8ByteLength(f.text) + utf8ByteLength(f.json)
      : f.state === "error"
        ? utf8ByteLength(f.detail)
        : 0);
  return (
    96 +
    field(value.key) +
    field(value.value) +
    value.headers.reduce(
      (n, h) => n + 48 + utf8ByteLength(h.key) + utf8ByteLength(h.value) + utf8ByteLength(h.error),
      0,
    )
  );
}

export function kafkaRawMessageRetainedBytes(message: KafkaMessage): number {
  return (
    (message.provenance === undefined
      ? 0
      : 96 +
        utf8ByteLength(message.provenance.clusterId) +
        utf8ByteLength(message.provenance.topicId)) +
    utf8ByteLength(message.key) +
    utf8ByteLength(message.payload) +
    utf8ByteLength(message.preview) +
    Object.entries(message.headers).reduce(
      (total, [key, value]) => total + utf8ByteLength(key) + utf8ByteLength(value),
      0,
    ) +
    structuredRetainedBytes(message.structured) +
    originalRetainedBytes(message.original)
  );
}

export function kafkaMessageRetainedBytes(message: KafkaMessage): number {
  return (
    kafkaRawMessageRetainedBytes(message) +
    (isExploredMessage(message) ? kafkaLiveRuleEvidenceBytes(message.ruleEvaluation) : 0)
  );
}
