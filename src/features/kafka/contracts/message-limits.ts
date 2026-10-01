import type { KafkaLiveRuleEvaluation } from "./live-rule-types";
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

export function kafkaRawMessageRetainedBytes(message: KafkaMessage): number {
  return utf8ByteLength(message.key) + utf8ByteLength(message.payload);
}

export function kafkaMessageRetainedBytes(message: KafkaMessage): number {
  return (
    kafkaRawMessageRetainedBytes(message) +
    (isExploredMessage(message) ? kafkaLiveRuleEvidenceBytes(message.ruleEvaluation) : 0)
  );
}
