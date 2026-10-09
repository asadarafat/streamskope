import type { KafkaLiveRuleEvaluation } from "./live-rule-types";
import type { KafkaOriginalRecord } from "./record-bytes";
import type { KafkaRecordProvenance } from "./record-locator";
import type { StructuredRecord } from "./structured-record";

export interface KafkaMessage {
  readonly provenance?: KafkaRecordProvenance;
  readonly structured?: StructuredRecord;
  readonly original?: KafkaOriginalRecord;
  readonly headers: Readonly<Record<string, string>>;
  readonly id: string;
  readonly key: string | null;
  readonly offset: string;
  readonly originalByteSize: number;
  /** Full broker record bytes including every ordered header. */
  readonly recordByteSize?: number;
  readonly partition: number;
  readonly payload: string | null;
  readonly payloadTruncated?: boolean;
  readonly preview: string;
  readonly timestamp: string;
  readonly topic: string;
  readonly truncated: boolean;
}

export interface KafkaExploredMessage extends KafkaMessage {
  readonly ruleEvaluation: KafkaLiveRuleEvaluation;
}
