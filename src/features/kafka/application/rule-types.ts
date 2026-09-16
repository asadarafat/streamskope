import type {
  HostErrorCode,
  HostErrorStage,
  KafkaRuleDefinition,
  KafkaRuleIssue,
  KafkaRuleStoreCapability,
} from "../contracts";

import type { KafkaRuleSampleLimits } from "./rule-sample-error";

export interface KafkaRuleDocument {
  readonly rules: readonly KafkaRuleDefinition[];
}

export interface KafkaRuleStore {
  capability(): KafkaRuleStoreCapability;
  commit(document: KafkaRuleDocument, signal?: AbortSignal): Promise<void>;
  load(signal?: AbortSignal): Promise<KafkaRuleDocument | undefined>;
}

export interface KafkaRuleExpressionValidation {
  readonly diagnostic?: string;
  readonly valid: boolean;
}

export interface KafkaRulePredicate {
  evaluate(sample: unknown): boolean;
}

export interface KafkaRuleEvaluator {
  compile(expression: string): KafkaRulePredicate;
  evaluate(expression: string, sample: unknown): boolean;
  parseSample(sample: string, limits?: KafkaRuleSampleLimits): unknown;
  validate(expression: string): KafkaRuleExpressionValidation;
}

export interface KafkaRuleStructuredError extends Error {
  readonly code: HostErrorCode;
  readonly issues?: readonly KafkaRuleIssue[];
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly target: string | undefined;
}
