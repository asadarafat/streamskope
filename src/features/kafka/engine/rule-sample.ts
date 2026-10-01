import { KAFKA_RULE_LIMITS, utf8ByteLength } from "../contracts";
import {
  KafkaRuleSampleParseError,
  type KafkaRuleSampleFailureReason,
  type KafkaRuleSampleLimits,
} from "../application";

const DEFAULT_LIMITS: KafkaRuleSampleLimits = {
  bytes: KAFKA_RULE_LIMITS.sampleBytes,
  depth: KAFKA_RULE_LIMITS.sampleDepth,
  nodes: KAFKA_RULE_LIMITS.sampleNodes,
};

export class KafkaRuleSampleError extends KafkaRuleSampleParseError {
  constructor(message: string, reason: KafkaRuleSampleFailureReason, options?: ErrorOptions) {
    super(message.slice(0, KAFKA_RULE_LIMITS.diagnosticCharacters), reason, options);
    this.name = "KafkaRuleSampleError";
  }
}

export function parseKafkaRuleSample(
  sample: string,
  limits: KafkaRuleSampleLimits = DEFAULT_LIMITS,
): unknown {
  if (utf8ByteLength(sample) > limits.bytes) {
    throw new KafkaRuleSampleError(
      `Sample exceeds ${String(limits.bytes)} UTF-8 bytes.`,
      "limit-exceeded",
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(sample) as unknown;
  } catch (error) {
    throw new KafkaRuleSampleError("Sample must be valid JSON.", "malformed", { cause: error });
  }

  const stack: Array<{ readonly depth: number; readonly value: unknown }> = [
    { depth: 0, value: parsed },
  ];
  let nodes = 0;
  while (stack.length > 0) {
    const current = stack.pop();
    if (current === undefined) {
      continue;
    }
    nodes += 1;
    if (nodes > limits.nodes) {
      throw new KafkaRuleSampleError(
        `Sample exceeds ${String(limits.nodes)} structural nodes.`,
        "limit-exceeded",
      );
    }
    if (current.depth > limits.depth) {
      throw new KafkaRuleSampleError(
        `Sample exceeds structural depth ${String(limits.depth)}.`,
        "limit-exceeded",
      );
    }
    if (current.value === null || typeof current.value !== "object") {
      continue;
    }
    const children = Array.isArray(current.value)
      ? current.value
      : Object.values(current.value as Record<string, unknown>);
    for (const child of children) {
      stack.push({ depth: current.depth + 1, value: child });
    }
  }
  return parsed;
}
