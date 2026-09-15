import { describe, expect, it } from "vitest";

import { KafkaRuleSampleError, parseKafkaRuleSample } from "../../src/kafka/engine";

function capturedError(operation: () => unknown): unknown {
  try {
    operation();
  } catch (error) {
    return error;
  }
  return undefined;
}

describe("Kafka rule sample live limits", () => {
  it("distinguishes a valid payload beyond the caller byte limit from malformed JSON", () => {
    const limited = capturedError(() =>
      parseKafkaRuleSample('{"message":"bounded"}', {
        bytes: 8,
        depth: 64,
        nodes: 100,
      }),
    );
    const malformed = capturedError(() =>
      parseKafkaRuleSample("{", {
        bytes: 8,
        depth: 64,
        nodes: 100,
      }),
    );

    expect(limited).toBeInstanceOf(KafkaRuleSampleError);
    expect(limited).toMatchObject({ reason: "limit-exceeded" });
    expect(malformed).toBeInstanceOf(KafkaRuleSampleError);
    expect(malformed).toMatchObject({ reason: "malformed" });
  });

  it("accepts the exact caller node bound and rejects one additional node", () => {
    const limits = { bytes: 1_024, depth: 64, nodes: 4 };

    expect(parseKafkaRuleSample("[null,null,null]", limits)).toEqual([null, null, null]);
    expect(
      capturedError(() => parseKafkaRuleSample("[null,null,null,null]", limits)),
    ).toMatchObject({
      reason: "limit-exceeded",
    });
  });
});
