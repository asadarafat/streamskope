import { MultipleErrors, ProtocolError, TimeoutError } from "@platformatic/kafka";
import { describe, expect, it } from "vitest";

import {
  mapKafkaAdminFailure,
  mapKafkaConsumerGroupFailure,
} from "../../src/features/kafka/engine/failure";

describe("Kafka administration failure mapping", () => {
  it.each(["sdk", "nested-sdk", "nested-socket"] as const)(
    "reports a %s timeout with actionable, sanitized diagnostics and retained causes",
    (shape) => {
      const privateText = "private-token-and-broker-response";
      const timeout =
        shape === "nested-socket"
          ? Object.assign(new Error(privateText), { code: "ETIMEDOUT" })
          : new TimeoutError(privateText);
      const error =
        shape === "sdk"
          ? timeout
          : new Error("Request failed", {
              cause: new MultipleErrors("Broker response", [timeout]),
            });
      const cleanupCause = new Error("Cleanup failed");

      const failure = mapKafkaAdminFailure(error, "selected topic", cleanupCause);

      expect(failure).toMatchObject({
        code: "TIMEOUT",
        retryable: true,
        stage: "broker",
        target: "selected topic",
      });
      expect(failure.message).toMatch(/request timed out/iu);
      expect(failure.recovery).toMatch(/broker endpoints.*network path.*retry/iu);
      expect(`${failure.message} ${failure.recovery}`).not.toContain(privateText);
      expect(failure.cause).toBe(error);
      expect(failure.cleanupCause).toBe(cleanupCause);
    },
  );

  it("reports an unsupported broker API distinctly from empty data", () => {
    const error = Object.assign(new Error("The broker returned unsupported version"), {
      code: "UNSUPPORTED_VERSION",
    });

    expect(mapKafkaAdminFailure(error, "Kafka ACLs")).toMatchObject({
      code: "UNSUPPORTED_OPERATION",
      retryable: false,
      stage: "kafka",
      target: "Kafka ACLs",
    });
  });
});

it("recognizes permission codes in real client aggregate errors and handles cyclic causes", () => {
  const denied = new ProtocolError("GROUP_AUTHORIZATION_FAILED");
  const grouped = new MultipleErrors("Broker response", [denied]);
  const outer = new Error("Operation failed", { cause: grouped });
  Object.assign(grouped, { cause: outer });
  expect(mapKafkaConsumerGroupFailure(outer, "group")).toMatchObject({
    code: "AUTHORIZATION_DENIED",
    retryable: false,
  });
  expect(
    mapKafkaAdminFailure(
      new MultipleErrors("Broker response", [new ProtocolError("CLUSTER_AUTHORIZATION_FAILED")]),
      "ACL",
    ),
  ).toMatchObject({ code: "AUTHORIZATION_DENIED", retryable: false });
});
