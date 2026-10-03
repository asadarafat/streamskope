import { MultipleErrors, ProtocolError } from "@platformatic/kafka";
import { describe, expect, it } from "vitest";

import {
  mapKafkaAdminFailure,
  mapKafkaConsumerGroupFailure,
} from "../../src/features/kafka/engine/failure";

describe("Kafka administration failure mapping", () => {
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
