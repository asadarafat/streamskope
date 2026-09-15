import { describe, expect, it } from "vitest";

import { mapKafkaAdminFailure } from "../../src/kafka/engine/failure";

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
