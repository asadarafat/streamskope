import { expect, it } from "vitest";

import { NativeKafkaFixtureError } from "../support/native-fixture-error";
import { nativeArchiveExtractor } from "../support/native-kafka-fixture";

it("retains a safe Java failure identity without command arguments or exception messages", () => {
  const password = "fixture-secret-sentinel";
  const error = new NativeKafkaFixtureError(
    "format broker storage",
    { code: 1, message: `java -storepass ${password}`, stderr: password },
    `java.nio.file.AccessDeniedException: C:/private/${password}\n` +
      `Caused by: org.apache.kafka.common.KafkaException: ${password}\n` +
      `java.nio.file.AccessDeniedException: repeated\n${password}Error`,
    "passed",
  );
  expect(error.diagnostic).toEqual({
    phase: "format broker storage",
    exceptionClasses: [
      "java.nio.file.AccessDeniedException",
      "org.apache.kafka.common.KafkaException",
    ],
    exitCode: 1,
    cleanup: "passed",
  });
  expect(JSON.stringify(error)).not.toContain(password);
  expect(error.message).not.toContain(password);
});

it("retains only allowlisted system codes and records cleanup failures separately", () => {
  expect(
    new NativeKafkaFixtureError("generate fixture TLS key", { code: "ENOENT" }, "", "failed")
      .diagnostic,
  ).toMatchObject({ code: "ENOENT", cleanup: "failed" });
  const untrusted = new NativeKafkaFixtureError("prepare Kafka distribution", {
    code: "secret-value",
    message: "secret-value",
  });
  expect(untrusted.diagnostic).toEqual({
    phase: "prepare Kafka distribution",
    exceptionClasses: [],
    cleanup: "not started",
  });
  expect(JSON.stringify(untrusted)).not.toContain("secret-value");
});

it("uses native Windows archive extraction without depending on Git or MSYS PATH tools", () => {
  expect(nativeArchiveExtractor("win32", "C:\\Windows")).toBe("C:\\Windows\\System32\\tar.exe");
  expect(nativeArchiveExtractor("win32", "D:\\Windows")).toBe("D:\\Windows\\System32\\tar.exe");
  expect(() => nativeArchiveExtractor("win32", "relative")).toThrow("absolute SystemRoot");
  expect(nativeArchiveExtractor("darwin", "irrelevant")).toBe("tar");
  expect(nativeArchiveExtractor("linux", "irrelevant")).toBe("tar");
});
