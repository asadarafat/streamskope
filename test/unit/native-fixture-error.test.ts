import { mkdtemp, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { expect, it } from "vitest";

import { NativeKafkaFixtureError } from "../support/native-fixture-error";
import {
  nativeArchiveExtractor,
  startNativeKafkaFixture,
  disposeNativeFixtureResources,
} from "../support/native-kafka-fixture";

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

it("records a JavaScript download timeout without inventing a process exit code", () => {
  const error = new NativeKafkaFixtureError(
    "prepare Kafka distribution",
    new DOMException("private download details", "TimeoutError"),
  );
  expect(error.diagnostic).toEqual({
    phase: "prepare Kafka distribution",
    exceptionClasses: [],
    code: "ETIMEDOUT",
    cleanup: "not started",
  });
  expect(JSON.stringify(error)).not.toContain("private download details");
});

it("does not report a cancelled JavaScript request as a process exit", () => {
  const error = new NativeKafkaFixtureError(
    "prepare Kafka distribution",
    new DOMException("private cancellation details", "AbortError"),
  );
  expect(error.diagnostic).not.toHaveProperty("exitCode");
  expect(error.diagnostic).not.toHaveProperty("code");
  expect(JSON.stringify(error)).not.toContain("private cancellation details");
});

it("uses native Windows archive extraction without depending on Git or MSYS PATH tools", () => {
  expect(nativeArchiveExtractor("win32", "C:\\Windows")).toBe("C:\\Windows\\System32\\tar.exe");
  expect(nativeArchiveExtractor("win32", "D:\\Windows")).toBe("D:\\Windows\\System32\\tar.exe");
  expect(() => nativeArchiveExtractor("win32", "relative")).toThrow("absolute SystemRoot");
  expect(nativeArchiveExtractor("darwin", "irrelevant")).toBe("tar");
  expect(nativeArchiveExtractor("linux", "irrelevant")).toBe("tar");
});

it("removes both owned directories when configuration fails before OAuth or JVM startup", async () => {
  const extraction = await mkdtemp(join(tmpdir(), "native-extract-test-"));
  const directory = await mkdtemp(join(tmpdir(), "native-data-test-"));
  await expect(
    startNativeKafkaFixture({
      distribution: () => Promise.resolve(extraction),
      directory: () => Promise.resolve(directory),
      configuration: () => Promise.reject(new Error("private configuration failure")),
    }),
  ).rejects.toMatchObject({ diagnostic: { phase: "initialize fixture", cleanup: "passed" } });
  await expect(stat(extraction)).rejects.toMatchObject({ code: "ENOENT" });
  await expect(stat(directory)).rejects.toMatchObject({ code: "ENOENT" });
});

it("still removes later resources when closing an earlier resource fails", async () => {
  const directory = await mkdtemp(join(tmpdir(), "native-cleanup-test-"));
  let subsequentClose = false;
  await expect(
    disposeNativeFixtureResources([
      (): Promise<void> => Promise.reject(new Error("simulated stream close failure")),
      (): Promise<void> => {
        subsequentClose = true;
        return Promise.resolve();
      },
      (): Promise<void> => rm(directory, { recursive: true, force: true }),
    ]),
  ).rejects.toThrow("Native fixture cleanup failed");
  expect(subsequentClose).toBe(true);
  await expect(stat(directory)).rejects.toMatchObject({ code: "ENOENT" });
});
