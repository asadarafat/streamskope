import { mkdtemp, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { expect, it } from "vitest";

import { KafkaFixtureError } from "../../aio-kafka/fixtures/error";
import { startKafkaFixture } from "../../aio-kafka/fixtures/broker";
import { disposeFixtureResources } from "../../aio-kafka/fixtures/resources";

it("retains a safe Java failure identity without command arguments or exception messages", () => {
  const password = "fixture-secret-sentinel";
  const error = new KafkaFixtureError(
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
    new KafkaFixtureError("generate fixture TLS key", { code: "ENOENT" }, "", "failed").diagnostic,
  ).toMatchObject({ code: "ENOENT", cleanup: "failed" });
  const untrusted = new KafkaFixtureError("prepare broker runtime", {
    code: "secret-value",
    message: "secret-value",
  });
  expect(untrusted.diagnostic).toEqual({
    phase: "prepare broker runtime",
    exceptionClasses: [],
    cleanup: "not started",
  });
  expect(JSON.stringify(untrusted)).not.toContain("secret-value");
});

it("records a JavaScript image request timeout without inventing a process exit code", () => {
  const error = new KafkaFixtureError(
    "prepare broker runtime",
    new DOMException("private download details", "TimeoutError"),
  );
  expect(error.diagnostic).toEqual({
    phase: "prepare broker runtime",
    exceptionClasses: [],
    code: "ETIMEDOUT",
    cleanup: "not started",
  });
  expect(JSON.stringify(error)).not.toContain("private download details");
});

it("does not report a cancelled JavaScript request as a process exit", () => {
  const error = new KafkaFixtureError(
    "prepare broker runtime",
    new DOMException("private cancellation details", "AbortError"),
  );
  expect(error.diagnostic).not.toHaveProperty("exitCode");
  expect(error.diagnostic).not.toHaveProperty("code");
  expect(JSON.stringify(error)).not.toContain("private cancellation details");
});

it("removes owned data and disposes the driver when configuration fails before startup", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aio-data-test-"));
  let driverDisposed = false;
  await expect(
    startKafkaFixture({
      prepare: () =>
        Promise.resolve({
          metadata: { backend: "container" as const, kafkaVersion: "4.3.1" },
          tool: () => Promise.resolve(),
          start: () => Promise.resolve(),
          exited: () => Promise.resolve(true),
          logs: () => Promise.resolve(""),
          dispose: () => {
            driverDisposed = true;
            return Promise.resolve();
          },
        }),
      directory: () => Promise.resolve(directory),
      configuration: () => Promise.reject(new Error("private configuration failure")),
    }),
  ).rejects.toMatchObject({ diagnostic: { phase: "initialize fixture", cleanup: "passed" } });
  expect(driverDisposed).toBe(true);
  await expect(stat(directory)).rejects.toMatchObject({ code: "ENOENT" });
});

it("still removes later resources when closing an earlier resource fails", async () => {
  const directory = await mkdtemp(join(tmpdir(), "native-cleanup-test-"));
  let subsequentClose = false;
  await expect(
    disposeFixtureResources([
      (): Promise<void> => Promise.reject(new Error("simulated stream close failure")),
      (): Promise<void> => {
        subsequentClose = true;
        return Promise.resolve();
      },
      (): Promise<void> => rm(directory, { recursive: true, force: true }),
    ]),
  ).rejects.toThrow("AIO Kafka fixture cleanup failed");
  expect(subsequentClose).toBe(true);
  await expect(stat(directory)).rejects.toMatchObject({ code: "ENOENT" });
});

it("retains private fixture material when broker cleanup is unconfirmed", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aio-unresolved-cleanup-"));
  try {
    await expect(
      startKafkaFixture({
        directory: () => Promise.resolve(directory),
        configuration: () => Promise.reject(new Error("private fixture configuration")),
        prepare: () =>
          Promise.resolve({
            metadata: { backend: "container", kafkaVersion: "4.3.1" },
            tool: () => Promise.resolve(),
            start: () => Promise.resolve(),
            exited: () => Promise.resolve(true),
            logs: () => Promise.resolve(""),
            dispose: () => Promise.reject(new Error("Docker removal not confirmed")),
          }),
      }),
    ).rejects.toMatchObject({ diagnostic: { phase: "initialize fixture", cleanup: "failed" } });
    expect((await stat(directory)).isDirectory()).toBe(true);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
