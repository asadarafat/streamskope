import { execFileSync, spawn } from "node:child_process";
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";

import { Admin, Consumer, type BaseOptions, type MessagesStream } from "@platformatic/kafka";
import { describe, expect, it } from "vitest";

import type { FixtureConnection } from "../support/kafka-fixture";
import {
  fetchFixtureToken,
  loadFixtureConfig,
  loadFixtureConnection,
} from "../support/kafka-fixture";

const require = createRequire(import.meta.url);
const tsxCli = require.resolve("tsx/cli");
const usesExternalFixture = process.env.STREAMSKOPE_TEST_KAFKA_ENDPOINT !== undefined;

function inspectContainer(format: string, containerName: string): string {
  return execFileSync("docker", ["inspect", "--format", format, containerName], {
    encoding: "utf8",
  }).trim();
}

function kafkaOptions(connection: FixtureConnection, ca: string, token: string): BaseOptions {
  return {
    bootstrapBrokers: [connection.kafkaEndpoint],
    clientId: "streamskope-fixture-verification",
    connectTimeout: 5_000,
    requestTimeout: 5_000,
    retries: 0,
    sasl: {
      mechanism: "OAUTHBEARER",
      token,
    },
    tls: { ca: [ca], rejectUnauthorized: true },
  };
}

describe("real secure Kafka fixture", () => {
  it.skipIf(usesExternalFixture)(
    "deploys bounded liveness, heap and producer policy in the owned fixture",
    () => {
      const fixtureName = process.env.STREAMSKOPE_TEST_FIXTURE_NAME ?? "streamskope-kafka";
      const containers = [
        `clab-${fixtureName}-broker`,
        `clab-${fixtureName}-oauth`,
        `clab-${fixtureName}-schema-registry`,
      ];

      for (const container of containers) {
        expect(inspectContainer("{{.State.Health.Status}}", container)).toBe("healthy");
        const health = JSON.parse(inspectContainer("{{json .Config.Healthcheck}}", container)) as {
          readonly Interval?: unknown;
          readonly Retries?: unknown;
          readonly Test?: unknown;
          readonly Timeout?: unknown;
        };
        expect(health).toMatchObject({
          Interval: 10_000_000_000,
          Retries: 6,
          Timeout: 2_000_000_000,
        });
        expect(health.Test).toEqual(
          expect.arrayContaining(["awk", "/proc/net/tcp", "/proc/net/tcp6"]),
        );
        expect(JSON.stringify(health.Test)).not.toContain("/dev/tcp");
        expect(health.Test).not.toEqual(
          expect.arrayContaining([
            expect.stringMatching(/kafka-broker-api-versions|python3?\s+-c/u),
          ]),
        );
      }

      const broker = containers[0] ?? "";
      const environment = JSON.parse(inspectContainer("{{json .Config.Env}}", broker)) as unknown;
      expect(environment).toEqual(
        expect.arrayContaining(["KAFKA_HEAP_OPTS=-Xms256M -Xmx512M", "PRODUCER_ENABLED=false"]),
      );
      const processes = execFileSync("docker", ["top", broker, "-eo", "pid,args"], {
        encoding: "utf8",
      });
      expect(processes).toMatch(/java\s+-Xms256M\s+-Xmx512M[\s\S]*kafka\.Kafka/u);
      expect(processes).not.toContain("ConsoleProducer");
    },
  );

  it("terminates the external attachment command after closing its readiness client", async () => {
    const connection = await loadFixtureConnection();
    const child = spawn(
      process.execPath,
      [
        tsxCli,
        resolve(process.cwd(), "tools/kafka-fixture/cli.ts"),
        "attach",
        "--kafka",
        connection.kafkaEndpoint,
        "--oauth",
        connection.oauthEndpoint,
        "--ca",
        connection.caPath,
      ],
      {
        cwd: process.cwd(),
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let output = "";
    let errorOutput = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      output = `${output}${chunk}`.slice(-8_192);
    });
    child.stderr.on("data", (chunk: string) => {
      errorOutput = `${errorOutput}${chunk}`.slice(-8_192);
    });

    const result = await new Promise<{ readonly code: number | null; readonly timedOut: boolean }>(
      (resolveResult, reject) => {
        let timedOut = false;
        const timeout = setTimeout(() => {
          timedOut = true;
          child.kill("SIGTERM");
        }, 8_000);
        child.once("error", (error) => {
          clearTimeout(timeout);
          reject(error);
        });
        child.once("close", (code) => {
          clearTimeout(timeout);
          resolveResult({ code, timedOut });
        });
      },
    );

    expect(result.timedOut, errorOutput).toBe(false);
    expect(result.code, errorOutput).toBe(0);
    expect(JSON.parse(output)).toMatchObject({ ownership: "external" });
  }, 12_000);

  it("lists the test topic and consumes the deterministic seed through TLS and OAuth", async () => {
    const config = await loadFixtureConfig();
    const connection = await loadFixtureConnection();
    const ca = await readFile(connection.caPath, "utf8");
    const token = await fetchFixtureToken(connection, config);
    const options = kafkaOptions(connection, ca, token);
    const admin = new Admin(options);
    let consumer: Consumer | undefined;
    let stream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | undefined;

    try {
      expect(await admin.listTopics()).toContain(config.topic);

      consumer = new Consumer({
        ...options,
        clientId: "streamskope-fixture-consumer",
        groupId: `streamskope-verification-${randomUUID()}`,
        retries: 5,
        retryDelay: 200,
      });
      stream = await consumer.consume({
        fallbackMode: "earliest",
        mode: "earliest",
        topics: [config.topic],
      });
      const activeStream = stream;
      const message = (async (): Promise<string> => {
        for await (const kafkaMessage of activeStream) {
          const value = kafkaMessage.value.toString("utf8");
          if (value === config.seedPayload) {
            return value;
          }
        }
        throw new Error("Kafka message stream ended before the seed record.");
      })();
      let timeoutId: NodeJS.Timeout | undefined;
      const timeout = new Promise<never>((_resolve, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error("Timed out waiting for the seed record.")),
          10_000,
        );
      });

      try {
        await expect(Promise.race([message, timeout])).resolves.toBe(config.seedPayload);
      } finally {
        if (timeoutId !== undefined) {
          clearTimeout(timeoutId);
        }
      }
    } finally {
      await stream?.close();
      await consumer?.close();
      await admin.close();
    }
  }, 20_000);

  it("rejects missing and malformed Registry bearers but accepts the fixture token", async () => {
    const config = await loadFixtureConfig();
    const connection = await loadFixtureConnection();
    if (connection.schemaRegistryEndpoint === undefined) {
      throw new Error("The owned fixture has no Schema Registry endpoint evidence.");
    }
    const endpoint = `${connection.schemaRegistryEndpoint}/subjects`;

    const missing = await fetch(endpoint, { signal: AbortSignal.timeout(5_000) });
    expect(missing.status).toBe(401);

    const malformed = await fetch(endpoint, {
      headers: { authorization: "Bearer malformed-fixture-token" },
      signal: AbortSignal.timeout(5_000),
    });
    expect(malformed.status).toBe(401);

    const token = await fetchFixtureToken(connection, config);
    const valid = await fetch(endpoint, {
      headers: { authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(5_000),
    });
    expect(valid.status).toBe(200);
    await expect(valid.json()).resolves.toContain(config.schemaSubject);
  });
});
