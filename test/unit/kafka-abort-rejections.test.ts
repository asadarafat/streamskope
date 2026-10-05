import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { describe, expect, it } from "vitest";

const runProcess = promisify(execFile);
const fixture = fileURLToPath(new URL("../support/abort-operation-process.ts", import.meta.url));

async function evidence(scenario: "engine-pre-abort" | "latency-send-abort"): Promise<unknown> {
  const { stdout, stderr } = await runProcess(
    process.execPath,
    ["--unhandled-rejections=throw", "--import", "tsx", fixture, scenario],
    { cwd: process.cwd(), encoding: "utf8", timeout: 10_000 },
  );
  expect(stderr).toBe("");
  const parsed: unknown = JSON.parse(stdout);
  return parsed;
}

describe("cancelled Kafka driver promises", () => {
  it("observes a rejected metadata read after a pre-aborted request and closes its admin once", async () => {
    expect(await evidence("engine-pre-abort")).toEqual({
      code: "CANCELLED",
      listCalls: 2,
      adminCloseCalls: 1,
      unhandledRejections: 0,
    });
  });

  it("observes a rejected send that synchronously cancels its probe and closes the owned producer and reader", async () => {
    expect(await evidence("latency-send-abort")).toEqual({
      code: "CANCELLED",
      consumerCalls: 1,
      sendCalls: 1,
      streamCloseCalls: 1,
      producerCloseCalls: 1,
      producerForcedClose: true,
      pumpEnded: true,
      unhandledRejections: 0,
    });
  });
});
