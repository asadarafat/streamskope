import { describe, expect, it } from "vitest";

import {
  evaluateStreamReplayQualification,
  STREAM_REPLAY_QUALIFICATION_BUDGET,
} from "../support/stream-replay-qualification";

const baseline = {
  boundsPassed: true,
  cpuPercentOneCore: 24,
  generated: 900_000,
  hostDisplayDrops: 4_388,
  ingestToReducerP95Ms: 420,
  ingestToReducerP99Ms: 717,
  offered: 900_000,
  peakRss: 357 * 1024 * 1024,
  eventLoopP99Ms: 36,
};

describe("stream replay qualification budgets", () => {
  it("accepts the recorded 15-minute baseline and exposes the measured limits", () => {
    expect(evaluateStreamReplayQualification(baseline)).toEqual({ passed: true, failures: [] });
    expect(STREAM_REPLAY_QUALIFICATION_BUDGET.minimumGeneratedRatio).toBe(0.99);
  });

  it.each([
    ["throughput", { generated: 800_000 }],
    ["CPU", { cpuPercentOneCore: 61 }],
    ["resident memory", { peakRss: 769 * 1024 * 1024 }],
    ["event-loop delay", { eventLoopP99Ms: 151 }],
    ["display drops", { hostDisplayDrops: 18_001 }],
    ["delivery latency", { ingestToReducerP95Ms: 1_001 }],
    ["tail delivery latency", { ingestToReducerP99Ms: 2_001 }],
    ["unbounded processing", { boundsPassed: false }],
  ])("rejects a replay that exceeds the %s budget", (_name, regression) => {
    expect(evaluateStreamReplayQualification({ ...baseline, ...regression })).toMatchObject({
      passed: false,
    });
  });

  it("rejects invalid numeric evidence", () => {
    expect(
      evaluateStreamReplayQualification({ ...baseline, cpuPercentOneCore: Number.NaN }),
    ).toMatchObject({ passed: false });
  });
});
