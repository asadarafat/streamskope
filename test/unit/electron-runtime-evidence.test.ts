import { describe, expect, it } from "vitest";

import {
  electronProcessEvidenceFromMetrics,
  summarizeElectronProcessEvidence,
} from "../../tools/electron-runtime-evidence";

describe("Electron runtime evidence conversion", () => {
  it("converts Electron working-set KiB to bytes without dropping unknown process costs", () => {
    expect(
      electronProcessEvidenceFromMetrics([
        {
          cpu: { percentCPUUsage: 2.5 },
          memory: { workingSetSize: 100 },
          type: "Browser",
        },
        {
          cpu: { percentCPUUsage: 0.5 },
          memory: { workingSetSize: 25 },
          type: "Zygote",
        },
      ]),
    ).toEqual([
      {
        cpuPercent: 2.5,
        processType: "Browser",
        workingSetBytes: 102_400,
      },
      {
        cpuPercent: 0.5,
        processType: "Other",
        workingSetBytes: 25_600,
      },
    ]);
  });

  it("reports combined samples and median CPU without hiding individual processes", () => {
    const processes = [
      { cpuPercent: 2, processType: "Browser" as const, workingSetBytes: 100 },
      { cpuPercent: 3, processType: "Tab" as const, workingSetBytes: 200 },
      { cpuPercent: 1, processType: "GPU" as const, workingSetBytes: 300 },
    ];

    expect(summarizeElectronProcessEvidence([processes, processes])).toEqual({
      medianCombinedCpuPercent: 6,
      samples: [
        {
          combinedCpuPercent: 6,
          combinedWorkingSetBytes: 600,
          processes,
        },
        {
          combinedCpuPercent: 6,
          combinedWorkingSetBytes: 600,
          processes,
        },
      ],
    });
  });
});
