import type {
  ElectronProcessEvidence,
  ElectronProcessType,
} from "./electron-runtime-efficiency-policy";
import { median } from "./electron-runtime-efficiency-policy";

export interface ElectronRuntimeMetric {
  readonly cpu: {
    readonly percentCPUUsage: number;
  };
  readonly memory: {
    readonly workingSetSize: number;
  };
  readonly type: string;
}

function processType(type: string): ElectronProcessType {
  switch (type) {
    case "Browser":
    case "GPU":
    case "Tab":
    case "Utility":
      return type;
    default:
      return "Other";
  }
}

export function electronProcessEvidenceFromMetrics(
  metrics: readonly ElectronRuntimeMetric[],
): ElectronProcessEvidence[] {
  return metrics.map((metric) => ({
    cpuPercent: metric.cpu.percentCPUUsage,
    processType: processType(metric.type),
    workingSetBytes: metric.memory.workingSetSize * 1_024,
  }));
}

export interface ElectronProcessSampleEvidence {
  readonly combinedCpuPercent: number;
  readonly combinedWorkingSetBytes: number;
  readonly processes: readonly ElectronProcessEvidence[];
}

export interface ElectronProcessSummaryEvidence {
  readonly medianCombinedCpuPercent: number;
  readonly samples: readonly ElectronProcessSampleEvidence[];
}

export function summarizeElectronProcessEvidence(
  samples: readonly (readonly ElectronProcessEvidence[])[],
): ElectronProcessSummaryEvidence {
  const summarized = samples.map((processes) => ({
    combinedCpuPercent: processes.reduce((total, process) => total + process.cpuPercent, 0),
    combinedWorkingSetBytes: processes.reduce(
      (total, process) => total + process.workingSetBytes,
      0,
    ),
    processes,
  }));
  return {
    medianCombinedCpuPercent: median(summarized.map((sample) => sample.combinedCpuPercent)),
    samples: summarized,
  };
}
