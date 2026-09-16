export type VerifiedElectronPlatform = "darwin" | "linux" | "win32";
export type ElectronProcessType = "Browser" | "GPU" | "Other" | "Tab" | "Utility";
export type ElectronResourceState = "connected" | "idle";

const MEBIBYTE = 1_048_576;

export const ELECTRON_RUNTIME_EFFICIENCY_POLICY = Object.freeze({
  backgroundWorkOwnerFiles: Object.freeze([
    "src/features/kafka/application/session.ts",
    "src/features/kafka/application/trust-acquisition-service.ts",
    "src/features/kafka/facade/facade-support.ts",
    "src/features/kafka/engine/engine.ts",
    "src/features/kafka/engine/platformatic-latency.ts",
    "src/features/kafka/ui/LatencyWorkspace.tsx",
    "src/features/kafka/ui/OperationalPreferencesDialog.tsx",
    "src/features/kafka/ui/TrustRecipeManager.tsx",
    "src/features/kafka/ui/stream-monitor-observer.ts",
    "src/platform/electron/main/electron-profile-protection.ts",
    "src/platform/electron/main/electron-event-delivery.ts",
    "src/platform/electron/main/ssh2-kafka-remote-session.ts",
    "src/platform/electron/main/ssh2-kafka-remote-trust-adapter.ts",
  ]),
  cpuMedianPercent: Object.freeze({
    connected: 40,
    idle: 25,
  }),
  messageStress: Object.freeze({
    heapDeltaBytes: 96 * MEBIBYTE,
    messages: 10_000,
    sustainedMessages: 100_000,
  }),
  packageBytes: Object.freeze({
    darwin: 420_000_000,
    linux: 300_000_000,
    win32: 360_000_000,
  }),
  processWorkingSetBytes: Object.freeze({
    Browser: 384 * MEBIBYTE,
    GPU: 256 * MEBIBYTE,
    Other: 192 * MEBIBYTE,
    Tab: 256 * MEBIBYTE,
    Utility: 192 * MEBIBYTE,
  }),
  startupMs: Object.freeze({
    cold: 10_000,
    warm: 5_000,
  }),
  totalWorkingSetBytes: 1_024 * MEBIBYTE,
});

export interface ElectronProcessEvidence {
  readonly cpuPercent: number;
  readonly processType: ElectronProcessType;
  readonly workingSetBytes: number;
}

export interface PackageEfficiencyEvidence {
  readonly artifactBytes: number;
  readonly localeFiles: readonly string[];
  readonly platform: VerifiedElectronPlatform;
  readonly removedLocaleBytes: number;
}

export interface StartupEfficiencyEvidence {
  readonly coldMs: number;
  readonly warmMs: number;
}

function finiteNonNegative(value: number, field: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${field} must be finite and non-negative.`);
  }
}

export function median(values: readonly number[]): number {
  if (values.length === 0) {
    throw new RangeError("Median requires at least one sample.");
  }
  for (const [index, value] of values.entries()) {
    finiteNonNegative(value, `samples[${String(index)}]`);
  }
  const ordered = [...values].sort((left, right) => left - right);
  const middle = Math.floor(ordered.length / 2);
  const upper = ordered[middle];
  if (upper === undefined) {
    throw new RangeError("Median requires at least one sample.");
  }
  if (ordered.length % 2 === 1) {
    return upper;
  }
  const lower = ordered[middle - 1];
  if (lower === undefined) {
    throw new RangeError("Median requires at least one sample.");
  }
  return (lower + upper) / 2;
}

export function assertElectronProcessEvidence(
  state: ElectronResourceState,
  samples: readonly (readonly ElectronProcessEvidence[])[],
): void {
  if (samples.length === 0) {
    throw new Error(`Electron ${state} evidence requires at least one process sample.`);
  }
  const combinedCpuSamples: number[] = [];
  for (const [sampleIndex, sample] of samples.entries()) {
    if (sample.length === 0) {
      throw new Error(`Electron ${state} sample ${String(sampleIndex)} contains no processes.`);
    }
    let combinedCpu = 0;
    let combinedWorkingSet = 0;
    const observedProcessTypes = new Set<ElectronProcessType>();
    for (const process of sample) {
      finiteNonNegative(process.cpuPercent, `${process.processType}.cpuPercent`);
      finiteNonNegative(process.workingSetBytes, `${process.processType}.workingSetBytes`);
      const processBudget =
        ELECTRON_RUNTIME_EFFICIENCY_POLICY.processWorkingSetBytes[process.processType];
      if (process.workingSetBytes > processBudget) {
        throw new Error(
          `${process.processType} working set ${String(process.workingSetBytes)} exceeds ${String(processBudget)} bytes.`,
        );
      }
      combinedCpu += process.cpuPercent;
      combinedWorkingSet += process.workingSetBytes;
      observedProcessTypes.add(process.processType);
    }
    for (const requiredProcessType of ["Browser", "Tab", "GPU"] as const) {
      if (!observedProcessTypes.has(requiredProcessType)) {
        throw new Error(
          `Electron ${state} sample ${String(sampleIndex)} is missing required ${requiredProcessType} process evidence.`,
        );
      }
    }
    if (combinedWorkingSet > ELECTRON_RUNTIME_EFFICIENCY_POLICY.totalWorkingSetBytes) {
      throw new Error(
        `Combined working set ${String(combinedWorkingSet)} exceeds ${String(ELECTRON_RUNTIME_EFFICIENCY_POLICY.totalWorkingSetBytes)} bytes.`,
      );
    }
    combinedCpuSamples.push(combinedCpu);
  }
  const medianCpu = median(combinedCpuSamples);
  const cpuBudget = ELECTRON_RUNTIME_EFFICIENCY_POLICY.cpuMedianPercent[state];
  if (medianCpu > cpuBudget) {
    throw new Error(
      `${state} median combined CPU ${String(medianCpu)} exceeds ${String(cpuBudget)} percentage points.`,
    );
  }
}

export function assertStartupEvidence(evidence: StartupEfficiencyEvidence): void {
  for (const startupKind of ["cold", "warm"] as const) {
    const value = startupKind === "cold" ? evidence.coldMs : evidence.warmMs;
    finiteNonNegative(value, `${startupKind}Ms`);
    const budget = ELECTRON_RUNTIME_EFFICIENCY_POLICY.startupMs[startupKind];
    if (value > budget) {
      throw new Error(
        `${startupKind} startup ${String(value)} milliseconds exceeds ${String(budget)} milliseconds.`,
      );
    }
  }
}

const SUPPORTED_ELECTRON_LOCALES: Readonly<Record<VerifiedElectronPlatform, readonly string[]>> =
  Object.freeze({
    darwin: Object.freeze(["en.lproj", "en_GB.lproj"]),
    linux: Object.freeze(["en-GB.pak", "en-US.pak"]),
    win32: Object.freeze(["en-GB.pak", "en-US.pak"]),
  });

export function supportedElectronLocaleFiles(
  platform: VerifiedElectronPlatform,
): readonly string[] {
  return SUPPORTED_ELECTRON_LOCALES[platform];
}

export function assertPackageEfficiency(evidence: PackageEfficiencyEvidence): void {
  finiteNonNegative(evidence.artifactBytes, "artifactBytes");
  finiteNonNegative(evidence.removedLocaleBytes, "removedLocaleBytes");
  const packageBudget = ELECTRON_RUNTIME_EFFICIENCY_POLICY.packageBytes[evidence.platform];
  if (evidence.artifactBytes > packageBudget) {
    throw new Error(
      `Package size ${String(evidence.artifactBytes)} exceeds ${String(packageBudget)} bytes for ${evidence.platform}.`,
    );
  }
  const supported = new Set(supportedElectronLocaleFiles(evidence.platform));
  const unexpected = evidence.localeFiles.find((locale) => !supported.has(locale));
  if (unexpected !== undefined) {
    throw new Error(`Package contains unsupported Electron locale ${unexpected}.`);
  }
  if (evidence.localeFiles.length === 0) {
    throw new Error("Package contains no supported Electron locale resource.");
  }
}
