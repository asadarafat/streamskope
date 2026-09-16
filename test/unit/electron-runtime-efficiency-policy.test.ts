import { describe, expect, it } from "vitest";

import {
  ELECTRON_RUNTIME_EFFICIENCY_POLICY,
  assertElectronProcessEvidence,
  assertPackageEfficiency,
  assertStartupEvidence,
  median,
  supportedElectronLocaleFiles,
} from "../../tools/electron-runtime-efficiency-policy";
import { auditProductionBackgroundWork } from "../../tools/audit-production-background-work";

describe("Electron runtime efficiency policy", () => {
  it("owns every measurable budget in one immutable policy", () => {
    expect(ELECTRON_RUNTIME_EFFICIENCY_POLICY).toMatchObject({
      messageStress: {
        heapDeltaBytes: 96 * 1_048_576,
        messages: 10_000,
        sustainedMessages: 100_000,
      },
      packageBytes: {
        linux: 300_000_000,
      },
      startupMs: {
        cold: 10_000,
        warm: 5_000,
      },
    });
    expect(Object.isFrozen(ELECTRON_RUNTIME_EFFICIENCY_POLICY)).toBe(true);
  });

  it("calculates a deterministic median without mutating samples", () => {
    const samples = Object.freeze([9, 1, 5, 3]);

    expect(median(samples)).toBe(4);
    expect(samples).toEqual([9, 1, 5, 3]);
    expect(() => median([])).toThrow("at least one");
  });

  it("accepts bounded native process evidence and rejects an exceeded process", () => {
    const bounded = [
      { cpuPercent: 2, processType: "Browser", workingSetBytes: 240 * 1_048_576 },
      { cpuPercent: 1, processType: "Tab", workingSetBytes: 130 * 1_048_576 },
      { cpuPercent: 1, processType: "GPU", workingSetBytes: 140 * 1_048_576 },
      { cpuPercent: 0, processType: "Utility", workingSetBytes: 80 * 1_048_576 },
    ] as const;

    expect(() => assertElectronProcessEvidence("idle", [bounded, bounded, bounded])).not.toThrow();
    expect(() =>
      assertElectronProcessEvidence("idle", [
        [
          ...bounded.slice(0, 1),
          { cpuPercent: 1, processType: "Tab", workingSetBytes: 257 * 1_048_576 },
          ...bounded.slice(2),
        ],
      ]),
    ).toThrow(/Tab.*268435456/u);
    expect(() =>
      assertElectronProcessEvidence("idle", [
        bounded.filter((process) => process.processType !== "Tab"),
      ]),
    ).toThrow(/missing.*Tab/iu);
  });

  it("enforces distinct cold and warm startup budgets", () => {
    expect(() => assertStartupEvidence({ coldMs: 9_999, warmMs: 4_999 })).not.toThrow();
    expect(() => assertStartupEvidence({ coldMs: 10_001, warmMs: 4_999 })).toThrow(
      /cold.*10001.*10000/iu,
    );
    expect(() => assertStartupEvidence({ coldMs: 9_999, warmMs: 5_001 })).toThrow(
      /warm.*5001.*5000/iu,
    );
  });

  it("enforces package bytes and platform-supported locale resources", () => {
    expect(supportedElectronLocaleFiles("linux")).toEqual(["en-GB.pak", "en-US.pak"]);
    expect(() =>
      assertPackageEfficiency({
        artifactBytes: 299_999_999,
        localeFiles: ["en-US.pak", "en-GB.pak"],
        platform: "linux",
        removedLocaleBytes: 40_000_000,
      }),
    ).not.toThrow();
    expect(() =>
      assertPackageEfficiency({
        artifactBytes: 300_000_001,
        localeFiles: ["en-US.pak"],
        platform: "linux",
        removedLocaleBytes: 40_000_000,
      }),
    ).toThrow(/300000001.*300000000/u);
    expect(() =>
      assertPackageEfficiency({
        artifactBytes: 280_000_000,
        localeFiles: ["de.pak", "en-US.pak"],
        platform: "linux",
        removedLocaleBytes: 40_000_000,
      }),
    ).toThrow("de.pak");
  });

  it("finds no polling interval or undeclared production timer owner", async () => {
    const report = await auditProductionBackgroundWork(process.cwd());

    expect(report.forbiddenUsages).toEqual([]);
    expect(report.undeclaredOwners).toEqual([]);
    expect(report.missingOwners).toEqual([]);
    expect(report.ownerFiles).toHaveLength(13);
    expect(report.ownerFiles).toContain("src/features/kafka/ui/TrustRecipeManager.tsx");
    expect(report.ownerFiles).toContain("src/platform/electron/main/electron-event-delivery.ts");
    expect(report.ownerFiles).toContain(
      "src/features/kafka/application/trust-acquisition-service.ts",
    );
  });
});
