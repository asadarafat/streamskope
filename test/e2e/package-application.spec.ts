import { access, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import {
  _electron as electron,
  expect,
  test,
  type ElectronApplication,
  type Page,
} from "@playwright/test";

import {
  ELECTRON_RUNTIME_EFFICIENCY_POLICY,
  assertElectronProcessEvidence,
  assertStartupEvidence,
} from "../../tools/electron-runtime-efficiency-policy";
import { summarizeElectronProcessEvidence } from "../../tools/electron-runtime-evidence";
import { createPerformanceEvidence } from "../../tools/performance-evidence";
import { sampleElectronProcesses } from "../support/electron-runtime";

interface VerificationPackage {
  readonly applicationFileCount: number;
  readonly applicationManifestPath: string;
  readonly arch: string;
  readonly archivePath: string;
  readonly artifactBytes: number;
  readonly bundlePath: string;
  readonly executablePath: string;
  readonly localeFiles: readonly string[];
  readonly packageBudgetBytes: number;
  readonly platform: string;
  readonly removedLocaleBytes: number;
}

async function verificationPackage(): Promise<VerificationPackage> {
  const reportPath = resolve("dist/package/verification.json");
  const report = JSON.parse(await readFile(reportPath, "utf8")) as VerificationPackage;
  await access(report.executablePath);
  await access(report.archivePath);
  await access(report.applicationManifestPath);
  return report;
}

async function launchPackage(
  packaged: VerificationPackage,
  userDataPath: string,
): Promise<{
  readonly application: ElectronApplication;
  readonly page: Page;
  readonly pageErrors: readonly string[];
  readonly startupMs: number;
}> {
  const launchArguments = [
    ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
    `--user-data-dir=${userDataPath}`,
  ];
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined,
    ),
  );
  delete environment.ELECTRON_RUN_AS_NODE;
  delete environment.STREAMSKOPE_RENDERER_URL;
  environment.NODE_ENV = "production";
  const startedAt = performance.now();
  const application = await electron.launch({
    args: launchArguments,
    env: environment,
    executablePath: packaged.executablePath,
  });
  const page = await application.firstWindow();
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => {
    pageErrors.push(error.message);
  });
  await expect(page.getByRole("banner")).toContainText("StreamSkope");
  return {
    application,
    page,
    pageErrors,
    startupMs: performance.now() - startedAt,
  };
}

test("launches and measures the inspected production package", async () => {
  // The startup policy below still enforces the cold/warm launch budgets. This
  // larger harness timeout also covers two clean launches, process sampling,
  // manifest verification, and orderly shutdown on slower CI workers.
  test.setTimeout(60_000);
  const packaged = await verificationPackage();
  const temporaryDirectory = await mkdtemp(join(tmpdir(), "streamskope-package-runtime-"));
  const userDataPath = join(temporaryDirectory, "user-data");
  let coldApplication: ElectronApplication | undefined;
  let warmApplication: ElectronApplication | undefined;

  try {
    const cold = await launchPackage(packaged, userDataPath);
    coldApplication = cold.application;
    await expect(cold.page.getByLabel("Connection status")).toContainText("Disconnected");
    await coldApplication.close();
    coldApplication = undefined;

    const warm = await launchPackage(packaged, userDataPath);
    warmApplication = warm.application;
    const startup = { coldMs: cold.startupMs, warmMs: warm.startupMs };
    assertStartupEvidence(startup);
    const idleSamples = await sampleElectronProcesses(warm.application, warm.page);
    assertElectronProcessEvidence("idle", idleSamples);
    const idle = summarizeElectronProcessEvidence(idleSamples);

    const page = warm.page;
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    await expect(page.getByRole("button", { name: "Configure ad hoc connection" })).toHaveCount(0);
    await expect(page.getByText(/OS-protected profiles|Profile storage unavailable/)).toBeVisible();
    expect(await page.evaluate(() => globalThis.location.href)).toBe("streamskope://app/");
    const contentManifest = JSON.parse(
      await readFile(packaged.applicationManifestPath, "utf8"),
    ) as {
      readonly files: readonly { readonly path: string; readonly sha256: string }[];
      readonly schemaVersion: number;
    };
    expect(contentManifest.schemaVersion).toBe(1);
    expect(contentManifest.files).toHaveLength(packaged.applicationFileCount);
    expect(contentManifest.files.map((entry) => entry.path)).toContain("dist/renderer/index.html");
    expect(contentManifest.files.every((entry) => /^[a-f0-9]{64}$/u.test(entry.sha256))).toBe(true);
    expect(packaged.platform).toBe(process.platform);
    expect(packaged.arch).toBe(process.arch);
    expect(packaged.artifactBytes).toBeLessThanOrEqual(packaged.packageBudgetBytes);
    expect(packaged.localeFiles.length).toBeGreaterThan(0);
    if (packaged.platform === "darwin") {
      expect(packaged.removedLocaleBytes).toBeGreaterThanOrEqual(0);
    } else {
      expect(packaged.removedLocaleBytes).toBeGreaterThan(0);
    }
    expect([...cold.pageErrors, ...warm.pageErrors]).toEqual([]);
    await writeFile(
      resolve("dist/package/runtime-evidence.json"),
      `${JSON.stringify(
        createPerformanceEvidence({
          capturedAt: new Date().toISOString(),
          check: "packaged-electron-runtime",
          command: "npm run package:verify",
          evidence: {
            idle,
            idlePolicyCpuMedianPercent: ELECTRON_RUNTIME_EFFICIENCY_POLICY.cpuMedianPercent.idle,
            processWorkingSetPolicyBytes: ELECTRON_RUNTIME_EFFICIENCY_POLICY.processWorkingSetBytes,
            startup,
            startupPolicyMs: ELECTRON_RUNTIME_EFFICIENCY_POLICY.startupMs,
            totalWorkingSetPolicyBytes: ELECTRON_RUNTIME_EFFICIENCY_POLICY.totalWorkingSetBytes,
          },
          outcome: "passed",
          runtime: {
            arch: process.arch,
            node: process.version,
            platform: process.platform,
          },
          sampleMethod:
            "Cold and warm packaged launches followed by three Electron app.getAppMetrics samples at 500 millisecond intervals.",
        }),
        null,
        2,
      )}\n`,
      "utf8",
    );
  } finally {
    await coldApplication?.close();
    await warmApplication?.close();
    await rm(temporaryDirectory, { force: true, recursive: true });
  }
});
