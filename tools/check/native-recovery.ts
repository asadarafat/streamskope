import { mkdtemp, mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { startNativeKafkaFixture } from "../../test/support/native-kafka-fixture";

import {
  downloadNativeInstaller,
  installerName,
  loadCandidateInstaller,
  nativeCommand,
  uninstallNativeRecovery,
  writeRecoveryPlan,
  type NativeRecoveryPlan,
} from "./native-installers";

function failureCode(error: unknown): string {
  const code =
    error !== null && typeof error === "object" && "code" in error ? error.code : undefined;
  return typeof code === "string" &&
    ["ENOENT", "EACCES", "EPERM", "EBUSY", "ETIMEDOUT", "ENOSPC", "EIO", "ENOTEMPTY"].includes(code)
    ? code
    : "VERIFICATION_FAILED";
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const candidate =
    args.length === 6 && args[2] === "--candidate" && args[4] === "--candidate-version";
  const published = args.length === 4 && args[2] === "--to";
  if (args[0] !== "--from" || (!candidate && !published)) {
    throw new Error(
      "Usage: native-recovery.ts --from VERSION (--to VERSION | --candidate dist/installers/FILE --candidate-version 0.0.0-dev)",
    );
  }
  const from = args[1]!;
  const to = candidate ? args[5]! : args[3]!;
  if (from === to) throw new Error("Choose distinct baseline and target versions.");
  installerName(from);
  installerName(to);
  if (process.platform === "win32" && process.env.GITHUB_ACTIONS !== "true") {
    throw new Error("Windows installer recovery requires a disposable GitHub Actions account.");
  }
  const fixtureVariables = [
    "STREAMSKOPE_TEST_KAFKA_ENDPOINT",
    "STREAMSKOPE_TEST_OAUTH_ENDPOINT",
    "STREAMSKOPE_TEST_CA_PATH",
  ];
  const supplied = fixtureVariables.filter((name) => process.env[name]);
  if (supplied.length !== 0 && supplied.length !== fixtureVariables.length) {
    throw new Error(
      "Provide all three STREAMSKOPE_TEST_* connection variables, or none to start the owned fixture.",
    );
  }
  const output = resolve("dist", "native-recovery");
  const outputPath = join(output, `${process.platform}-${process.arch}.json`);
  const reportPath = resolve("test-results", "electron", "native-recovery.json");
  await rm(outputPath, { force: true });
  await rm(reportPath, { force: true });
  const root = await realpath(await mkdtemp(join(tmpdir(), "streamskope native-recovery-")));
  let plan: NativeRecoveryPlan | undefined;
  let report: Record<string, unknown> | undefined;
  let fixture: Awaited<ReturnType<typeof startNativeKafkaFixture>> | undefined;
  let phase = "target installer";
  let failure: { phase: string; code: string } | undefined;
  const cleanup: Record<string, { outcome: "passed" | "failed"; code?: string }> = {};
  const clean = async (name: string, action: () => Promise<void>): Promise<void> => {
    try {
      await action();
      cleanup[name] = { outcome: "passed" };
    } catch (error) {
      const code = failureCode(error);
      cleanup[name] = { outcome: "failed", code };
      failure ??= { phase: name, code };
    }
  };
  try {
    const target = candidate
      ? await loadCandidateInstaller(args[3]!, to, root)
      : await downloadNativeInstaller(to, root);
    phase = "baseline installer";
    plan = {
      schemaVersion: 1,
      root,
      platform: process.platform,
      architecture: process.arch,
      from: await downloadNativeInstaller(from, root),
      to: target,
      ...(target.sourceRevision === undefined
        ? {}
        : { candidateSourceRevision: target.sourceRevision }),
    };
    if (supplied.length === 0) {
      phase = "disposable Kafka fixture";
      fixture = await startNativeKafkaFixture();
      Object.assign(process.env, fixture.environment);
    }
    process.env.STREAMSKOPE_NATIVE_RECOVERY_PLAN = await writeRecoveryPlan(plan);
    process.env.STREAMSKOPE_NATIVE_RECOVERY = "1";
    phase = "native installer replacement and profile recovery";
    await nativeCommand(
      process.execPath,
      ["tools/package/e2e.mjs", "electron", "test/e2e/electron-profile-recovery.spec.ts"],
      { timeoutMs: 480_000 },
    );
    phase = "native recovery evidence";
    report = JSON.parse(await readFile(reportPath, "utf8")) as Record<string, unknown>;
  } catch (error) {
    // Public failure evidence contains fixed phase names and allowlisted codes only.
    // Keep arbitrary process messages, fixture credentials and raw renderer data out.
    failure = { phase, code: failureCode(error) };
  } finally {
    const ownedPlan = plan;
    const ownedFixture = fixture;
    if (ownedPlan !== undefined)
      await clean("owned installation", () => uninstallNativeRecovery(ownedPlan));
    if (ownedFixture !== undefined)
      await clean("disposable Kafka fixture", () => ownedFixture.dispose());
    await clean("temporary files", () => rm(root, { recursive: true, force: true, maxRetries: 3 }));
  }
  if (plan === undefined || report === undefined)
    failure ??= { phase: "native recovery evidence", code: "VERIFICATION_FAILED" };
  await mkdir(output, { recursive: true });
  await writeFile(
    outputPath,
    `${JSON.stringify(
      {
        ...report,
        schemaVersion: 1,
        outcome: failure === undefined ? "passed" : "failed",
        ...(failure === undefined ? {} : { failure }),
        cleanup,
        ownedInstallationCleanup:
          cleanup["owned installation"]?.outcome === "passed" &&
          cleanup["temporary files"]?.outcome === "passed"
            ? "passed"
            : "not completed",
        capturedAt: new Date().toISOString(),
        platform: process.platform,
        architecture: process.arch,
        command: candidate
          ? `node --import tsx tools/check/native-recovery.ts --from ${from} --candidate dist/installers/${installerName(to)} --candidate-version ${to}`
          : `node --import tsx tools/check/native-recovery.ts --from ${from} --to ${to}`,
        targetKind: candidate ? "unreleased source installer" : "published installer",
        installers:
          plan === undefined
            ? []
            : [plan.from, plan.to].map(({ path: _path, ...published }) => published),
        fixture: fixture?.metadata ?? {
          source: supplied.length === 0 ? "not started" : "explicit external disposable fixture",
        },
      },
      null,
      2,
    )}\n`,
  );
  if (failure !== undefined)
    throw new Error(
      `Qualification failed during ${failure.phase}; sanitized evidence: ${outputPath}`,
    );
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `Native recovery failed: ${error instanceof Error ? error.message : "Unknown failure."}\n`,
  );
  process.exitCode = 1;
});
