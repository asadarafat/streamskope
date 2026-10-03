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
  await rm(outputPath, { force: true });
  const root = await realpath(await mkdtemp(join(tmpdir(), "streamskope native-recovery-")));
  let plan: NativeRecoveryPlan | undefined;
  let report: Record<string, unknown> | undefined;
  let fixture: Awaited<ReturnType<typeof startNativeKafkaFixture>> | undefined;
  try {
    const target = candidate
      ? await loadCandidateInstaller(args[3]!, to, root)
      : await downloadNativeInstaller(to, root);
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
      fixture = await startNativeKafkaFixture();
      Object.assign(process.env, fixture.environment);
    }
    process.env.STREAMSKOPE_NATIVE_RECOVERY_PLAN = await writeRecoveryPlan(plan);
    process.env.STREAMSKOPE_NATIVE_RECOVERY = "1";
    await nativeCommand(
      process.execPath,
      ["tools/package/e2e.mjs", "electron", "test/e2e/electron-profile-recovery.spec.ts"],
      { timeoutMs: 480_000 },
    );
    const reportPath = resolve("test-results", "electron", "native-recovery.json");
    report = JSON.parse(await readFile(reportPath, "utf8")) as Record<string, unknown>;
  } finally {
    try {
      if (plan !== undefined) await uninstallNativeRecovery(plan);
    } finally {
      try {
        await fixture?.dispose();
      } finally {
        await rm(root, { recursive: true, force: true, maxRetries: 3 });
      }
    }
  }
  if (plan === undefined || report === undefined)
    throw new Error("Native recovery evidence was not completed.");
  await mkdir(output, { recursive: true });
  await writeFile(
    outputPath,
    `${JSON.stringify(
      {
        schemaVersion: 1,
        outcome: "passed",
        ownedInstallationCleanup: "passed",
        capturedAt: new Date().toISOString(),
        command: candidate
          ? `node --import tsx tools/check/native-recovery.ts --from ${from} --candidate dist/installers/${plan.to.name} --candidate-version ${to}`
          : `node --import tsx tools/check/native-recovery.ts --from ${from} --to ${to}`,
        targetKind: candidate ? "unreleased source installer" : "published installer",
        installers: [plan.from, plan.to].map(({ path: _path, ...published }) => published),
        fixture: fixture?.metadata ?? { source: "explicit external disposable fixture" },
        ...report,
      },
      null,
      2,
    )}\n`,
  );
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `Native recovery failed: ${error instanceof Error ? error.message : "Unknown failure."}\n`,
  );
  process.exitCode = 1;
});
