import { execFileSync, spawnSync } from "node:child_process";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  symlink,
  utimes,
  writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, expect, it } from "vitest";

import {
  beginLocalQualification,
  beginLocalStage,
  completeLocalStage,
  finishLocalQualification,
  LOCAL_STAGES,
  LOCAL_LIVE_CHECKS,
  localQualificationSource,
  validateLocalQualificationBundle,
  type LocalQualificationReceipt,
  type LocalQualificationScope,
  type LocalStage,
} from "../../tools/check/qualification";

const directories: string[] = [];
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});
async function json(root: string, path: string, value: unknown): Promise<void> {
  await mkdir(dirname(join(root, path)), { recursive: true });
  await writeFile(join(root, path), JSON.stringify(value));
  // Filesystem write timestamps may lag the system clock by a scheduling tick.
  const recorded = new Date();
  await utimes(join(root, path), recorded, recorded);
}
async function fixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-local-evidence-"));
  directories.push(root);
  await writeFile(join(root, ".gitignore"), ".artifacts/\ndist/\n");
  await writeFile(join(root, "source.txt"), "qualified source\n");
  const git = (...args: string[]): void => {
    execFileSync("git", args, { cwd: root, stdio: "pipe" });
  };
  git("init", "--quiet");
  git("add", ".");
  git(
    "-c",
    "user.name=Evidence test",
    "-c",
    "user.email=evidence@example.invalid",
    "commit",
    "--quiet",
    "-m",
    "fixture",
  );
  return root;
}
function bundle(root: string, receipt: LocalQualificationReceipt): string {
  return join(root, ".artifacts/qualification", receipt.execution.id);
}
async function report(root: string, stage: LocalStage, live = false): Promise<void> {
  const timestamp = new Date().toISOString();
  if (stage === "shared")
    await json(root, ".artifacts/ci/vitest.json", {
      startTime: Date.now(),
      success: true,
      numTotalTests: 12,
      numPassedTests: 11,
      numPendingTests: 1,
      numTodoTests: 0,
      numFailedTests: 0,
      numFailedTestSuites: 0,
      testResults: [{ privateLog: "password=never-export-this" }],
    });
  else if (stage === "soak")
    await json(root, "dist/performance/qualification-soak.json", {
      schemaVersion: 1,
      capturedAt: timestamp,
      check: "bounded-stream-replay",
      outcome: "passed",
      evidence: {
        qualification: { passed: true },
        config: { seconds: 60, rate: 1000, mixed: true, roundTrip: true },
        elapsedMs: 60_001,
        generated: 60_000,
        published: 59_900,
        hostDisplayDrops: 100,
        peakRss: 400_000_000,
        host: "private-host.example",
      },
    });
  else if (stage === "docs")
    await json(root, ".artifacts/website/qualification.json", {
      schemaVersion: 1,
      outcome: "passed",
      startedAt: timestamp,
      completedAt: timestamp,
      htmlPages: 70,
      routes: 65,
      browserSha256: "b".repeat(64),
      media: { outcome: "skipped", reason: "unchanged-media-inputs", fingerprint: "a".repeat(64) },
    });
  else
    await json(
      root,
      `dist/ci/${stage}.json`,
      live
        ? {
            outcome: "passed",
            checkedAt: timestamp,
            checks: LOCAL_LIVE_CHECKS[stage],
            targetVersion: "v26.8.2",
            target: { product: "26.4.0", raw: "private-build-description" },
            apiCertificateVerification: true,
            apiTrust: "provided-ca",
            ownedTopic: "private-topic",
            failure: "password=never-export-this",
            endpoint: "https://private-host.example",
            knownRecord: { payload: "private business payload" },
          }
        : { outcome: "skipped", checkedAt: timestamp, checks: [], reasonCode: "not-configured" },
    );
}
async function successful(
  root: string,
  live = false,
  scope: LocalQualificationScope = "full",
  stages: readonly LocalStage[] = LOCAL_STAGES,
): Promise<LocalQualificationReceipt> {
  const receipt = await beginLocalQualification(root, scope);
  for (const stage of stages) {
    await beginLocalStage(root, receipt.execution.id, stage);
    await report(root, stage, live);
    await completeLocalStage(root, receipt.execution.id, stage);
  }
  return finishLocalQualification(root, receipt.execution.id, 0);
}

it("retains successful source-bound checks and explicit unavailable live tests in independent bundles", async () => {
  const root = await fixture();
  const first = await successful(root);
  const second = await successful(root);
  expect(first.execution.id).not.toBe(second.execution.id);
  expect(await validateLocalQualificationBundle(bundle(root, first))).toEqual(first);
  expect(first.outcome).toBe("passed");
  expect(first).toMatchObject({ schemaVersion: 2, scope: "full" });
  expect(first.source.unchanged).toBe(true);
  expect(first.source.start.dirty).toBe(false);
  expect(first.stages.map(({ outcome }) => outcome)).toEqual([
    "passed",
    "passed",
    "passed",
    "skipped",
    "skipped",
  ]);
  expect(first.stages[0]!.reports[0]!.summary).toMatchObject({ total: 12, passed: 11, skipped: 1 });
  expect(first.stages[3]!.reason).toBe("not-configured");
});

it("preserves the API v-prefixed EDA release in validated live evidence without exposing private data", async () => {
  const root = await fixture();
  const receipt = await successful(root, true);
  await validateLocalQualificationBundle(bundle(root, receipt));
  const projection = await readFile(join(bundle(root, receipt), "evidence/nsp-live.json"), "utf8");
  expect(projection).not.toMatch(/private|password|endpoint|payload|ownedTopic|knownRecord/u);
  expect(receipt.stages[3]!.reports[0]!.summary).toMatchObject({
    checks: LOCAL_LIVE_CHECKS["eda-live"].length,
    targetVersion: "v26.8.2",
    apiTrust: "provided-ca",
  });
  expect(receipt.stages[4]!.reports[0]!.summary).toMatchObject({
    checks: LOCAL_LIVE_CHECKS["nsp-live"].length,
    targetVersion: "26.4.0",
    scope: "development-host-plugin-lifecycle",
  });
  const eda: unknown = JSON.parse(
    await readFile(join(bundle(root, receipt), "evidence/eda-live.json"), "utf8"),
  );
  expect(eda).toMatchObject({ summary: { targetVersion: "v26.8.2" } });
  const shared = await readFile(join(bundle(root, receipt), "evidence/shared.json"), "utf8");
  expect(shared).not.toContain("never-export-this");
});

it.each<{ scope: LocalQualificationScope; stages: readonly LocalStage[] }>([
  { scope: "core", stages: ["shared", "soak", "docs"] },
  { scope: "full", stages: ["shared", "soak", "docs", "eda-live", "nsp-live"] },
  { scope: "eda", stages: ["eda-live"] },
  { scope: "nsp", stages: ["nsp-live"] },
  { scope: "live", stages: ["eda-live", "nsp-live"] },
])(
  "qualifies only the selected $scope scope with no invented evidence",
  async ({ scope, stages }) => {
    const root = await fixture();
    const receipt = await successful(root, true, scope, stages);
    expect(await validateLocalQualificationBundle(bundle(root, receipt))).toEqual(receipt);
    expect(receipt).toMatchObject({ schemaVersion: 2, scope, outcome: "passed" });
    expect(
      receipt.stages.filter((stage) => stage.outcome === "passed").map((stage) => stage.stage),
    ).toEqual(stages);
    for (const stage of receipt.stages.filter((stage) => !stages.includes(stage.stage)))
      expect(stage).toEqual({
        stage: stage.stage,
        outcome: "not-run",
        reason: "not-selected",
        startedAt: null,
        completedAt: null,
        reports: [],
      });
  },
);

it.each<{ scope: LocalQualificationScope; stage: LocalStage }>([
  { scope: "eda", stage: "eda-live" },
  { scope: "nsp", stage: "nsp-live" },
  { scope: "live", stage: "eda-live" },
])("requires actual live checks for explicit $scope scope", async ({ scope, stage }) => {
  const root = await fixture();
  const started = await beginLocalQualification(root, scope);
  await beginLocalStage(root, started.execution.id, stage);
  await report(root, stage);
  await expect(completeLocalStage(root, started.execution.id, stage)).rejects.toThrow(
    /did not qualify/u,
  );
  const receipt = await finishLocalQualification(root, started.execution.id, 1);
  expect(await validateLocalQualificationBundle(bundle(root, receipt))).toEqual(receipt);
  expect(receipt.outcome).toBe("failed");
  const selected = receipt.stages.find((item) => item.stage === stage)!;
  expect(selected).toMatchObject({ outcome: "failed", reason: "stage-failed" });
  expect(selected.reports[0]!.summary).toEqual({
    outcome: "skipped",
    checks: 0,
    checkIds: [],
    reason: "not-configured",
  });
  expect(receipt.stages[0]).toMatchObject({ outcome: "not-run", reason: "not-selected" });
  selected.outcome = "skipped";
  selected.reason = "not-configured";
  await json(bundle(root, receipt), "qualification.json", receipt);
  await expect(validateLocalQualificationBundle(bundle(root, receipt))).rejects.toThrow(
    /cannot skip selected/u,
  );
});

it("rejects out-of-scope and out-of-order work without deleting another stage's report", async () => {
  const root = await fixture();
  const receipt = await beginLocalQualification(root, "nsp");
  await report(root, "shared");
  const original = await readFile(join(root, ".artifacts/ci/vitest.json"));
  await expect(beginLocalStage(root, receipt.execution.id, "shared")).rejects.toThrow(
    /once, in order/u,
  );
  await expect(completeLocalStage(root, receipt.execution.id, "shared")).rejects.toThrow(
    /running stage/u,
  );
  expect(await readFile(join(root, ".artifacts/ci/vitest.json"))).toEqual(original);
  const live = await beginLocalQualification(root, "live");
  await expect(beginLocalStage(root, live.execution.id, "nsp-live")).rejects.toThrow(
    /once, in order/u,
  );
});

it("retains a selected preflight failure without claiming any stage ran", async () => {
  const root = await fixture();
  const started = await beginLocalQualification(root, "nsp");
  const receipt = await finishLocalQualification(root, started.execution.id, 2);
  expect(await validateLocalQualificationBundle(bundle(root, receipt))).toEqual(receipt);
  expect(receipt.outcome).toBe("failed");
  expect(receipt.stages[4]).toEqual({
    stage: "nsp-live",
    outcome: "not-run",
    reason: null,
    startedAt: null,
    completedAt: null,
    reports: [],
  });
  expect(receipt.stages.slice(0, 4).every((stage) => stage.reason === "not-selected")).toBe(true);
});

it("requires every selected stage and preserves the live-stage stop order", async () => {
  const root = await fixture();
  const incomplete = await beginLocalQualification(root, "core");
  await beginLocalStage(root, incomplete.execution.id, "shared");
  await report(root, "shared");
  await completeLocalStage(root, incomplete.execution.id, "shared");
  const missing = await finishLocalQualification(root, incomplete.execution.id, 0);
  expect(missing.outcome).toBe("failed");
  await validateLocalQualificationBundle(bundle(root, missing));

  const live = await beginLocalQualification(root, "live");
  await beginLocalStage(root, live.execution.id, "eda-live");
  const failed = await finishLocalQualification(root, live.execution.id, 143);
  expect(failed.stages.slice(0, 3).every((stage) => stage.reason === "not-selected")).toBe(true);
  expect(failed.stages[3]).toMatchObject({ outcome: "failed", reason: "stage-failed" });
  expect(failed.stages[4]).toMatchObject({ outcome: "not-run", reason: null });
  await validateLocalQualificationBundle(bundle(root, failed));
});

it.each(["passed", "failed"] as const)(
  "still validates historical schema1 %s receipts",
  async (outcome) => {
    const root = await fixture();
    let completed: LocalQualificationReceipt;
    if (outcome === "passed") completed = await successful(root);
    else {
      const started = await beginLocalQualification(root);
      await beginLocalStage(root, started.execution.id, "shared");
      completed = await finishLocalQualification(root, started.execution.id, 37);
    }
    const historical = {
      schemaVersion: 1,
      execution: completed.execution,
      source: completed.source,
      environment: completed.environment,
      outcome: completed.outcome,
      stages: completed.stages,
      notQualified: completed.notQualified,
    };
    await json(bundle(root, completed), "qualification.json", historical);
    expect(await validateLocalQualificationBundle(bundle(root, completed))).toEqual(historical);
    await json(bundle(root, completed), "qualification.json", { ...historical, scope: "full" });
    await expect(validateLocalQualificationBundle(bundle(root, completed))).rejects.toThrow(
      /Unexpected evidence fields/u,
    );
  },
);

it.each(["unknown", "full", "core"])(
  "rejects relabeling standalone NSP evidence as %s",
  async (scope) => {
    const root = await fixture();
    const receipt = await successful(root, true, "nsp", ["nsp-live"]);
    await json(bundle(root, receipt), "qualification.json", { ...receipt, scope });
    await expect(validateLocalQualificationBundle(bundle(root, receipt))).rejects.toThrow();
  },
);

it.each(["reason", "outcome", "timestamps", "reports"])(
  "rejects forged unselected-stage %s",
  async (field) => {
    const root = await fixture();
    const receipt = await successful(root, true, "nsp", ["nsp-live"]);
    const unselected = receipt.stages[0]!;
    if (field === "reason") unselected.reason = null;
    if (field === "outcome") unselected.outcome = "passed";
    if (field === "timestamps") unselected.startedAt = receipt.execution.startedAt;
    if (field === "reports") unselected.reports = receipt.stages[4]!.reports;
    await json(bundle(root, receipt), "qualification.json", receipt);
    await expect(validateLocalQualificationBundle(bundle(root, receipt))).rejects.toThrow(
      /Unselected stages/u,
    );
  },
);

it("rejects disguising a selected live failure as unselected", async () => {
  const root = await fixture();
  const started = await beginLocalQualification(root, "nsp");
  const receipt = await finishLocalQualification(root, started.execution.id, 2);
  receipt.stages[4]!.reason = "not-selected";
  await json(bundle(root, receipt), "qualification.json", receipt);
  await expect(validateLocalQualificationBundle(bundle(root, receipt))).rejects.toThrow();
});

it.each([{ args: ["shared"] }, { args: ["nsp", "shared"] }])(
  "rejects invalid CLI begin arguments $args",
  async ({ args }) => {
    const root = await fixture();
    const result = spawnSync(
      process.execPath,
      [
        "--import",
        createRequire(import.meta.url).resolve("tsx"),
        fileURLToPath(new URL("../../tools/check/qualification.ts", import.meta.url)),
        "begin",
        ...args,
      ],
      { cwd: root, encoding: "utf8" },
    );
    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    await expect(readdir(join(root, ".artifacts/qualification"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  },
);

it("prints the completed CLI scope without claiming full acceptance", async () => {
  const root = await fixture();
  const args = [
    "--import",
    createRequire(import.meta.url).resolve("tsx"),
    fileURLToPath(new URL("../../tools/check/qualification.ts", import.meta.url)),
  ];
  const begin = spawnSync(process.execPath, [...args, "begin", "nsp"], {
    cwd: root,
    encoding: "utf8",
  });
  expect(begin.status, begin.stderr).toBe(0);
  const id = begin.stdout.trim();
  await beginLocalStage(root, id, "nsp-live");
  await report(root, "nsp-live", true);
  await completeLocalStage(root, id, "nsp-live");
  const finish = spawnSync(process.execPath, [...args, "finish", id, "0"], {
    cwd: root,
    encoding: "utf8",
  });
  expect(finish.status, finish.stderr).toBe(0);
  expect(finish.stdout).toContain("Local qualification (nsp): passed.");
  expect(finish.stdout).not.toContain("(full)");
});

it("clears only the known report and rejects missing, stale and replaced evidence", async () => {
  const root = await fixture();
  await report(root, "shared");
  await json(root, ".artifacts/ci/keep.json", { ownedBy: "another-check" });
  const receipt = await beginLocalQualification(root);
  await beginLocalStage(root, receipt.execution.id, "shared");
  await expect(completeLocalStage(root, receipt.execution.id, "shared")).rejects.toThrow();
  expect(await readFile(join(root, ".artifacts/ci/keep.json"), "utf8")).toContain("another-check");
  await report(root, "shared");
  await utimes(join(root, ".artifacts/ci/vitest.json"), 1, 1);
  await expect(completeLocalStage(root, receipt.execution.id, "shared")).rejects.toThrow(
    /predates/u,
  );
  const failed = await finishLocalQualification(root, receipt.execution.id, 2);
  expect(failed.stages.map(({ outcome }) => outcome)).toEqual([
    "failed",
    "not-run",
    "not-run",
    "not-run",
    "not-run",
  ]);
  expect((await validateLocalQualificationBundle(bundle(root, failed))).execution.exitCode).toBe(2);
});

it("records interruptions as failed with later stages not run", async () => {
  const root = await fixture();
  const receipt = await beginLocalQualification(root);
  await beginLocalStage(root, receipt.execution.id, "shared");
  await report(root, "shared");
  await completeLocalStage(root, receipt.execution.id, "shared");
  await beginLocalStage(root, receipt.execution.id, "soak");
  const failed = await finishLocalQualification(root, receipt.execution.id, 143);
  expect(failed.stages.map(({ outcome }) => outcome)).toEqual([
    "passed",
    "failed",
    "not-run",
    "not-run",
    "not-run",
  ]);
  expect((await validateLocalQualificationBundle(bundle(root, failed))).outcome).toBe("failed");
});

it("records dirty source honestly and detects source mutations during qualification", async () => {
  const root = await fixture();
  await writeFile(join(root, "source.txt"), "local edits\n");
  const dirty = await successful(root);
  expect(dirty.source.start.dirty).toBe(true);
  expect(dirty.source.unchanged).toBe(true);
  expect((await validateLocalQualificationBundle(bundle(root, dirty))).outcome).toBe("passed");
  const receipt = await beginLocalQualification(root);
  for (const stage of LOCAL_STAGES) {
    await beginLocalStage(root, receipt.execution.id, stage);
    await report(root, stage);
    await completeLocalStage(root, receipt.execution.id, stage);
  }
  await writeFile(join(root, "untracked-source.txt"), "new source\n");
  const failed = await finishLocalQualification(root, receipt.execution.id, 0);
  expect(failed.source.unchanged).toBe(false);
  expect(failed.outcome).toBe("failed");
  await validateLocalQualificationBundle(bundle(root, failed));
  expect((await localQualificationSource(root)).fingerprint).not.toBe(
    dirty.source.start.fingerprint,
  );
});

it("refuses report tampering and forged skip outcomes", async () => {
  const root = await fixture();
  const receipt = await successful(root);
  await writeFile(join(bundle(root, receipt), "evidence/shared.json"), "{}\n");
  await expect(validateLocalQualificationBundle(bundle(root, receipt))).rejects.toThrow(/hash/u);
  const second = await successful(root);
  second.stages[3]!.reason = null;
  await json(bundle(root, second), "qualification.json", second);
  await expect(validateLocalQualificationBundle(bundle(root, second))).rejects.toThrow();
});

it("refuses symlinked report parents without deleting their contents", async () => {
  const root = await fixture();
  const outside = await mkdtemp(join(tmpdir(), "streamskope-private-evidence-"));
  directories.push(outside);
  await writeFile(join(outside, "vitest.json"), "private data");
  const receipt = await beginLocalQualification(root);
  await symlink(outside, join(root, ".artifacts/ci"), "dir");
  await expect(beginLocalStage(root, receipt.execution.id, "shared")).rejects.toThrow(
    /owned directories/u,
  );
  expect(await readFile(join(outside, "vitest.json"), "utf8")).toBe("private data");
});

it("rejects implicit live skips even when the command exited successfully", async () => {
  const root = await fixture();
  const receipt = await beginLocalQualification(root);
  for (const stage of ["shared", "soak", "docs"] as const) {
    await beginLocalStage(root, receipt.execution.id, stage);
    await report(root, stage);
    await completeLocalStage(root, receipt.execution.id, stage);
  }
  await beginLocalStage(root, receipt.execution.id, "eda-live");
  await json(root, "dist/ci/eda-live.json", {
    outcome: "skipped",
    checkedAt: new Date().toISOString(),
    checks: [],
  });
  await expect(completeLocalStage(root, receipt.execution.id, "eda-live")).rejects.toThrow(
    /explicit reason/u,
  );
});

it("retains completed live operation IDs after failure without retaining the exception", async () => {
  const root = await fixture();
  const receipt = await beginLocalQualification(root);
  for (const stage of ["shared", "soak", "docs"] as const) {
    await beginLocalStage(root, receipt.execution.id, stage);
    await report(root, stage);
    await completeLocalStage(root, receipt.execution.id, stage);
  }
  await beginLocalStage(root, receipt.execution.id, "eda-live");
  await json(root, "dist/ci/eda-live.json", {
    outcome: "failed",
    checkedAt: new Date().toISOString(),
    targetVersion: "26.8.2",
    apiCertificateVerification: true,
    apiTrust: "provided-ca",
    checks: ["cluster-version"],
    failure: "private endpoint request failed",
  });
  const failed = await finishLocalQualification(root, receipt.execution.id, 1);
  const validated = await validateLocalQualificationBundle(bundle(root, failed));
  expect(validated.stages[3]!.reports[0]!.summary).toMatchObject({
    outcome: "failed",
    checks: 1,
    checkIds: ["cluster-version"],
  });
  expect(
    await readFile(join(bundle(root, failed), "evidence/eda-live.json"), "utf8"),
  ).not.toContain("private endpoint");
  expect(validated.stages[4]!.outcome).toBe("not-run");
});

it("refuses a live pass that omitted the owned-resource cleanup check", async () => {
  const root = await fixture();
  const receipt = await beginLocalQualification(root);
  for (const stage of ["shared", "soak", "docs"] as const) {
    await beginLocalStage(root, receipt.execution.id, stage);
    await report(root, stage);
    await completeLocalStage(root, receipt.execution.id, stage);
  }
  await beginLocalStage(root, receipt.execution.id, "eda-live");
  await json(root, "dist/ci/eda-live.json", {
    outcome: "passed",
    checkedAt: new Date().toISOString(),
    targetVersion: "26.8.2",
    apiCertificateVerification: true,
    apiTrust: "provided-ca",
    checks: LOCAL_LIVE_CHECKS["eda-live"].filter((id) => id !== "owned-resource-cleanup"),
  });
  await expect(completeLocalStage(root, receipt.execution.id, "eda-live")).rejects.toThrow(
    /every required operation/u,
  );
  const failed = await finishLocalQualification(root, receipt.execution.id, 1);
  expect(failed.stages[3]!.outcome).toBe("failed");
  expect(failed.stages[3]!.reports).toEqual([]);
});

it("refuses a redirected stored run identity", async () => {
  const root = await fixture();
  const receipt = await beginLocalQualification(root);
  const originalId = receipt.execution.id;
  receipt.execution.id = "../../private";
  await json(root, `.artifacts/qualification/${originalId}/qualification.json`, receipt);
  await expect(beginLocalStage(root, originalId, "shared")).rejects.toThrow(/identity/u);
});

it.each(["--assume-unchanged", "--skip-worktree"])(
  "rejects hidden source modifications with %s",
  async (flag) => {
    const root = await fixture();
    execFileSync("git", ["update-index", flag, "source.txt"], { cwd: root });
    await writeFile(join(root, "source.txt"), "hidden changes\n");
    await expect(beginLocalQualification(root)).rejects.toThrow(/assume-unchanged/u);
  },
);

it("the real check entry point keeps fail-fast status while its EXIT trap records failure", async () => {
  const root = await fixture();
  const bin = join(root, ".artifacts/bin");
  await mkdir(bin, { recursive: true });
  await writeFile(
    join(bin, "node"),
    `#!/usr/bin/env bash
if [[ "$1 $2 $3" == "--import tsx tools/check/qualification.ts" ]]; then
  shift 3
  exec "$QUALIFICATION_NODE" --import "$QUALIFICATION_TSX" "$QUALIFICATION_SCRIPT" "$@"
fi
exit 37
`,
  );
  await chmod(join(bin, "node"), 0o755);
  const executed = spawnSync(
    "bash",
    [fileURLToPath(new URL("../../tools/check.sh", import.meta.url))],
    {
      cwd: root,
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${bin}:${process.env.PATH}`,
        QUALIFICATION_NODE: process.execPath,
        QUALIFICATION_TSX: createRequire(import.meta.url).resolve("tsx"),
        QUALIFICATION_SCRIPT: fileURLToPath(
          new URL("../../tools/check/qualification.ts", import.meta.url),
        ),
      },
    },
  );
  expect(executed.status, executed.stderr).toBe(37);
  const runs = await readdir(join(root, ".artifacts/qualification"));
  expect(runs).toHaveLength(1);
  const receipt = await validateLocalQualificationBundle(
    join(root, ".artifacts/qualification", runs[0]!),
  );
  expect(receipt.execution.exitCode).toBe(37);
  expect(receipt.stages.map(({ outcome }) => outcome)).toEqual([
    "failed",
    "not-run",
    "not-run",
    "not-run",
    "not-run",
  ]);
});
