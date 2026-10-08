import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, expect, it } from "vitest";

import {
  aggregateCiEvidence,
  CI_LANES,
  CI_REPORTS,
  prepareCiLane,
  qualificationSource,
  readReportEvidence,
  recordCiLane,
  type CiLane,
  type LaneReceipt,
} from "../../tools/check/ci-evidence";

const directories: string[] = [];
const success = {
  shared: { result: "success" },
  docs: { result: "success" },
  runtime: { result: "success" },
};

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});

async function json(root: string, path: string, value: unknown): Promise<void> {
  await mkdir(dirname(join(root, path)), { recursive: true });
  await writeFile(join(root, path), JSON.stringify(value));
}

async function fixture(): Promise<{ root: string; env: NodeJS.ProcessEnv }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-ci-evidence-"));
  directories.push(root);
  const git = (...args: string[]): void => {
    execFileSync("git", args, { cwd: root, stdio: "pipe" });
  };
  git("init", "--quiet");
  await writeFile(join(root, ".gitignore"), ".artifacts/\ntest-results/\n");
  await writeFile(join(root, "source.txt"), "qualified source\n");
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
  return {
    root,
    env: {
      GITHUB_ACTIONS: "true",
      GITHUB_SHA: qualificationSource(root).commit,
      GITHUB_RUN_ID: "1234",
      GITHUB_RUN_ATTEMPT: "1",
    },
  };
}

async function completeLane(
  root: string,
  lane: CiLane,
  env: NodeJS.ProcessEnv,
): Promise<LaneReceipt> {
  const startedAt = new Date(Date.now() - 1000).toISOString();
  await prepareCiLane(root, lane);
  for (const report of CI_REPORTS[lane]) {
    await json(
      root,
      report.path,
      report.kind === "accessibility"
        ? []
        : report.kind === "docs"
          ? {
              schemaVersion: 1,
              outcome: "passed",
              startedAt: new Date().toISOString(),
              completedAt: new Date().toISOString(),
              htmlPages: 10,
              routes: 8,
              browserSha256: "a".repeat(64),
              media: {
                outcome: "skipped",
                reason: "unchanged-media-inputs",
                fingerprint: "b".repeat(64),
              },
            }
          : report.kind === "vitest"
            ? {
                startTime: Date.now(),
                success: true,
                numTotalTests: 3,
                numPassedTests: 3,
                numFailedTests: 0,
                numFailedTestSuites: 0,
                numPendingTests: 0,
                numTodoTests: 0,
              }
            : {
                errors: [],
                stats: {
                  startTime: new Date().toISOString(),
                  duration: 100,
                  expected: 2,
                  unexpected: 0,
                  flaky: 0,
                  skipped: 0,
                },
              },
    );
  }
  return recordCiLane({ root, lane, env, startedAt, exitCode: 0 });
}

async function completeAll(root: string, env: NodeJS.ProcessEnv): Promise<void> {
  for (const lane of CI_LANES) expect((await completeLane(root, lane, env)).outcome).toBe("passed");
}

it("assembles independently hashed suite evidence bound to the exact clean source and run", async () => {
  const { root, env } = await fixture();
  await completeAll(root, env);
  const index = await aggregateCiEvidence({ root, env, results: success });
  expect(index.outcome).toBe("passed");
  expect(index.source).toEqual(qualificationSource(root));
  expect(index.execution).toEqual({ runId: "1234", attempt: 1 });
  expect(index.lanes.map((lane) => lane.lane)).toEqual(["shared", "docs", "runtime"]);
  const browser = index.lanes
    .find((lane) => lane.lane === "runtime")!
    .reports.filter((report) => report.path.startsWith("test-results/"));
  expect(browser.map((report) => report.path)).toEqual([
    "test-results/web/production-startup/playwright-results.json",
    "test-results/web/workbench/playwright-results.json",
    "test-results/web/nats-workspace/playwright-results.json",
    "test-results/web/plugin-lifecycle/playwright-results.json",
  ]);
  for (const report of index.lanes.flatMap((lane) => lane.reports)) {
    const bytes = await readFile(join(root, report.path));
    expect(report.sha256).toBe(createHash("sha256").update(bytes).digest("hex"));
    expect(report.bytes).toBe(bytes.length);
  }
  expect(
    JSON.parse(await readFile(join(root, ".artifacts/ci/qualification-index.json"), "utf8")),
  ).toEqual(index);
});

it.each(["failure", "cancelled", "skipped", undefined])(
  "rejects a %s lane even when its report claims success",
  async (result) => {
    const { root, env } = await fixture();
    await completeAll(root, env);
    const index = await aggregateCiEvidence({
      root,
      env,
      results: { ...success, docs: result ? { result } : undefined },
    });
    expect(index.outcome).toBe("failed");
    expect(index.errors).toContain("Required docs job did not succeed.");
  },
);

it("rejects missing and changed evidence instead of accepting other successful suites", async () => {
  const { root, env } = await fixture();
  await completeAll(root, env);
  await rm(join(root, "test-results/web/workbench/playwright-results.json"));
  expect((await aggregateCiEvidence({ root, env, results: success })).outcome).toBe("failed");
  await completeLane(root, "runtime", env);
  const file = join(root, ".artifacts/ci/vitest.json");
  const changed = JSON.parse(await readFile(file, "utf8")) as Record<string, unknown>;
  await writeFile(file, JSON.stringify({ ...changed, numTotalTests: 4, numPassedTests: 4 }));
  const result = await aggregateCiEvidence({ root, env, results: success });
  expect(result.outcome).toBe("failed");
  expect(result.errors).toContain("Missing, failed, mismatched or changed shared evidence.");
});

it("rejects a rewritten receipt summary even when its referenced report bytes are unchanged", async () => {
  const { root, env } = await fixture();
  await completeAll(root, env);
  const path = ".artifacts/ci/lanes/shared.json";
  const receipt = JSON.parse(await readFile(join(root, path), "utf8")) as LaneReceipt;
  await json(root, path, {
    ...receipt,
    reports: receipt.reports.map((report) => ({
      ...report,
      summary: { ...report.summary, passed: 99 },
    })),
  });
  const index = await aggregateCiEvidence({ root, env, results: success });
  expect(index.outcome).toBe("failed");
  expect(index.errors).toContain("Missing, failed, mismatched or changed shared evidence.");
});

it("accepts earlier successful attempts of the same run while rejecting other runs and future attempts", async () => {
  const { root, env } = await fixture();
  await completeAll(root, env);
  await completeLane(root, "docs", { ...env, GITHUB_RUN_ATTEMPT: "2" });
  const retry = await aggregateCiEvidence({
    root,
    env: { ...env, GITHUB_RUN_ATTEMPT: "2" },
    results: success,
  });
  expect(retry.outcome).toBe("passed");
  expect(retry.lanes.map((lane) => lane.execution.attempt)).toEqual([1, 2, 1]);
  expect((await aggregateCiEvidence({ root, env, results: success })).outcome).toBe("failed");
  expect(
    (
      await aggregateCiEvidence({
        root,
        env: { ...env, GITHUB_RUN_ID: "9999", GITHUB_RUN_ATTEMPT: "2" },
        results: success,
      })
    ).outcome,
  ).toBe("failed");
});

it("keeps local dirty state explicit and rejects it for GitHub qualification", async () => {
  const { root, env } = await fixture();
  await writeFile(join(root, "source.txt"), "uncommitted implementation\n");
  const receipt = await completeLane(root, "shared", {});
  expect(receipt.source.dirty).toBe(true);
  await expect(completeLane(root, "shared", env)).rejects.toThrow("clean checkout");
});

it("rejects a different requested source before accepting its reports", async () => {
  const { root, env } = await fixture();
  await expect(
    completeLane(root, "shared", { ...env, GITHUB_SHA: "a".repeat(40) }),
  ).rejects.toThrow("requested GitHub source");
});

it("clears only the selected lane's old reports before a rerun", async () => {
  const { root, env } = await fixture();
  await completeAll(root, env);
  await prepareCiLane(root, "runtime");
  for (const report of CI_REPORTS.runtime)
    await expect(readFile(join(root, report.path))).rejects.toMatchObject({ code: "ENOENT" });
  await expect(readFile(join(root, ".artifacts/ci/lanes/runtime.json"))).rejects.toMatchObject({
    code: "ENOENT",
  });
  expect((await readFile(join(root, ".artifacts/ci/vitest.json"))).length).toBeGreaterThan(0);
  expect(await readFile(join(root, ".artifacts/website/search-accessibility.json"), "utf8")).toBe(
    "[]",
  );
});

it("rejects a copied old execution even when its file modification time is fresh", async () => {
  const { root, env } = await fixture();
  const startedAt = new Date(Date.now() - 1000).toISOString();
  await completeLane(root, "shared", env);
  const path = CI_REPORTS.shared[0]!.path;
  const previous = JSON.parse(await readFile(join(root, path), "utf8")) as Record<string, unknown>;
  await json(root, path, { ...previous, startTime: Date.now() - 60_000 });
  const result = await recordCiLane({ root, lane: "shared", env, startedAt, exitCode: 0 });
  expect(result.outcome).toBe("failed");
  expect(result.errors).toEqual([`Missing, invalid or stale evidence: ${path}`]);
});

it("records documentation scope and the explicit media skip in the CI index", async () => {
  const { root, env } = await fixture();
  await completeAll(root, env);
  const index = await aggregateCiEvidence({ root, env, results: success });
  expect(index.outcome).toBe("passed");
  expect(index.lanes.find(({ lane }) => lane === "docs")?.reports).toContainEqual(
    expect.objectContaining({
      path: ".artifacts/website/qualification.json",
      summary: {
        htmlPages: 10,
        routes: 8,
        media: "skipped",
        mediaReason: "unchanged-media-inputs",
        mediaFingerprint: "b".repeat(64),
      },
    }),
  );
});

it.each([
  { routes: 0 },
  { htmlPages: "ten" },
  { media: { outcome: "skipped", fingerprint: "b".repeat(64) } },
  { browserSha256: "missing" },
  { startedAt: "2020-01-01T00:00:00.000Z" },
  { completedAt: "2100-01-01T00:00:00.000Z" },
])("rejects incomplete or stale documentation evidence: %j", async (change) => {
  const { root, env } = await fixture();
  const receipt = await completeLane(root, "docs", env);
  const definition = CI_REPORTS.docs.find(({ kind }) => kind === "docs")!;
  const data = JSON.parse(await readFile(join(root, definition.path), "utf8")) as Record<
    string,
    unknown
  >;
  await json(root, definition.path, { ...data, ...change });
  await expect(
    readReportEvidence(root, definition, receipt.startedAt, receipt.completedAt),
  ).rejects.toThrow();
});

it("retains a failed lane receipt without promoting reports from its earlier steps", async () => {
  const { root, env } = await fixture();
  await completeLane(root, "shared", env);
  const receipt = await recordCiLane({
    root,
    lane: "shared",
    env,
    startedAt: new Date(Date.now() - 1000).toISOString(),
    exitCode: 7,
  });
  expect(receipt.outcome).toBe("failed");
  expect(receipt.exitCode).toBe(7);
  expect(receipt.reports).toHaveLength(1);
});

it("does not accept report symlinks escaping the checkout", async () => {
  const { root } = await fixture();
  const outside = await mkdtemp(join(tmpdir(), "streamskope-ci-outside-"));
  directories.push(outside);
  await writeFile(join(outside, "result.json"), "[]");
  await mkdir(join(root, ".artifacts/website"), { recursive: true });
  await symlink(join(outside, "result.json"), join(root, CI_REPORTS.docs[0]!.path));
  await expect(readReportEvidence(root, CI_REPORTS.docs[0]!)).rejects.toThrow(
    "outside the checkout",
  );
});

it("does not remove old evidence through a symlinked parent outside the checkout", async () => {
  const { root } = await fixture();
  const outside = await mkdtemp(join(tmpdir(), "streamskope-ci-owned-"));
  directories.push(outside);
  await mkdir(join(root, ".artifacts"));
  await writeFile(join(outside, "search-accessibility.json"), "preserve unrelated evidence");
  await symlink(outside, join(root, ".artifacts/website"), "dir");
  await expect(prepareCiLane(root, "docs")).rejects.toThrow();
  expect(await readFile(join(outside, "search-accessibility.json"), "utf8")).toBe(
    "preserve unrelated evidence",
  );
});

it.each([
  { numPassedTests: undefined },
  { numPassedTests: 99 },
  { numPendingTests: -1 },
  { numTodoTests: "not a count" },
])("rejects malformed successful test counts: %j", async (change) => {
  const { root, env } = await fixture();
  await completeLane(root, "shared", env);
  const definition = CI_REPORTS.shared[0]!;
  const existing = JSON.parse(await readFile(join(root, definition.path), "utf8")) as Record<
    string,
    unknown
  >;
  await json(root, definition.path, { ...existing, ...change });
  await expect(readReportEvidence(root, definition)).rejects.toThrow();
});

it("stages a lane with only one report root without flattening hidden repository paths", async () => {
  const { root, env } = await fixture();
  await completeLane(root, "shared", env);
  const temporary = await mkdtemp(join(tmpdir(), "streamskope-ci-stage-"));
  directories.push(temporary);
  const script = fileURLToPath(new URL("../../tools/check/ci-evidence.ts", import.meta.url));
  execFileSync(process.execPath, [script, "stage"], {
    cwd: root,
    env: { ...process.env, RUNNER_TEMP: temporary },
  });
  expect(await readFile(join(temporary, "qualification/.artifacts/ci/vitest.json"), "utf8")).toBe(
    await readFile(join(root, ".artifacts/ci/vitest.json"), "utf8"),
  );
  expect(
    JSON.parse(
      await readFile(join(temporary, "qualification/.artifacts/ci/lanes/shared.json"), "utf8"),
    ),
  ).toMatchObject({ lane: "shared", outcome: "passed" });
});
