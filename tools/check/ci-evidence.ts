import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, lstat, mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { dirname, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";

export const CI_LANES = ["shared", "docs", "runtime"] as const;
export type CiLane = (typeof CI_LANES)[number];
type ReportKind = "vitest" | "playwright" | "accessibility" | "docs";
interface ReportDefinition {
  readonly path: string;
  readonly kind: ReportKind;
}

const browserReport = (suite: string): ReportDefinition => ({
  path: `test-results/web/${suite}/playwright-results.json`,
  kind: "playwright",
});
export const CI_REPORTS: Record<CiLane, readonly ReportDefinition[]> = {
  shared: [{ path: ".artifacts/ci/vitest.json", kind: "vitest" }],
  docs: [
    { path: ".artifacts/website/search-accessibility.json", kind: "accessibility" },
    { path: ".artifacts/website/qualification.json", kind: "docs" },
  ],
  runtime: [
    { path: ".artifacts/ci/connection-profiles-real.json", kind: "vitest" },
    { path: ".artifacts/ci/observations-real.json", kind: "vitest" },
    { path: ".artifacts/ci/nats-real.json", kind: "vitest" },
    { path: ".artifacts/ci/production-startup.json", kind: "playwright" },
    ...["production-startup", "workbench", "nats-workspace", "plugin-lifecycle"].map(browserReport),
  ],
};

export interface QualificationSource {
  readonly commit: string;
  readonly tree: string;
  readonly dirty: boolean;
}
export interface CiExecution {
  readonly runId: string | null;
  readonly attempt: number;
}
export interface ReportEvidence {
  readonly path: string;
  readonly sha256: string;
  readonly bytes: number;
  readonly summary: Readonly<Record<string, number | boolean | string | null>>;
}
export interface LaneReceipt {
  readonly schemaVersion: 1;
  readonly lane: CiLane;
  readonly source: QualificationSource;
  readonly execution: CiExecution;
  readonly startedAt: string;
  readonly completedAt: string;
  readonly durationMs: number;
  readonly exitCode: number;
  readonly outcome: "passed" | "failed";
  readonly reports: readonly ReportEvidence[];
  readonly errors: readonly string[];
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Expected an evidence object.");
  return value as Record<string, unknown>;
}

export function qualificationSource(root: string): QualificationSource {
  const git = (args: string[]): string =>
    execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
  return {
    commit: git(["rev-parse", "HEAD"]),
    tree: git(["rev-parse", "HEAD^{tree}"]),
    dirty: git(["status", "--porcelain", "--untracked-files=normal"]).length > 0,
  };
}

export function ciExecution(env: NodeJS.ProcessEnv = process.env): CiExecution {
  const runId = env.GITHUB_RUN_ID ?? null;
  const attempt = Number(env.GITHUB_RUN_ATTEMPT ?? "1");
  if (
    (runId !== null && !/^[1-9]\d*$/u.test(runId)) ||
    !Number.isSafeInteger(attempt) ||
    attempt < 1
  )
    throw new Error("Invalid CI execution identity.");
  return { runId, attempt };
}

function assertSource(source: QualificationSource, env: NodeJS.ProcessEnv): void {
  if (env.GITHUB_SHA !== undefined && source.commit !== env.GITHUB_SHA)
    throw new Error("Qualification checkout does not match the requested GitHub source.");
  if (env.GITHUB_ACTIONS === "true" && (source.dirty || !env.GITHUB_RUN_ID))
    throw new Error("GitHub qualification requires a clean checkout and run identity.");
}

function reportSummary(value: unknown, kind: ReportKind): ReportEvidence["summary"] {
  if (kind === "accessibility") {
    if (!Array.isArray(value) || value.length !== 0)
      throw new Error("Documentation accessibility findings must be empty.");
    return { findings: 0 };
  }
  const data = object(value);
  const count = (value: unknown, minimum = 0): boolean =>
    Number.isSafeInteger(value) && Number(value) >= minimum;
  if (kind === "docs") {
    const media = object(data.media);
    if (
      data.schemaVersion !== 1 ||
      data.outcome !== "passed" ||
      !count(data.htmlPages, 1) ||
      !count(data.routes, 1) ||
      typeof data.browserSha256 !== "string" ||
      !/^[a-f0-9]{64}$/u.test(data.browserSha256) ||
      typeof data.startedAt !== "string" ||
      typeof data.completedAt !== "string" ||
      !Number.isFinite(Date.parse(data.startedAt)) ||
      !Number.isFinite(Date.parse(data.completedAt)) ||
      Date.parse(data.startedAt) > Date.parse(data.completedAt) ||
      Date.parse(data.completedAt) > Date.now() ||
      !(
        (media.outcome === "passed" && media.reason === undefined) ||
        (media.outcome === "skipped" && media.reason === "unchanged-media-inputs")
      ) ||
      typeof media.fingerprint !== "string" ||
      !/^[a-f0-9]{64}$/u.test(media.fingerprint)
    )
      throw new Error("Documentation evidence must contain successful page and browser checks.");
    return {
      htmlPages: Number(data.htmlPages),
      routes: Number(data.routes),
      media: String(media.outcome),
      mediaReason: media.outcome === "skipped" ? "unchanged-media-inputs" : null,
      mediaFingerprint: media.fingerprint,
    };
  }
  if (kind === "vitest") {
    if (
      data.success !== true ||
      !count(data.numTotalTests, 1) ||
      !count(data.numPassedTests, 1) ||
      Number(data.numPassedTests) > Number(data.numTotalTests) ||
      !count(data.numPendingTests ?? 0) ||
      !count(data.numTodoTests ?? 0) ||
      Number(data.numPendingTests ?? 0) > Number(data.numTotalTests) ||
      Number(data.numTodoTests ?? 0) > Number(data.numTotalTests) ||
      data.numFailedTests !== 0 ||
      data.numFailedTestSuites !== 0
    )
      throw new Error("Vitest evidence must contain successful executed tests.");
    return {
      total: Number(data.numTotalTests),
      passed: Number(data.numPassedTests),
      skipped: Number(data.numPendingTests ?? 0),
      todo: Number(data.numTodoTests ?? 0),
    };
  }
  const stats = object(data.stats);
  if (
    !Array.isArray(data.errors) ||
    data.errors.length !== 0 ||
    !count(stats.expected, 1) ||
    !count(stats.skipped ?? 0) ||
    stats.unexpected !== 0 ||
    stats.flaky !== 0 ||
    !Number.isFinite(stats.duration) ||
    Number(stats.duration) < 0
  )
    throw new Error("Playwright evidence must contain successful executed tests.");
  return {
    passed: Number(stats.expected),
    skipped: Number(stats.skipped ?? 0),
    durationMs: Number(stats.duration),
  };
}

/** Fixed report paths, bounded regular files and contained real paths also serve release collection. */
export async function readReportEvidence(
  root: string,
  definition: ReportDefinition,
  startedAt?: string,
  completedAt?: string,
): Promise<ReportEvidence> {
  const file = resolve(root, definition.path);
  const base = await realpath(root);
  const actual = await realpath(file);
  const stat = await lstat(file);
  if (
    !actual.startsWith(`${base}${sep}`) ||
    !stat.isFile() ||
    stat.size === 0 ||
    stat.size > 64 * 1024 * 1024 ||
    (startedAt !== undefined && stat.mtimeMs < Date.parse(startedAt))
  )
    throw new Error("Evidence is stale, outside the checkout or not a bounded regular file.");
  const contents = await readFile(file);
  const parsed = JSON.parse(contents.toString("utf8")) as unknown;
  if (startedAt !== undefined && definition.kind !== "accessibility") {
    const data = object(parsed);
    const timestamp =
      definition.kind === "vitest"
        ? Number(data.startTime)
        : definition.kind === "docs"
          ? Date.parse(String(data.startedAt))
          : Date.parse(String(object(data.stats).startTime));
    if (
      !Number.isFinite(timestamp) ||
      timestamp < Date.parse(startedAt) ||
      (completedAt !== undefined && timestamp > Date.parse(completedAt)) ||
      (definition.kind === "docs" &&
        completedAt !== undefined &&
        Date.parse(String(data.completedAt)) > Date.parse(completedAt))
    )
      throw new Error("The test execution is outside this qualification interval.");
  }
  return {
    path: definition.path,
    sha256: createHash("sha256").update(contents).digest("hex"),
    bytes: contents.byteLength,
    summary: reportSummary(parsed, definition.kind),
  };
}

async function assertOwnedParent(root: string, file: string): Promise<void> {
  const base = resolve(root);
  if (!file.startsWith(`${base}${sep}`)) throw new Error("Evidence path is outside the checkout.");
  for (let parent = dirname(file); parent !== base; parent = dirname(parent)) {
    const stat = await lstat(parent).catch((error: unknown) => {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw error;
    });
    if (stat && (!stat.isDirectory() || stat.isSymbolicLink()))
      throw new Error("Evidence parent must be an owned directory, not a symlink.");
  }
}

export async function prepareCiLane(root: string, lane: CiLane): Promise<void> {
  if (!CI_LANES.includes(lane)) throw new Error("Unknown CI lane.");
  const paths = [
    ...CI_REPORTS[lane].map(({ path }) => resolve(root, path)),
    resolve(root, `.artifacts/ci/lanes/${lane}.json`),
  ];
  for (const path of paths) await assertOwnedParent(root, path);
  for (const path of paths) await rm(path, { force: true });
}

/** A single upload root retains hidden repository-relative paths in every lane. */
async function stageCiEvidence(root: string, temporary: string): Promise<void> {
  const destination = resolve(temporary, "qualification");
  for (const path of [".artifacts/ci", ".artifacts/website", "test-results/web"]) {
    if (!(await lstat(resolve(root, path)).catch(() => undefined))) continue;
    await mkdir(dirname(resolve(destination, path)), { recursive: true });
    await cp(resolve(root, path), resolve(destination, path), { recursive: true });
  }
}

async function writeJson(root: string, path: string, value: unknown): Promise<void> {
  const file = resolve(root, path);
  await assertOwnedParent(root, file);
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, `${JSON.stringify(value, null, 2)}\n`);
}

export async function recordCiLane(options: {
  root: string;
  lane: CiLane;
  startedAt: string;
  exitCode: number;
  env?: NodeJS.ProcessEnv;
}): Promise<LaneReceipt> {
  const { root, lane, startedAt, exitCode } = options;
  const env = options.env ?? process.env;
  const completedAt = new Date().toISOString();
  if (
    !CI_LANES.includes(lane) ||
    !Number.isFinite(Date.parse(startedAt)) ||
    Date.parse(startedAt) > Date.parse(completedAt) ||
    !Number.isSafeInteger(exitCode) ||
    exitCode < 0 ||
    exitCode > 255
  )
    throw new Error("Invalid CI lane completion.");
  const source = qualificationSource(root);
  assertSource(source, env);
  const errors: string[] = [];
  const reports: ReportEvidence[] = [];
  for (const definition of CI_REPORTS[lane]) {
    try {
      reports.push(await readReportEvidence(root, definition, startedAt, completedAt));
    } catch {
      errors.push(`Missing, invalid or stale evidence: ${definition.path}`);
    }
  }
  const receipt: LaneReceipt = {
    schemaVersion: 1,
    lane,
    source,
    execution: ciExecution(env),
    startedAt,
    completedAt,
    durationMs: Date.parse(completedAt) - Date.parse(startedAt),
    exitCode,
    outcome: exitCode === 0 && errors.length === 0 ? "passed" : "failed",
    reports,
    errors,
  };
  await writeJson(root, `.artifacts/ci/lanes/${lane}.json`, receipt);
  return receipt;
}

export async function aggregateCiEvidence(options: {
  root: string;
  results: unknown;
  env?: NodeJS.ProcessEnv;
}): Promise<{
  schemaVersion: 1;
  source: QualificationSource;
  execution: CiExecution;
  generatedAt: string;
  outcome: "passed" | "failed";
  lanes: readonly LaneReceipt[];
  errors: readonly string[];
}> {
  const { root } = options;
  const env = options.env ?? process.env;
  const source = qualificationSource(root);
  assertSource(source, env);
  const execution = ciExecution(env);
  const results = object(options.results);
  const errors: string[] = [];
  const lanes: LaneReceipt[] = [];
  for (const lane of CI_LANES) {
    if (object(results[lane] ?? {}).result !== "success")
      errors.push(`Required ${lane} job did not succeed.`);
    try {
      const receipt = object(
        JSON.parse(await readFile(resolve(root, `.artifacts/ci/lanes/${lane}.json`), "utf8")),
      );
      const recordedSource = object(receipt.source);
      const recordedExecution = object(receipt.execution);
      if (
        receipt.schemaVersion !== 1 ||
        receipt.lane !== lane ||
        receipt.outcome !== "passed" ||
        receipt.exitCode !== 0 ||
        typeof receipt.startedAt !== "string" ||
        typeof receipt.completedAt !== "string" ||
        !Number.isFinite(Date.parse(receipt.startedAt)) ||
        !Number.isFinite(Date.parse(receipt.completedAt)) ||
        Date.parse(receipt.completedAt) > Date.now() ||
        Date.parse(receipt.startedAt) > Date.parse(receipt.completedAt) ||
        receipt.durationMs !== Date.parse(receipt.completedAt) - Date.parse(receipt.startedAt) ||
        recordedSource.commit !== source.commit ||
        recordedSource.tree !== source.tree ||
        recordedSource.dirty !== source.dirty ||
        recordedExecution.runId !== execution.runId ||
        !Number.isSafeInteger(recordedExecution.attempt) ||
        Number(recordedExecution.attempt) < 1 ||
        Number(recordedExecution.attempt) > execution.attempt ||
        !Array.isArray(receipt.errors) ||
        receipt.errors.length !== 0 ||
        !Array.isArray(receipt.reports) ||
        receipt.reports.length !== CI_REPORTS[lane].length
      )
        throw new Error("Invalid lane identity or outcome.");
      for (const [index, definition] of CI_REPORTS[lane].entries()) {
        const recorded = object(receipt.reports[index]);
        const actual = await readReportEvidence(
          root,
          definition,
          receipt.startedAt,
          receipt.completedAt,
        );
        if (
          recorded.path !== actual.path ||
          recorded.sha256 !== actual.sha256 ||
          recorded.bytes !== actual.bytes ||
          JSON.stringify(recorded.summary) !== JSON.stringify(actual.summary)
        )
          throw new Error("Lane report content changed.");
      }
      // Every authority-bearing field and report digest was checked against this run.
      lanes.push(receipt as unknown as LaneReceipt);
    } catch {
      errors.push(`Missing, failed, mismatched or changed ${lane} evidence.`);
    }
  }
  const index = {
    schemaVersion: 1 as const,
    source,
    execution,
    generatedAt: new Date().toISOString(),
    outcome: errors.length === 0 ? ("passed" as const) : ("failed" as const),
    lanes,
    errors,
  };
  await writeJson(root, ".artifacts/ci/qualification-index.json", index);
  return index;
}

async function main(): Promise<void> {
  const [command, lane, startedAt, exitCode, ...extra] = process.argv.slice(2);
  if (command === "prepare" && lane && startedAt === undefined) {
    await prepareCiLane(process.cwd(), lane as CiLane);
  } else if (command === "stage" && lane === undefined && process.env.RUNNER_TEMP) {
    await stageCiEvidence(process.cwd(), process.env.RUNNER_TEMP);
  } else if (
    command === "record" &&
    lane &&
    startedAt &&
    exitCode !== undefined &&
    extra.length === 0
  ) {
    const receipt = await recordCiLane({
      root: process.cwd(),
      lane: lane as CiLane,
      startedAt,
      exitCode: Number(exitCode),
    });
    if (Number(exitCode) === 0 && receipt.outcome !== "passed") {
      process.stderr.write(`${receipt.errors.join("\n")}\n`);
      process.exitCode = 1;
    }
  } else if (command === "aggregate" && lane === undefined) {
    const index = await aggregateCiEvidence({
      root: process.cwd(),
      results: JSON.parse(process.env.STREAMSKOPE_CI_RESULTS ?? "{}") as unknown,
    });
    process.stdout.write(`CI evidence ${index.outcome}; ${index.lanes.length} complete lanes.\n`);
    if (index.outcome !== "passed") {
      process.stderr.write(`${index.errors.join("\n")}\n`);
      process.exitCode = 1;
    }
  } else {
    throw new Error(
      "Usage: ci-evidence.ts prepare <lane> | record <lane> <started-at> <exit-code> | aggregate | stage",
    );
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href)
  void main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : "CI evidence failed."}\n`);
    process.exitCode = 1;
  });
