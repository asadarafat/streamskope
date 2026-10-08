import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { lstat, mkdir, readFile, readlink, rename, rm, writeFile } from "node:fs/promises";
import { dirname, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";

import { CI_REPORTS, qualificationSource, readReportEvidence } from "./ci-evidence";
import type { QualificationSource } from "./ci-evidence";

export const LOCAL_STAGES = ["shared", "soak", "docs", "eda-live", "nsp-live"] as const;
export type LocalStage = (typeof LOCAL_STAGES)[number];
type Summary = Record<string, string | number | boolean | null | readonly string[]>;
type Source = QualificationSource & { fingerprint: string };
type Outcome = "not-run" | "running" | "passed" | "failed" | "skipped";
export interface LocalReport {
  path: string;
  sha256: string;
  bytes: number;
  summary: Summary;
}
export interface LocalStageReceipt {
  stage: LocalStage;
  outcome: Outcome;
  startedAt: string | null;
  completedAt: string | null;
  reason: "not-configured" | "stage-failed" | null;
  reports: LocalReport[];
}
export interface LocalQualificationReceipt {
  schemaVersion: 1;
  execution: {
    id: string;
    kind: "local";
    startedAt: string;
    completedAt: string | null;
    exitCode: number | null;
  };
  source: { start: Source; end: Source | null; unchanged: boolean };
  environment: { platform: string; architecture: string; node: string };
  outcome: "running" | "passed" | "failed";
  stages: LocalStageReceipt[];
  notQualified: readonly string[];
}

const OUTPUTS: Record<LocalStage, string> = {
  shared: ".artifacts/ci/vitest.json",
  soak: "dist/performance/qualification-soak.json",
  docs: ".artifacts/website/qualification.json",
  "eda-live": "dist/ci/eda-live.json",
  "nsp-live": "dist/ci/nsp-live.json",
};
const LIMITATIONS = [
  "native-desktop-recovery",
  "installed-native-live-eda-nsp",
  "live-broker-renderer-endurance",
  "cross-platform-packaging",
] as const;
export const LOCAL_LIVE_CHECKS: Record<"eda-live" | "nsp-live", readonly string[]> = {
  "eda-live": [
    "cluster-version",
    "application-version",
    "source-discovery",
    "capture-ready",
    "lease-renewal",
    "kafka-record-receipt",
    "observed-health",
    "relationship-discovery",
    "source-preserved",
    "stop",
    "owned-resource-cleanup",
    "repeat-stop",
  ],
  "nsp-live": [
    "clean-package-install",
    "repeat-install-retains-one-plugin",
    "running-target-version",
    "combined-workflow",
    "core-profile-test",
    "profile-saved",
    "execution-cleanup",
    "repeat-reuses-profile-and-workflow",
    "saved-profile-connect",
    "topic-discovery",
    "known-record-produce-and-receipt",
    "observed-health",
    "relationship-discovery",
    "hot-update-preserves-and-refreshes-profile",
    "hot-remove-preserves-profile",
    "reinstall-reuses-profile",
    "foreign-request-marker-cleanup-refused",
    "real-interruption-recovery",
    "pending-cleanup-blocks-removal",
    "repeat-cleanup",
    "recovery-reuses-profile-and-workflow",
    "secrets-absent-from-renderer-events",
    "owned-topic-deletion-confirmed",
  ],
};
const sha = (value: Buffer | string): string => createHash("sha256").update(value).digest("hex");
const now = (): string => new Date().toISOString();
const object = (value: unknown): Record<string, unknown> => {
  assert(
    value !== null && typeof value === "object" && !Array.isArray(value),
    "Invalid evidence object.",
  );
  return value as Record<string, unknown>;
};
const exactKeys = (value: object, keys: string[]): void => {
  assert.deepEqual(Object.keys(value).sort(), [...keys].sort(), "Unexpected evidence fields.");
};
const count = (value: unknown, minimum = 0): number => {
  assert(
    typeof value === "number" && Number.isSafeInteger(value) && value >= minimum,
    "Invalid evidence count.",
  );
  return value;
};
const finite = (value: unknown, minimum = 0): number => {
  assert(
    typeof value === "number" && Number.isFinite(value) && value >= minimum,
    "Invalid evidence measurement.",
  );
  return value;
};
const timestamp = (value: unknown): number => {
  assert(
    typeof value === "string" && new Date(value).toISOString() === value,
    "Invalid evidence timestamp.",
  );
  return Date.parse(value);
};

async function ownedPath(root: string, relative: string): Promise<string> {
  const base = resolve(root);
  const rootStat = await lstat(base);
  assert(
    rootStat.isDirectory() && !rootStat.isSymbolicLink(),
    "Evidence root must be an owned directory.",
  );
  const file = resolve(base, relative);
  assert(file.startsWith(`${base}${sep}`), "Evidence must stay inside its directory.");
  for (let path = dirname(file); path !== base; path = dirname(path)) {
    const stat = await lstat(path).catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return null;
      throw error;
    });
    assert(
      !stat || (stat.isDirectory() && !stat.isSymbolicLink()),
      "Evidence parents must be owned directories.",
    );
  }
  return file;
}

async function bytesAt(root: string, relative: string, startedAt?: string): Promise<Buffer> {
  const file = await ownedPath(root, relative);
  const stat = await lstat(file);
  assert(
    stat.isFile() && stat.nlink === 1 && stat.size > 0 && stat.size <= 64 * 1024 * 1024,
    "Evidence must be a bounded regular file.",
  );
  assert(
    startedAt === undefined || stat.mtimeMs >= timestamp(startedAt),
    "Evidence predates this stage.",
  );
  return readFile(file);
}

async function writeJson(root: string, relative: string, value: unknown): Promise<void> {
  const file = await ownedPath(root, relative);
  await mkdir(dirname(file), { recursive: true, mode: 0o700 });
  const temporary = `${file}.${randomUUID()}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600, flag: "wx" });
  await rename(temporary, file);
}

/** Hash content and mode, including nonignored untracked files, without publishing file names. */
export async function localQualificationSource(root: string): Promise<Source> {
  const entries = execFileSync("git", ["ls-files", "-v", "-z"], {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
  });
  assert(
    entries
      .split("\0")
      .filter(Boolean)
      .every((entry) => entry.startsWith("H ")),
    "Qualification does not support assume-unchanged, skip-worktree or unmerged index entries.",
  );
  const source = qualificationSource(root);
  const paths = execFileSync(
    "git",
    ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
    { cwd: root, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 },
  );
  const hash = createHash("sha256");
  for (const path of [...new Set(paths.split("\0").filter(Boolean))].sort()) {
    const file = resolve(root, path);
    const stat = await lstat(file).catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return null;
      throw error;
    });
    hash.update(`${path}\0${stat?.mode ?? "missing"}\0`);
    if (stat?.isSymbolicLink()) hash.update(await readlink(file));
    else if (stat?.isFile()) hash.update(sha(await readFile(file)));
    else assert(!stat, "Qualification source contains an unsupported file type.");
    hash.update("\0");
  }
  assert.deepEqual(
    qualificationSource(root),
    source,
    "Source changed while its fingerprint was recorded.",
  );
  return { ...source, fingerprint: hash.digest("hex") };
}

function directory(root: string, id: string): string {
  assert(
    /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/u.test(id),
    "Invalid qualification identity.",
  );
  return resolve(root, ".artifacts/qualification", id);
}
async function receiptAt(root: string, id: string): Promise<LocalQualificationReceipt> {
  const receipt = JSON.parse(
    (await bytesAt(directory(root, id), "qualification.json")).toString("utf8"),
  ) as LocalQualificationReceipt;
  assert.equal(receipt.execution.id, id, "Stored qualification identity changed.");
  return receipt;
}
async function save(root: string, receipt: LocalQualificationReceipt): Promise<void> {
  await writeJson(
    root,
    `.artifacts/qualification/${receipt.execution.id}/qualification.json`,
    receipt,
  );
}

export async function beginLocalQualification(root: string): Promise<LocalQualificationReceipt> {
  const receipt: LocalQualificationReceipt = {
    schemaVersion: 1,
    execution: {
      id: randomUUID(),
      kind: "local",
      startedAt: now(),
      completedAt: null,
      exitCode: null,
    },
    source: { start: await localQualificationSource(root), end: null, unchanged: false },
    environment: { platform: process.platform, architecture: process.arch, node: process.version },
    outcome: "running",
    stages: LOCAL_STAGES.map((stage) => ({
      stage,
      outcome: "not-run",
      startedAt: null,
      completedAt: null,
      reason: null,
      reports: [],
    })),
    notQualified: LIMITATIONS,
  };
  await save(root, receipt);
  return receipt;
}

export async function beginLocalStage(root: string, id: string, stage: LocalStage): Promise<void> {
  const receipt = await receiptAt(root, id);
  assert.equal(receipt.outcome, "running");
  const selected = receipt.stages.find(
    (item) => item.outcome !== "passed" && item.outcome !== "skipped",
  );
  assert(
    selected?.stage === stage && selected.outcome === "not-run",
    "Stages must execute once, in order.",
  );
  const paths = [
    OUTPUTS[stage],
    ...(stage === "docs" ? [".artifacts/website/browser-checks.json"] : []),
  ];
  for (const path of paths) await ownedPath(root, path);
  for (const path of paths) await rm(resolve(root, path), { force: true });
  selected.outcome = "running";
  selected.startedAt = now();
  await save(root, receipt);
}

function liveSummary(stage: LocalStage, data: Record<string, unknown>): Summary {
  assert(
    ["passed", "failed", "skipped"].includes(String(data.outcome)),
    "Incomplete live evidence.",
  );
  assert(
    Array.isArray(data.checks) && data.checks.every((item) => typeof item === "string"),
    "Invalid completed live checks.",
  );
  if (data.outcome === "skipped") {
    assert(
      data.reasonCode === "not-configured" && data.checks.length === 0,
      "Live skips require an explicit reason.",
    );
    return { outcome: "skipped", checks: 0, checkIds: [], reason: "not-configured" };
  }
  const target = stage === "eda-live" ? data.targetVersion : object(data.target ?? {}).product;
  assert(
    (data.outcome === "failed" && target === undefined) ||
      (typeof target === "string" && /^\d+\.\d+(?:\.\d+)?(?:[-+][\w.-]+)?$/u.test(target)),
    "Live evidence needs the API target version.",
  );
  assert(
    typeof data.apiCertificateVerification === "boolean",
    "Live evidence needs its certificate policy.",
  );
  return {
    outcome: String(data.outcome),
    checks: data.checks.length,
    checkIds: data.checks,
    targetVersion: target === undefined ? null : target,
    apiCertificateVerification: data.apiCertificateVerification,
    apiTrust:
      stage === "eda-live"
        ? String(data.apiTrust)
        : data.apiCertificateVerification
          ? "system-trust"
          : "verification-disabled",
    scope:
      stage === "eda-live" ? "development-host-live-capture" : "development-host-plugin-lifecycle",
  };
}

async function collect(
  root: string,
  receipt: LocalQualificationReceipt,
  stage: LocalStageReceipt,
  completedAt: string,
): Promise<void> {
  assert(stage.startedAt);
  const raw = await bytesAt(root, OUTPUTS[stage.stage], stage.startedAt);
  const data = object(JSON.parse(raw.toString("utf8")));
  let summary: Summary;
  if (stage.stage === "shared") {
    const result = await readReportEvidence(
      root,
      CI_REPORTS.shared[0]!,
      stage.startedAt,
      completedAt,
    );
    assert(result.sha256 === sha(raw), "Report changed while it was being collected.");
    summary = { outcome: "passed", ...result.summary };
  } else {
    const capturedAt = data.checkedAt ?? data.capturedAt ?? data.completedAt;
    assert(
      timestamp(capturedAt) >= timestamp(stage.startedAt) &&
        timestamp(capturedAt) <= timestamp(completedAt),
      "Report execution is outside this stage.",
    );
    if (stage.stage === "soak") {
      const evidence = object(data.evidence);
      const config = object(evidence.config);
      assert(
        data.outcome === "passed" &&
          data.check === "bounded-stream-replay" &&
          object(evidence.qualification).passed === true,
      );
      assert(
        config.seconds === 60 &&
          config.rate === 1000 &&
          config.mixed === true &&
          config.roundTrip === true,
        "Expected the complete local soak configuration.",
      );
      summary = {
        outcome: "passed",
        seconds: 60,
        offeredRate: 1000,
        elapsedMs: finite(evidence.elapsedMs, 60_000),
        generated: count(evidence.generated, 1),
        published: count(evidence.published, 1),
        hostDisplayDrops: count(evidence.hostDisplayDrops),
        peakRss: count(evidence.peakRss, 1),
        scope: "node-ingestion-replay",
      };
    } else if (stage.stage === "docs") {
      const definition = CI_REPORTS.docs.find(({ path }) => path === OUTPUTS.docs);
      assert(definition);
      const result = await readReportEvidence(root, definition, stage.startedAt, completedAt);
      assert(result.sha256 === sha(raw), "Report changed while it was being collected.");
      summary = { outcome: "passed", ...result.summary };
    } else summary = liveSummary(stage.stage, data);
  }
  validateSummary(stage.stage, summary);
  const projection = {
    schemaVersion: 1,
    stage: stage.stage,
    origin: { sha256: sha(raw), bytes: raw.length },
    summary,
  };
  const path = `evidence/${stage.stage}.json`;
  const bundle = directory(root, receipt.execution.id);
  await writeJson(bundle, path, projection);
  const copied = await bytesAt(bundle, path);
  stage.reports = [{ path, sha256: sha(copied), bytes: copied.length, summary }];
  stage.outcome = summary.outcome as "passed" | "failed" | "skipped";
  stage.reason =
    stage.outcome === "skipped"
      ? "not-configured"
      : stage.outcome === "failed"
        ? "stage-failed"
        : null;
}

export async function completeLocalStage(
  root: string,
  id: string,
  name: LocalStage,
): Promise<void> {
  const receipt = await receiptAt(root, id);
  const stage = receipt.stages.find((item) => item.stage === name);
  assert(
    receipt.outcome === "running" && stage?.outcome === "running",
    "No matching running stage.",
  );
  stage.completedAt = now();
  await collect(root, receipt, stage, stage.completedAt);
  assert(["passed", "skipped"].includes(stage.outcome), "The stage did not qualify.");
  await save(root, receipt);
}

export async function finishLocalQualification(
  root: string,
  id: string,
  exitCode: number,
): Promise<LocalQualificationReceipt> {
  count(exitCode);
  assert(exitCode <= 255);
  const receipt = await receiptAt(root, id);
  assert.equal(receipt.outcome, "running");
  const completedAt = now();
  for (const stage of receipt.stages.filter((item) => item.outcome === "running")) {
    // Retain safe completed checks when a producer wrote a failed report; missing reports stay explicit.
    await collect(root, receipt, stage, completedAt).catch(() => undefined);
    stage.outcome = "failed";
    stage.reason = "stage-failed";
    stage.completedAt = completedAt;
  }
  receipt.source.end = await localQualificationSource(root);
  receipt.source.unchanged =
    JSON.stringify(receipt.source.start) === JSON.stringify(receipt.source.end);
  receipt.execution.completedAt = completedAt;
  receipt.execution.exitCode = exitCode;
  receipt.outcome =
    exitCode === 0 &&
    receipt.source.unchanged &&
    receipt.stages.every((item) => item.outcome === "passed" || item.outcome === "skipped")
      ? "passed"
      : "failed";
  await save(root, receipt);
  return receipt;
}

function validateSummary(stage: LocalStage, summary: Summary): void {
  assert(["passed", "failed", "skipped"].includes(String(summary.outcome)));
  if (stage === "shared") {
    exactKeys(summary, ["outcome", "total", "passed", "skipped", "todo"]);
    assert(summary.outcome === "passed");
    count(summary.total, 1);
    count(summary.passed, 1);
    count(summary.skipped);
    count(summary.todo);
    assert(Number(summary.passed) <= Number(summary.total));
  } else if (stage === "soak") {
    exactKeys(summary, [
      "outcome",
      "seconds",
      "offeredRate",
      "elapsedMs",
      "generated",
      "published",
      "hostDisplayDrops",
      "peakRss",
      "scope",
    ]);
    assert(
      summary.outcome === "passed" &&
        summary.seconds === 60 &&
        summary.offeredRate === 1000 &&
        summary.scope === "node-ingestion-replay",
    );
    finite(summary.elapsedMs, 60_000);
    count(summary.generated, 1);
    count(summary.published, 1);
    count(summary.hostDisplayDrops);
    count(summary.peakRss, 1);
  } else if (stage === "docs") {
    exactKeys(summary, [
      "outcome",
      "htmlPages",
      "routes",
      "media",
      "mediaReason",
      "mediaFingerprint",
    ]);
    assert(summary.outcome === "passed");
    count(summary.htmlPages, 1);
    count(summary.routes, 1);
    assert(
      (summary.media === "passed" && summary.mediaReason === null) ||
        (summary.media === "skipped" && summary.mediaReason === "unchanged-media-inputs"),
    );
    assert(
      typeof summary.mediaFingerprint === "string" &&
        /^[a-f0-9]{64}$/u.test(summary.mediaFingerprint),
    );
  } else if (summary.outcome === "skipped") {
    exactKeys(summary, ["outcome", "checks", "checkIds", "reason"]);
    assert(summary.checks === 0 && summary.reason === "not-configured");
    assert.deepEqual(summary.checkIds, []);
  } else {
    exactKeys(summary, [
      "outcome",
      "checks",
      "checkIds",
      "targetVersion",
      "apiCertificateVerification",
      "apiTrust",
      "scope",
    ]);
    count(summary.checks, summary.outcome === "passed" ? 1 : 0);
    assert(
      (summary.outcome === "failed" && summary.targetVersion === null) ||
        (typeof summary.targetVersion === "string" &&
          /^\d+\.\d+(?:\.\d+)?(?:[-+][\w.-]+)?$/u.test(summary.targetVersion)),
    );
    assert(
      Array.isArray(summary.checkIds) &&
        summary.checkIds.length === summary.checks &&
        new Set(summary.checkIds).size === summary.checks &&
        summary.checkIds.every(
          (id: unknown) => typeof id === "string" && LOCAL_LIVE_CHECKS[stage].includes(id),
        ),
    );
    if (summary.outcome === "passed")
      assert.equal(
        summary.checkIds.length,
        LOCAL_LIVE_CHECKS[stage].length,
        "Passed live evidence must include every required operation, including cleanup.",
      );
    assert(typeof summary.apiCertificateVerification === "boolean");
    assert(
      ["system-trust", "provided-ca", "verification-disabled"].includes(String(summary.apiTrust)),
    );
    assert(summary.apiCertificateVerification === (summary.apiTrust !== "verification-disabled"));
    assert(
      summary.scope ===
        (stage === "eda-live"
          ? "development-host-live-capture"
          : "development-host-plugin-lifecycle"),
    );
  }
}

/** Validate a self-contained, sanitized bundle without relying on mutable reports elsewhere. */
export async function validateLocalQualificationBundle(
  bundle: string,
): Promise<LocalQualificationReceipt> {
  const receipt = JSON.parse(
    (await bytesAt(bundle, "qualification.json")).toString("utf8"),
  ) as LocalQualificationReceipt;
  exactKeys(receipt, [
    "schemaVersion",
    "execution",
    "source",
    "environment",
    "outcome",
    "stages",
    "notQualified",
  ]);
  assert(receipt.schemaVersion === 1 && ["passed", "failed"].includes(receipt.outcome));
  exactKeys(receipt.execution, ["id", "kind", "startedAt", "completedAt", "exitCode"]);
  directory("/", receipt.execution.id);
  assert(receipt.execution.kind === "local" && count(receipt.execution.exitCode) <= 255);
  const start = timestamp(receipt.execution.startedAt),
    end = timestamp(receipt.execution.completedAt);
  assert(start <= end);
  exactKeys(receipt.source, ["start", "end", "unchanged"]);
  for (const source of [receipt.source.start, receipt.source.end]) {
    assert(source);
    exactKeys(source, ["commit", "tree", "dirty", "fingerprint"]);
    assert(
      /^[a-f0-9]{40,64}$/u.test(source.commit) &&
        /^[a-f0-9]{40,64}$/u.test(source.tree) &&
        /^[a-f0-9]{64}$/u.test(source.fingerprint) &&
        typeof source.dirty === "boolean",
    );
  }
  assert.equal(
    receipt.source.unchanged,
    JSON.stringify(receipt.source.start) === JSON.stringify(receipt.source.end),
  );
  exactKeys(receipt.environment, ["platform", "architecture", "node"]);
  assert(
    /^[a-z0-9]{2,16}$/u.test(receipt.environment.platform) &&
      /^[a-z0-9]{2,16}$/u.test(receipt.environment.architecture) &&
      /^v\d+\.\d+\.\d+$/u.test(receipt.environment.node),
  );
  assert.deepEqual(receipt.notQualified, LIMITATIONS);
  assert(Array.isArray(receipt.stages) && receipt.stages.length === LOCAL_STAGES.length);
  let previous = start,
    stopped = false;
  for (const [index, stage] of receipt.stages.entries()) {
    exactKeys(stage, ["stage", "outcome", "startedAt", "completedAt", "reason", "reports"]);
    assert(
      stage.stage === LOCAL_STAGES[index] &&
        ["passed", "failed", "skipped", "not-run"].includes(stage.outcome),
    );
    assert(Array.isArray(stage.reports) && stage.reports.length <= 1);
    if (stage.outcome === "not-run") {
      assert(
        stage.startedAt === null &&
          stage.completedAt === null &&
          stage.reason === null &&
          stage.reports.length === 0,
      );
      stopped = true;
      continue;
    }
    assert(
      !stopped &&
        timestamp(stage.startedAt) >= previous &&
        timestamp(stage.completedAt) >= timestamp(stage.startedAt) &&
        timestamp(stage.completedAt) <= end,
    );
    previous = timestamp(stage.completedAt);
    assert(
      stage.reason ===
        (stage.outcome === "skipped"
          ? "not-configured"
          : stage.outcome === "failed"
            ? "stage-failed"
            : null),
    );
    if (stage.outcome === "failed") stopped = true;
    else assert(stage.reports.length === 1);
    for (const report of stage.reports) {
      exactKeys(report, ["path", "sha256", "bytes", "summary"]);
      assert(report.path === `evidence/${stage.stage}.json`);
      const bytes = await bytesAt(bundle, report.path);
      assert(
        report.sha256 === sha(bytes) && report.bytes === bytes.length,
        "Evidence hash or size changed.",
      );
      const projection = object(JSON.parse(bytes.toString("utf8")));
      exactKeys(projection, ["schemaVersion", "stage", "origin", "summary"]);
      assert(projection.schemaVersion === 1 && projection.stage === stage.stage);
      const origin = object(projection.origin);
      exactKeys(origin, ["sha256", "bytes"]);
      assert(typeof origin.sha256 === "string" && /^[a-f0-9]{64}$/u.test(origin.sha256));
      count(origin.bytes, 1);
      assert.deepEqual(projection.summary, report.summary);
      validateSummary(stage.stage, report.summary);
      assert(stage.outcome === "failed" || stage.outcome === report.summary.outcome);
    }
  }
  assert.equal(
    receipt.outcome === "passed",
    receipt.execution.exitCode === 0 &&
      receipt.source.unchanged &&
      receipt.stages.every((stage) => stage.outcome === "passed" || stage.outcome === "skipped"),
  );
  return receipt;
}

async function main(): Promise<void> {
  const [command, id, stage] = process.argv.slice(2);
  const root = process.cwd();
  if (command === "begin" && id === undefined)
    process.stdout.write(`${(await beginLocalQualification(root)).execution.id}\n`);
  else if (command === "stage" && id && LOCAL_STAGES.includes(stage as LocalStage))
    await beginLocalStage(root, id, stage as LocalStage);
  else if (command === "complete" && id && LOCAL_STAGES.includes(stage as LocalStage))
    await completeLocalStage(root, id, stage as LocalStage);
  else if (command === "finish" && id && /^\d+$/u.test(stage ?? "")) {
    const receipt = await finishLocalQualification(root, id, Number(stage));
    process.stdout.write(
      `Local qualification: ${receipt.outcome}. Receipt: .artifacts/qualification/${id}/qualification.json\n`,
    );
    if (receipt.outcome !== "passed") process.exitCode = 1;
  } else throw new Error("Invalid local qualification command.");
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  void main().catch(() => {
    process.stderr.write(
      "Local qualification evidence could not be recorded or validated; inspect the stage outcome and source state.\n",
    );
    process.exitCode = 1;
  });
}
