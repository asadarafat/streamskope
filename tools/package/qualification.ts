import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readdir, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { fileURLToPath } from "node:url";

import { readBoundedFile } from "../../src/platform/node/bounded-file";
import {
  CI_LANES,
  CI_REPORTS,
  readReportEvidence,
  type CiExecution,
  type ReportEvidence,
} from "../check/ci-evidence";
import {
  validateLocalQualificationBundle,
  type LocalQualificationReceipt,
} from "../check/qualification";

import { releaseIdentity, type ReleaseComponent } from "./release-version";
import {
  validateBrowserInstallerEvidence,
  type BrowserInstallerEvidence,
} from "./browser-installer-evidence";

const MAX_REPORT = 4 * 1024 * 1024;
const COMMIT = /^[a-f0-9]{40}$/u;
const FILE = /^[A-Za-z0-9][A-Za-z0-9._+-]{0,180}$/u;
export const QUALIFICATION_LIMITATIONS = [
  "Source and packaging checks do not establish live EDA or NSP behavior.",
  "Local acceptance requires an attached receipt of unchanged clean source, documentation and soak checks.",
  "Native package checks do not establish every operating-system, browser or recovery scenario.",
] as const;

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid qualification evidence object.");
  return value as Record<string, unknown>;
}
const digest = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const encoded = (value: unknown): Buffer => Buffer.from(`${JSON.stringify(value, null, 2)}\n`);

export interface ReleasePayloadEvidence {
  readonly file: string;
  readonly bytes: number;
  readonly sha256: string;
}
interface ReleaseSource {
  readonly commit: string;
  readonly tree: string;
}
export interface ReleaseQualification {
  readonly schemaVersion: 1;
  readonly component: ReleaseComponent;
  readonly version: string;
  readonly tag: string;
  readonly source: ReleaseSource;
  readonly generatedAt: string;
  readonly execution: CiExecution;
  readonly sourceQualification: {
    readonly outcome: "passed";
    readonly indexSha256: string;
    readonly reports: readonly ReportEvidence[];
  };
  readonly packaging: {
    readonly outcome: "passed";
    readonly prerequisiteJobs: Readonly<Record<string, "success">>;
    readonly browserInstallers: readonly BrowserInstallerEvidence[];
  };
  readonly payloads: readonly ReleasePayloadEvidence[];
  readonly local:
    | {
        readonly outcome: "not-recorded";
      }
    | {
        readonly outcome: "recorded";
        readonly sourceEquivalence: "same-commit" | "identical-tree";
        readonly receipt: LocalQualificationReceipt;
      };
  readonly acceptance: {
    readonly sourceAndPackaging: "passed";
    readonly local: "passed" | "not-recorded";
    readonly live: { readonly eda: string; readonly nsp: string };
  };
  readonly limitations: readonly string[];
}

export function releaseSource(root: string, commit: string): ReleaseSource {
  if (!COMMIT.test(commit)) throw new Error("Qualification requires an exact Git commit.");
  try {
    const git = (argument: string): string =>
      execFileSync("git", ["rev-parse", "--verify", argument], {
        cwd: root,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      }).trim();
    if (git(`${commit}^{commit}`) !== commit) throw new Error("Invalid commit.");
    return { commit, tree: git(`${commit}^{tree}`) };
  } catch {
    throw new Error(
      "Qualification source commit is unavailable locally; fetch the release and executed source commits first.",
    );
  }
}

/** Validate the supplied clean CI evidence against Git objects, not a stamped build worktree. */
export async function validateReleaseCiEvidence(
  directory: string,
  source: ReleaseSource,
  execution: CiExecution,
): Promise<ReleaseQualification["sourceQualification"]> {
  const bytes = await readBoundedFile(
    join(directory, ".artifacts/ci/qualification-index.json"),
    MAX_REPORT,
    { rejectSymlinks: true },
  );
  const index = object(JSON.parse(bytes.toString("utf8")));
  const recordedSource = object(index.source),
    recordedExecution = object(index.execution);
  if (
    index.schemaVersion !== 1 ||
    index.outcome !== "passed" ||
    recordedSource.commit !== source.commit ||
    recordedSource.tree !== source.tree ||
    recordedSource.dirty !== false ||
    recordedExecution.runId !== execution.runId ||
    !Number.isSafeInteger(recordedExecution.attempt) ||
    Number(recordedExecution.attempt) < 1 ||
    Number(recordedExecution.attempt) > execution.attempt ||
    !Array.isArray(index.errors) ||
    index.errors.length ||
    !Array.isArray(index.lanes) ||
    index.lanes.length !== CI_LANES.length
  )
    throw new Error("CI qualification does not match this clean source and release run.");
  const reports: ReportEvidence[] = [];
  for (const [position, name] of CI_LANES.entries()) {
    const lane = object(index.lanes[position]),
      laneSource = object(lane.source),
      laneExecution = object(lane.execution);
    if (
      lane.schemaVersion !== 1 ||
      lane.lane !== name ||
      lane.outcome !== "passed" ||
      lane.exitCode !== 0 ||
      laneSource.commit !== source.commit ||
      laneSource.tree !== source.tree ||
      laneSource.dirty !== false ||
      laneExecution.runId !== execution.runId ||
      !Number.isSafeInteger(laneExecution.attempt) ||
      Number(laneExecution.attempt) < 1 ||
      Number(laneExecution.attempt) > execution.attempt ||
      typeof lane.startedAt !== "string" ||
      typeof lane.completedAt !== "string" ||
      !Number.isFinite(Date.parse(lane.startedAt)) ||
      !Number.isFinite(Date.parse(lane.completedAt)) ||
      Date.parse(lane.startedAt) > Date.parse(lane.completedAt) ||
      Date.parse(lane.completedAt) > Date.now() ||
      lane.durationMs !== Date.parse(lane.completedAt) - Date.parse(lane.startedAt) ||
      !Array.isArray(lane.errors) ||
      lane.errors.length ||
      !Array.isArray(lane.reports) ||
      lane.reports.length !== CI_REPORTS[name].length
    )
      throw new Error("A required CI lane is missing or unqualified.");
    for (const [index, definition] of CI_REPORTS[name].entries()) {
      const actual = await readReportEvidence(
        directory,
        definition,
        lane.startedAt,
        lane.completedAt,
      );
      if (!isDeepStrictEqual(lane.reports[index], actual))
        throw new Error("CI report content or summary changed after qualification.");
      reports.push(actual);
    }
  }
  return { outcome: "passed", indexSha256: digest(bytes), reports };
}

async function payloadEvidence(directories: readonly string[]): Promise<ReleasePayloadEvidence[]> {
  const payloads: ReleasePayloadEvidence[] = [];
  for (const directory of directories)
    for (const file of await readdir(directory)) {
      if (file === "SHA256SUMS") continue;
      if (!FILE.test(file) || file.startsWith("qualification-"))
        throw new Error("Unexpected release payload filename.");
      if (payloads.some((item) => item.file === file))
        throw new Error("Duplicate release payload filename.");
      const path = join(directory, file),
        stat = await lstat(path);
      if (!stat.isFile() || stat.size <= 0 || stat.size >= 2 * 1024 ** 3)
        throw new Error("Release payload must be a nonempty bounded regular file.");
      const hash = createHash("sha256");
      for await (const bytes of createReadStream(path)) hash.update(bytes as Buffer);
      payloads.push({ file, bytes: stat.size, sha256: hash.digest("hex") });
    }
  if (!payloads.length) throw new Error("Release qualification requires packaged payloads.");
  return payloads.sort((a, b) => a.file.localeCompare(b.file));
}

export function releasePrerequisiteJobs(
  component: ReleaseComponent,
  value: unknown,
): Record<string, "success"> {
  const jobs = object(value);
  const names =
    component === "desktop"
      ? ["prepare", "checks", "desktop", "eda", "browser", "registry", "installer"]
      : ["prepare", "checks", "plugin"];
  if (
    Object.keys(jobs).length !== names.length ||
    names.some((name) => object(jobs[name]).result !== "success")
  )
    throw new Error("Every component packaging prerequisite must succeed before assembly.");
  return Object.fromEntries(names.map((name) => [name, "success"]));
}

export async function assembleReleaseQualification(options: {
  root: string;
  evidenceDirectory: string;
  assetDirectories: readonly string[];
  outputDirectory: string;
  component: ReleaseComponent;
  version: string;
  commit: string;
  execution: CiExecution;
  prerequisites: unknown;
  installerDirectory?: string;
  browserManifest?: string;
}): Promise<ReleaseQualification> {
  const identity = releaseIdentity(options.component, options.version);
  if (
    !options.execution.runId ||
    !/^[1-9]\d*$/u.test(options.execution.runId) ||
    !Number.isSafeInteger(options.execution.attempt) ||
    options.execution.attempt < 1
  )
    throw new Error("Release qualification requires workflow run provenance.");
  const source = releaseSource(options.root, options.commit);
  const sourceQualification = await validateReleaseCiEvidence(
    options.evidenceDirectory,
    source,
    options.execution,
  );
  const jobs = releasePrerequisiteJobs(identity.component, options.prerequisites);
  const browserInstallers: BrowserInstallerEvidence[] = [];
  if (identity.component === "desktop") {
    if (!options.installerDirectory || !options.browserManifest)
      throw new Error(
        "Desktop qualification requires both native installer receipts and the browser manifest.",
      );
    const manifest = object(
      JSON.parse(
        (
          await readBoundedFile(options.browserManifest, MAX_REPORT, { rejectSymlinks: true })
        ).toString("utf8"),
      ),
    );
    const image = object(manifest.registry).reference;
    if (
      typeof image !== "string" ||
      manifest.version !== identity.version ||
      manifest.sourceRevision !== source.commit
    )
      throw new Error("Browser manifest differs from the release source.");
    for (const arch of ["amd64", "arm64"] as const) {
      const bytes = await readBoundedFile(
        join(
          options.installerDirectory,
          `browser-installer-${arch}`,
          `browser-installer-${arch}.json`,
        ),
        64 * 1024,
        { rejectSymlinks: true },
      );
      browserInstallers.push(
        validateBrowserInstallerEvidence(JSON.parse(bytes.toString("utf8")), {
          version: identity.version,
          sourceRevision: source.commit,
          platform: `linux/${arch}`,
          image,
          execution: options.execution,
        }),
      );
    }
  }
  const report: ReleaseQualification = {
    schemaVersion: 1,
    component: identity.component,
    version: identity.version,
    tag: identity.tag,
    source,
    generatedAt: new Date().toISOString(),
    execution: options.execution,
    sourceQualification,
    packaging: { outcome: "passed", prerequisiteJobs: jobs, browserInstallers },
    payloads: await payloadEvidence(options.assetDirectories),
    local: { outcome: "not-recorded" },
    acceptance: {
      sourceAndPackaging: "passed",
      local: "not-recorded",
      live: { eda: "not-recorded", nsp: "not-recorded" },
    },
    limitations: QUALIFICATION_LIMITATIONS,
  };
  await writeReleaseQualification(options.outputDirectory, report);
  return report;
}

export function qualificationFile(version: string): string {
  return `qualification-v${version}.json`;
}
export function qualificationChecksums(report: ReleaseQualification, bytes: Uint8Array): string {
  return (
    [
      ...report.payloads.map((payload) => `${payload.sha256}  ${payload.file}`),
      `${digest(bytes)}  ${qualificationFile(report.version)}`,
    ].join("\n") + "\n"
  );
}
export async function writeReleaseQualification(
  directory: string,
  report: ReleaseQualification,
): Promise<void> {
  const bytes = encoded(report);
  await writeFile(join(directory, qualificationFile(report.version)), bytes, {
    flag: "wx",
    mode: 0o644,
  });
  await writeFile(join(directory, "SHA256SUMS"), qualificationChecksums(report, bytes), {
    mode: 0o644,
  });
}

/** Preserve the execution commit; a merge may establish equivalence only through its Git tree. */
export async function attachLocalQualification(
  root: string,
  report: ReleaseQualification,
  directory: string,
): Promise<ReleaseQualification> {
  const receipt = await validateLocalQualificationBundle(directory);
  if (
    receipt.outcome !== "passed" ||
    !receipt.source.unchanged ||
    receipt.source.start.dirty ||
    !receipt.source.end ||
    receipt.source.end.dirty
  )
    throw new Error(
      "Only completed qualification of unchanged clean source can satisfy release acceptance.",
    );
  const executed = releaseSource(root, receipt.source.start.commit),
    released = releaseSource(root, report.source.commit);
  if (
    executed.tree !== receipt.source.start.tree ||
    executed.tree !== released.tree ||
    released.tree !== report.source.tree
  )
    throw new Error("Local qualification and release source trees differ.");
  const stage = (name: string): string =>
    receipt.stages.find((item) => item.stage === name)?.outcome ?? "not-recorded";
  if (["shared", "soak", "docs"].some((name) => stage(name) !== "passed"))
    throw new Error("Local source, documentation and soak acceptance must all pass.");
  if (report.component !== "desktop" && stage(`${report.component}-live`) !== "passed")
    throw new Error("A plugin release requires passed live qualification of its target system.");
  return {
    ...report,
    local: {
      outcome: "recorded",
      sourceEquivalence: executed.commit === released.commit ? "same-commit" : "identical-tree",
      receipt,
    },
    acceptance: {
      sourceAndPackaging: "passed",
      local: "passed",
      live: { eda: stage("eda-live"), nsp: stage("nsp-live") },
    },
  };
}

async function main(): Promise<void> {
  const { runDraftQualification } = await import("./qualification-draft.js");
  await runDraftQualification();
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : "Qualification failed."}\n`);
    process.exitCode = 1;
  });
