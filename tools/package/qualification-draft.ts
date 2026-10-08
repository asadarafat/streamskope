import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { isDeepStrictEqual, promisify } from "node:util";

import { CI_LANES, CI_REPORTS, type ReportEvidence } from "../check/ci-evidence";

import {
  validateBrowserInstallerEvidence,
  type BrowserInstallerEvidence,
} from "./browser-installer-evidence";
import {
  attachLocalQualification,
  qualificationChecksums,
  qualificationFile,
  releasePrerequisiteJobs,
  QUALIFICATION_LIMITATIONS,
  type ReleasePayloadEvidence,
  type ReleaseQualification,
} from "./qualification";
import { releaseIdentity } from "./release-version";

const execute = promisify(execFile);
const MAX_REPORT = 4 * 1024 * 1024;
const SHA = /^[a-f0-9]{64}$/u;
const COMMIT = /^[a-f0-9]{40}$/u;
const FILE = /^[A-Za-z0-9][A-Za-z0-9._+-]{0,180}$/u;
const digest = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const encoded = (value: unknown): Buffer => Buffer.from(`${JSON.stringify(value, null, 2)}\n`);
function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid draft evidence object.");
  return value as Record<string, unknown>;
}

export interface DraftReleaseAsset {
  readonly id: number;
  readonly name: string;
  readonly size: number;
  readonly digest: string;
}
export interface DraftReleaseState {
  readonly id: number;
  readonly tag_name: string;
  readonly draft: boolean;
  readonly immutable: boolean;
  readonly assets: readonly DraftReleaseAsset[];
}
export interface DraftQualificationPort {
  inspect(tag: string): Promise<DraftReleaseState>;
  source(tag: string): Promise<string>;
  download(tag: string, name: string): Promise<Buffer>;
  upload(tag: string, name: string, contents: Buffer): Promise<void>;
}

function assertDraft(state: DraftReleaseState, tag: string): void {
  if (
    !Number.isSafeInteger(state.id) ||
    state.id < 1 ||
    state.tag_name !== tag ||
    state.draft !== true ||
    state.immutable !== false ||
    !Array.isArray(state.assets)
  )
    throw new Error("Qualification can update only an unpublished, mutable draft release.");
}
function validateDraftPayloads(state: DraftReleaseState, report: ReleaseQualification): void {
  const expectedNames = [
    ...report.payloads.map((payload) => payload.file),
    qualificationFile(report.version),
    "SHA256SUMS",
  ].sort();
  if (!isDeepStrictEqual(state.assets.map((asset) => asset.name).sort(), expectedNames))
    throw new Error("Draft release asset inventory changed.");
  for (const payload of report.payloads) {
    const asset = state.assets.find((item) => item.name === payload.file)!;
    if (
      !FILE.test(payload.file) ||
      !SHA.test(payload.sha256) ||
      !Number.isSafeInteger(payload.bytes) ||
      payload.bytes <= 0 ||
      asset.digest !== `sha256:${payload.sha256}` ||
      asset.size !== payload.bytes
    )
      throw new Error("Draft payload hashes or sizes no longer match qualified artifacts.");
  }
}

/** Reconstruct the public fields of an existing report; arbitrary asset additions never propagate. */
function publicDraftReport(
  value: unknown,
  identity: ReturnType<typeof releaseIdentity>,
): ReleaseQualification {
  const report = object(value) as unknown as ReleaseQualification;
  if (
    report.schemaVersion !== 1 ||
    report.component !== identity.component ||
    report.version !== identity.version ||
    report.tag !== identity.tag ||
    !COMMIT.test(report.source?.commit ?? "") ||
    !COMMIT.test(report.source?.tree ?? "") ||
    typeof report.generatedAt !== "string" ||
    !Number.isFinite(Date.parse(report.generatedAt)) ||
    Date.parse(report.generatedAt) > Date.now() ||
    !report.execution ||
    !/^[1-9]\d*$/u.test(report.execution.runId ?? "") ||
    !Number.isSafeInteger(report.execution.attempt) ||
    report.execution.attempt < 1 ||
    report.sourceQualification?.outcome !== "passed" ||
    !SHA.test(report.sourceQualification.indexSha256) ||
    report.packaging?.outcome !== "passed" ||
    report.acceptance?.sourceAndPackaging !== "passed" ||
    !Array.isArray(report.payloads) ||
    !Array.isArray(report.sourceQualification.reports)
  )
    throw new Error("Draft qualification report has an invalid identity or outcome.");
  const definitions = CI_LANES.flatMap((lane) => CI_REPORTS[lane]);
  if (report.sourceQualification.reports.length !== definitions.length)
    throw new Error("Draft qualification report inventory is incomplete.");
  const reports = report.sourceQualification.reports.map((item: ReportEvidence, index) => {
    const definition = definitions[index]!;
    const keys =
      definition.kind === "accessibility"
        ? ["findings"]
        : definition.kind === "docs"
          ? ["htmlPages", "routes", "media", "mediaReason", "mediaFingerprint"]
          : definition.kind === "vitest"
            ? ["total", "passed", "skipped", "todo"]
            : ["passed", "skipped", "durationMs"];
    if (
      item.path !== definition.path ||
      !SHA.test(item.sha256) ||
      !Number.isSafeInteger(item.bytes) ||
      item.bytes <= 0 ||
      !isDeepStrictEqual(Object.keys(object(item.summary)).sort(), keys.sort()) ||
      (definition.kind === "docs"
        ? !Number.isSafeInteger(item.summary.htmlPages) ||
          Number(item.summary.htmlPages) < 1 ||
          !Number.isSafeInteger(item.summary.routes) ||
          Number(item.summary.routes) < 1 ||
          typeof item.summary.mediaFingerprint !== "string" ||
          !SHA.test(item.summary.mediaFingerprint) ||
          !(
            (item.summary.media === "passed" && item.summary.mediaReason === null) ||
            (item.summary.media === "skipped" &&
              item.summary.mediaReason === "unchanged-media-inputs")
          )
        : Object.values(item.summary).some(
            (value) => typeof value !== "number" || !Number.isFinite(value) || value < 0,
          ))
    )
      throw new Error("Draft source qualification reports are malformed.");
    return {
      path: item.path,
      sha256: item.sha256,
      bytes: item.bytes,
      summary: { ...item.summary },
    };
  });
  const jobs = releasePrerequisiteJobs(
    identity.component,
    Object.fromEntries(
      Object.entries(object(report.packaging.prerequisiteJobs)).map(([key, result]) => [
        key,
        { result },
      ]),
    ),
  );
  const platforms = identity.component === "desktop" ? ["linux/amd64", "linux/arm64"] : [];
  if (
    !Array.isArray(report.packaging.browserInstallers) ||
    report.packaging.browserInstallers.length !== platforms.length
  )
    throw new Error("Draft native installer evidence is incomplete.");
  const browserInstallers = report.packaging.browserInstallers.map(
    (item: BrowserInstallerEvidence, index) => {
      if (
        typeof item.image !== "string" ||
        !/^ghcr\.io\/[a-z0-9_./-]+@sha256:[a-f0-9]{64}$/u.test(item.image)
      )
        throw new Error("Native installer image is not digest-pinned.");
      return validateBrowserInstallerEvidence(item, {
        version: identity.version,
        sourceRevision: report.source.commit,
        platform: platforms[index]!,
        image: item.image,
        execution: report.execution,
      });
    },
  );
  return {
    schemaVersion: 1,
    component: identity.component,
    version: identity.version,
    tag: identity.tag,
    source: { commit: report.source.commit, tree: report.source.tree },
    generatedAt: report.generatedAt,
    execution: { runId: report.execution.runId, attempt: report.execution.attempt },
    sourceQualification: {
      outcome: "passed",
      indexSha256: report.sourceQualification.indexSha256,
      reports,
    },
    packaging: { outcome: "passed", prerequisiteJobs: jobs, browserInstallers },
    payloads: report.payloads.map(({ file, bytes, sha256 }: ReleasePayloadEvidence) => ({
      file,
      bytes,
      sha256,
    })),
    local: { outcome: "not-recorded" },
    acceptance: {
      sourceAndPackaging: "passed",
      local: "not-recorded",
      live: { eda: "not-recorded", nsp: "not-recorded" },
    },
    limitations: QUALIFICATION_LIMITATIONS,
  };
}

export async function enrichDraftQualification(options: {
  root: string;
  tag: string;
  localBundle: string;
  backupDirectory: string;
  port: DraftQualificationPort;
}): Promise<ReleaseQualification> {
  const { port, tag } = options;
  const before = await port.inspect(tag);
  assertDraft(before, tag);
  const match = /^(?:plugins\/(eda|nsp)\/)?v(.+)$/u.exec(tag);
  if (!match) throw new Error("Unknown qualification release tag.");
  const identity = releaseIdentity(match[1] ?? "desktop", match[2]!);
  const file = qualificationFile(identity.version);
  for (const [name, maximum] of [
    [file, MAX_REPORT],
    ["SHA256SUMS", 64 * 1024],
  ] as const) {
    const asset = before.assets.find((item) => item.name === name);
    if (
      !asset ||
      !Number.isSafeInteger(asset.size) ||
      asset.size <= 0 ||
      asset.size > maximum ||
      !/^sha256:[a-f0-9]{64}$/u.test(asset.digest)
    )
      throw new Error("Draft qualification assets need bounded sizes and GitHub SHA256 digests.");
  }
  const originalBytes = await port.download(tag, file),
    originalChecksums = await port.download(tag, "SHA256SUMS");
  if (originalBytes.length > MAX_REPORT || originalChecksums.length > 64 * 1024)
    throw new Error("Draft qualification exceeds its bound.");
  const original = publicDraftReport(JSON.parse(originalBytes.toString("utf8")), identity);
  validateDraftPayloads(before, original);
  for (const [name, bytes] of [
    [file, originalBytes],
    ["SHA256SUMS", originalChecksums],
  ] as const) {
    const asset = before.assets.find((item) => item.name === name)!;
    if (asset.digest !== `sha256:${digest(bytes)}` || asset.size !== bytes.length)
      throw new Error("Draft evidence download differs from GitHub's digest.");
  }
  if (originalChecksums.toString("utf8") !== qualificationChecksums(original, originalBytes))
    throw new Error("Draft checksum inventory differs from qualified payloads.");
  if ((await port.source(tag)) !== original.source.commit)
    throw new Error("Draft tag source differs from qualification.");
  const report = await attachLocalQualification(options.root, original, options.localBundle);
  const updated = encoded(report),
    checksums = Buffer.from(qualificationChecksums(report, updated));
  await mkdir(options.backupDirectory, { recursive: true, mode: 0o700 });
  await writeFile(join(options.backupDirectory, file), originalBytes, { flag: "wx", mode: 0o600 });
  await writeFile(join(options.backupDirectory, "SHA256SUMS"), originalChecksums, {
    flag: "wx",
    mode: 0o600,
  });
  const unchanged = async (allowReportUpdate: boolean): Promise<void> => {
    const current = await port.inspect(tag);
    assertDraft(current, tag);
    if (current.id !== before.id || (await port.source(tag)) !== original.source.commit)
      throw new Error("Draft identity changed during qualification update.");
    validateDraftPayloads(current, original);
    for (const asset of before.assets) {
      if (allowReportUpdate && asset.name === file) {
        const currentReport = current.assets.find((item) => item.name === file)!;
        if (
          currentReport.digest !== `sha256:${digest(updated)}` ||
          currentReport.size !== updated.length
        )
          throw new Error("Draft report changed concurrently before checksum upload.");
        continue;
      }
      if (
        !isDeepStrictEqual(
          current.assets.find((item) => item.name === asset.name),
          asset,
        )
      )
        throw new Error("Draft assets changed concurrently.");
    }
  };
  try {
    await unchanged(false);
    await port.upload(tag, file, updated);
    await unchanged(true);
    await port.upload(tag, "SHA256SUMS", checksums);
    const after = await port.inspect(tag);
    assertDraft(after, tag);
    if (after.id !== before.id || (await port.source(tag)) !== original.source.commit)
      throw new Error("Draft identity changed during qualification update.");
    validateDraftPayloads(after, report);
    for (const [name, bytes] of [
      [file, updated],
      ["SHA256SUMS", checksums],
    ] as const) {
      const asset = after.assets.find((item) => item.name === name)!;
      if (
        asset.digest !== `sha256:${digest(bytes)}` ||
        asset.size !== bytes.length ||
        !(await port.download(tag, name)).equals(bytes)
      )
        throw new Error("Uploaded qualification could not be verified.");
    }
    const verified = await port.inspect(tag);
    assertDraft(verified, tag);
    if (!isDeepStrictEqual(verified, after) || (await port.source(tag)) !== report.source.commit)
      throw new Error("Draft changed while uploaded evidence was being verified.");
    return report;
  } catch {
    throw new Error(
      `Draft qualification update did not complete. Do not publish; inspect the two evidence assets and restore or retry using the originals retained in ${options.backupDirectory}.`,
    );
  }
}

class GithubDraftQualification implements DraftQualificationPort {
  constructor(
    private readonly repository: string,
    private readonly directory: string,
  ) {}
  private async gh(args: string[]): Promise<string> {
    try {
      return (await execute("gh", args, { timeout: 60_000, maxBuffer: MAX_REPORT })).stdout;
    } catch {
      throw new Error("GitHub draft qualification request failed.");
    }
  }
  async inspect(tag: string): Promise<DraftReleaseState> {
    // The tag endpoint does not reliably resolve unpublished drafts; gh performs the draft lookup.
    const release = object(
      JSON.parse(
        await this.gh(["release", "view", tag, "--repo", this.repository, "--json", "databaseId"]),
      ),
    );
    if (!Number.isSafeInteger(release.databaseId) || Number(release.databaseId) < 1)
      throw new Error("Draft release was not found.");
    return JSON.parse(
      await this.gh(["api", `repos/${this.repository}/releases/${Number(release.databaseId)}`]),
    ) as DraftReleaseState;
  }
  async source(tag: string): Promise<string> {
    const ref = object(
      JSON.parse(
        await this.gh(["api", `repos/${this.repository}/git/ref/tags/${encodeURIComponent(tag)}`]),
      ),
    );
    const target = object(ref.object);
    if (target.type !== "commit" || typeof target.sha !== "string" || !COMMIT.test(target.sha))
      throw new Error("Draft requires an exact lightweight release tag.");
    return target.sha;
  }
  async download(tag: string, name: string): Promise<Buffer> {
    if (!FILE.test(name)) throw new Error("Invalid evidence asset name.");
    const state = await this.inspect(tag);
    assertDraft(state, tag);
    const asset = state.assets.find((item) => item.name === name);
    if (
      !asset ||
      !Number.isSafeInteger(asset.id) ||
      asset.id < 1 ||
      !Number.isSafeInteger(asset.size) ||
      asset.size <= 0 ||
      asset.size > MAX_REPORT
    )
      throw new Error("Invalid or oversized evidence download.");
    try {
      const result = await execute(
        "gh",
        [
          "api",
          `repos/${this.repository}/releases/assets/${asset.id}`,
          "-H",
          "Accept: application/octet-stream",
        ],
        { timeout: 60_000, maxBuffer: MAX_REPORT, encoding: "buffer" },
      );
      return result.stdout;
    } catch {
      throw new Error("Bounded GitHub evidence download failed.");
    }
  }
  async upload(tag: string, name: string, contents: Buffer): Promise<void> {
    if (!FILE.test(name)) throw new Error("Invalid evidence asset name.");
    const path = join(this.directory, name);
    await writeFile(path, contents, { mode: 0o600 });
    await this.gh(["release", "upload", tag, path, "--repo", this.repository, "--clobber"]);
  }
}

export async function runDraftQualification(): Promise<void> {
  const [tag, option, bundle, ...extra] = process.argv.slice(2);
  if (!tag || option !== "--local" || !bundle || extra.length)
    throw new Error("Usage: npm run package -- qualification <tag> --local <qualification-bundle>");
  const repository = process.env.GH_REPO ?? "asadarafat/streamskope";
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/u.test(repository))
    throw new Error("Invalid GitHub repository.");
  const parent = resolve(".artifacts/qualification-drafts");
  await mkdir(parent, { recursive: true, mode: 0o700 });
  const directory = await mkdtemp(join(parent, "update-"));
  const report = await enrichDraftQualification({
    root: process.cwd(),
    tag,
    localBundle: resolve(bundle),
    backupDirectory: join(directory, "original"),
    port: new GithubDraftQualification(repository, directory),
  });
  process.stdout.write(
    `Attached verified local qualification to draft ${report.tag}; execution source ${report.local.outcome === "recorded" ? report.local.receipt.source.start.commit : "unrecorded"}.\n`,
  );
}
