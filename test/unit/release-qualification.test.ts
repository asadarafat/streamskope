import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterEach, expect, it } from "vitest";

import {
  aggregateCiEvidence,
  CI_LANES,
  CI_REPORTS,
  qualificationSource,
  recordCiLane,
} from "../../tools/check/ci-evidence";
import {
  beginLocalQualification,
  beginLocalStage,
  completeLocalStage,
  finishLocalQualification,
  LOCAL_STAGES,
} from "../../tools/check/qualification";
import { browserInstallerEvidence } from "../../tools/package/browser-installer-evidence";
import { BROWSER_DATA_COMPATIBILITY } from "../../src/platform/node/browser-data-compatibility";
import { browserDataEvidenceFixture } from "../support/browser-data-evidence";
import {
  assembleReleaseQualification,
  attachLocalQualification,
  qualificationChecksums,
  qualificationFile,
  releaseSource,
  validateReleaseCiEvidence,
  type ReleaseQualification,
} from "../../tools/package/qualification";
import {
  enrichDraftQualification,
  type DraftQualificationPort,
  type DraftReleaseState,
} from "../../tools/package/qualification-draft";
import { qualificationNotes } from "../../tools/package/release";

const directories: string[] = [];
const execution = { runId: "1234", attempt: 1 };
const env = { GITHUB_ACTIONS: "true", GITHUB_RUN_ID: "1234", GITHUB_RUN_ATTEMPT: "1" };
const sha = (value: Buffer | string): string => createHash("sha256").update(value).digest("hex");
const bytes = (value: unknown): Buffer => Buffer.from(`${JSON.stringify(value, null, 2)}\n`);
const git = (root: string, ...args: string[]): string =>
  execFileSync("git", args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
const commit = (root: string, message: string): void => {
  git(
    root,
    "-c",
    "user.name=Qualification test",
    "-c",
    "user.email=test@example.invalid",
    "commit",
    "--quiet",
    "--allow-empty",
    "-m",
    message,
  );
};
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});
async function json(root: string, path: string, value: unknown): Promise<void> {
  await mkdir(dirname(join(root, path)), { recursive: true });
  await writeFile(join(root, path), bytes(value));
  const timestamp = new Date();
  await utimes(join(root, path), timestamp, timestamp);
}
async function fixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-release-evidence-"));
  directories.push(root);
  git(root, "init", "--quiet");
  await writeFile(join(root, ".gitignore"), ".artifacts/\ntest-results/\ndist/\n");
  await writeFile(join(root, "source.txt"), "qualified\n");
  git(root, "add", ".");
  commit(root, "fixture");
  return root;
}
function vitest(): object {
  return {
    startTime: Date.now(),
    success: true,
    numTotalTests: 3,
    numPassedTests: 3,
    numPendingTests: 0,
    numTodoTests: 0,
    numFailedTests: 0,
    numFailedTestSuites: 0,
  };
}
async function ci(root: string): Promise<void> {
  for (const lane of CI_LANES) {
    const startedAt = new Date(Date.now() - 1000).toISOString();
    for (const report of CI_REPORTS[lane])
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
                htmlPages: 2,
                routes: 2,
                browserSha256: "a".repeat(64),
                media: { outcome: "passed", fingerprint: "a".repeat(64) },
              }
            : report.kind === "vitest"
              ? vitest()
              : {
                  errors: [],
                  stats: {
                    startTime: new Date().toISOString(),
                    expected: 2,
                    skipped: 0,
                    unexpected: 0,
                    flaky: 0,
                    duration: 1,
                  },
                },
      );
    expect((await recordCiLane({ root, lane, startedAt, exitCode: 0, env })).outcome).toBe(
      "passed",
    );
  }
  expect(
    (
      await aggregateCiEvidence({
        root,
        env,
        results: Object.fromEntries(CI_LANES.map((lane) => [lane, { result: "success" }])),
      })
    ).outcome,
  ).toBe("passed");
}
function browserManifest(sourceRevision: string): object {
  const version = "1.2.3";
  const image = `ghcr.io/asadarafat/streamskope:${version}`;
  return {
    schemaVersion: 4,
    version,
    sourceRevision,
    dataCompatibility: BROWSER_DATA_COMPATIBILITY,
    registry: {
      schemaVersion: 1,
      version,
      sourceRevision,
      image,
      digest: `sha256:${"a".repeat(64)}`,
      reference: `${image}@sha256:${"a".repeat(64)}`,
      platforms: ["amd64", "arm64"].map((arch, index) => ({
        platform: `linux/${arch}`,
        imageId: `sha256:${(index ? "c" : "b").repeat(64)}`,
        manifestDigest: `sha256:${(index ? "e" : "d").repeat(64)}`,
      })),
    },
  };
}
async function report(
  root: string,
  component: "desktop" | "eda" = "desktop",
): Promise<ReleaseQualification> {
  await ci(root);
  const source = qualificationSource(root);
  const output = join(root, "dist/assets");
  await mkdir(output, { recursive: true });
  await writeFile(join(output, "payload.bin"), "packaged bytes");
  const image = `ghcr.io/asadarafat/streamskope:1.2.3@sha256:${"a".repeat(64)}`;
  await json(root, "dist/browser.json", browserManifest(source.commit));
  if (component === "desktop")
    await json(
      root,
      "dist/assets/streamskope-1.2.3-container.json",
      browserManifest(source.commit),
    );
  for (const arch of ["amd64", "arm64"])
    await json(
      root,
      `dist/installers/browser-installer-${arch}/browser-installer-${arch}.json`,
      browserInstallerEvidence(
        {
          version: "1.2.3",
          sourceRevision: source.commit,
          platform: `linux/${arch}`,
          image,
          startedAt: new Date(Date.now() - 1000).toISOString(),
          preflight: browserDataEvidenceFixture(
            "1.2.3",
            `sha256:${(arch === "amd64" ? "b" : "c").repeat(64)}`,
          ),
        },
        env,
      ),
    );
  return assembleReleaseQualification({
    root,
    evidenceDirectory: root,
    assetDirectories: [output],
    outputDirectory: output,
    component,
    version: "1.2.3",
    commit: source.commit,
    execution,
    prerequisites: Object.fromEntries(
      (component === "desktop"
        ? ["prepare", "checks", "desktop", "eda", "browser", "registry", "installer"]
        : ["prepare", "checks", "plugin"]
      ).map((name) => [name, { result: "success" }]),
    ),
    installerDirectory: join(root, "dist/installers"),
    browserManifest: join(root, "dist/browser.json"),
  });
}
async function local(root: string): Promise<string> {
  const receipt = await beginLocalQualification(root);
  for (const stage of LOCAL_STAGES) {
    await beginLocalStage(root, receipt.execution.id, stage);
    const timestamp = new Date().toISOString();
    if (stage === "shared") await json(root, ".artifacts/ci/vitest.json", vitest());
    else if (stage === "soak")
      await json(root, "dist/performance/qualification-soak.json", {
        capturedAt: timestamp,
        check: "bounded-stream-replay",
        outcome: "passed",
        evidence: {
          qualification: { passed: true },
          config: { seconds: 60, rate: 1000, mixed: true, roundTrip: true },
          elapsedMs: 60_001,
          generated: 60000,
          published: 60000,
          hostDisplayDrops: 0,
          peakRss: 400_000_000,
        },
      });
    else if (stage === "docs")
      await json(root, ".artifacts/website/qualification.json", {
        schemaVersion: 1,
        outcome: "passed",
        startedAt: timestamp,
        completedAt: timestamp,
        htmlPages: 2,
        routes: 2,
        browserSha256: "a".repeat(64),
        media: { outcome: "passed", fingerprint: "a".repeat(64) },
      });
    else
      await json(root, `dist/ci/${stage}.json`, {
        outcome: "skipped",
        checkedAt: timestamp,
        checks: [],
        reasonCode: "not-configured",
      });
    await completeLocalStage(root, receipt.execution.id, stage);
  }
  expect((await finishLocalQualification(root, receipt.execution.id, 0)).outcome).toBe("passed");
  return join(root, ".artifacts/qualification", receipt.execution.id);
}

it("assembles hashed source and native receipts without claiming local or live qualification", async () => {
  const root = await fixture();
  const result = await report(root);
  expect(result.acceptance).toEqual({
    sourceAndPackaging: "passed",
    local: "not-recorded",
    live: { eda: "not-recorded", nsp: "not-recorded" },
  });
  expect(result.packaging.browserInstallers.map((item) => item.platform)).toEqual([
    "linux/amd64",
    "linux/arm64",
  ]);
  expect(result.sourceQualification.reports).toHaveLength(
    CI_LANES.flatMap((lane) => CI_REPORTS[lane]).length,
  );
  const content = await readFile(join(root, "dist/assets", qualificationFile(result.version)));
  expect(await readFile(join(root, "dist/assets/SHA256SUMS"), "utf8")).toBe(
    qualificationChecksums(result, content),
  );
  expect(result.payloads).toContainEqual({
    file: "payload.bin",
    bytes: 14,
    sha256: sha("packaged bytes"),
  });
  expect(result.payloads).toHaveLength(2);
  const notes = qualificationNotes(result, "asadarafat/streamskope");
  expect(notes).toContain("https://github.com/asadarafat/streamskope/actions/runs/1234");
  expect(notes).toContain(result.source.commit);
  expect(notes).toContain("explicitly unrecorded");
});

it("accepts already-qualified clean source despite intentional release stamping and rejects another run", async () => {
  const root = await fixture();
  await ci(root);
  const source = releaseSource(root, qualificationSource(root).commit);
  await writeFile(join(root, "source.txt"), "disposable release stamping");
  await expect(validateReleaseCiEvidence(root, source, execution)).resolves.toMatchObject({
    outcome: "passed",
  });
  await expect(
    validateReleaseCiEvidence(root, source, { runId: "999", attempt: 1 }),
  ).rejects.toThrow(/release run/u);
});

it("fails on changed report bytes, missing lane, dirty source or wrong tree", async () => {
  const root = await fixture();
  await ci(root);
  const source = releaseSource(root, qualificationSource(root).commit);
  const indexFile = ".artifacts/ci/qualification-index.json";
  const original = JSON.parse(await readFile(join(root, indexFile), "utf8")) as Awaited<
    ReturnType<typeof aggregateCiEvidence>
  >;
  for (const replacement of [
    { ...original, lanes: original.lanes.slice(1) },
    { ...original, source: { ...original.source, dirty: true } },
    { ...original, source: { ...original.source, tree: "a".repeat(40) } },
  ]) {
    await json(root, indexFile, replacement);
    await expect(validateReleaseCiEvidence(root, source, execution)).rejects.toThrow();
  }
  await json(root, indexFile, original);
  await json(root, CI_REPORTS.shared[0]!.path, { ...vitest(), numTotalTests: 9 });
  await expect(validateReleaseCiEvidence(root, source, execution)).rejects.toThrow();
});

it("retains the executed commit when a clean merged commit has an identical Git tree", async () => {
  const root = await fixture();
  const bundle = await local(root);
  const executed = qualificationSource(root).commit;
  commit(root, "squash merge identity");
  const released = await report(root);
  const result = await attachLocalQualification(root, released, bundle);
  expect(result.local).toMatchObject({
    outcome: "recorded",
    sourceEquivalence: "identical-tree",
    receipt: { source: { start: { commit: executed } } },
  });
  expect(result.source.commit).not.toBe(executed);
  expect(result.acceptance.live).toEqual({ eda: "skipped", nsp: "skipped" });
});

it("refuses dirty or different executed trees and never treats skipped live checks as plugin acceptance", async () => {
  const root = await fixture();
  const bundle = await local(root);
  await writeFile(join(root, "source.txt"), "new implementation");
  git(root, "add", ".");
  commit(root, "different source");
  const release = await report(root);
  await expect(attachLocalQualification(root, release, bundle)).rejects.toThrow(/trees differ/u);
  const cleanBundle = await local(root);
  await expect(
    attachLocalQualification(root, { ...release, component: "eda" }, cleanBundle),
  ).rejects.toThrow(/requires passed live/u);
  await writeFile(join(root, "source.txt"), "uncommitted");
  const dirtyBundle = await local(root);
  await expect(attachLocalQualification(root, release, dirtyBundle)).rejects.toThrow(
    /clean source/u,
  );
});

class Draft implements DraftQualificationPort {
  readonly assets = new Map<string, Buffer>();
  draft = true;
  immutable = false;
  uploads: string[] = [];
  onUpload?: (name: string) => void;
  onInspect?: () => void;
  onDownload?: (name: string) => void;
  constructor(readonly report: ReleaseQualification) {
    const content = bytes(report);
    this.assets.set("payload.bin", Buffer.from("packaged bytes"));
    if (report.component === "desktop")
      this.assets.set(
        `streamskope-${report.version}-container.json`,
        bytes(browserManifest(report.source.commit)),
      );
    this.assets.set(qualificationFile(report.version), content);
    this.assets.set("SHA256SUMS", Buffer.from(qualificationChecksums(report, content)));
  }
  inspect(): Promise<DraftReleaseState> {
    this.onInspect?.();
    return Promise.resolve({
      id: 12,
      tag_name: this.report.tag,
      draft: this.draft,
      immutable: this.immutable,
      assets: [...this.assets].map(([name, value], index) => ({
        id: index + 1,
        name,
        size: value.length,
        digest: `sha256:${sha(value)}`,
      })),
    });
  }
  source(): Promise<string> {
    return Promise.resolve(this.report.source.commit);
  }
  download(_tag: string, name: string): Promise<Buffer> {
    this.onDownload?.(name);
    return Promise.resolve(this.assets.get(name)!);
  }
  upload(_tag: string, name: string, contents: Buffer): Promise<void> {
    this.uploads.push(name);
    this.onUpload?.(name);
    this.assets.set(name, contents);
    return Promise.resolve();
  }
}
async function draftFixture(): Promise<{
  root: string;
  port: Draft;
  options: Parameters<typeof enrichDraftQualification>[0];
}> {
  const root = await fixture();
  const result = await report(root);
  const localBundle = await local(root);
  const port = new Draft(result);
  return {
    root,
    port,
    options: {
      root,
      tag: result.tag,
      localBundle,
      backupDirectory: join(root, ".artifacts/backups"),
      port,
    },
  };
}
it("enriches only the two draft evidence files, preserving payloads and originals", async () => {
  const { root, port, options } = await draftFixture();
  const payload = port.assets.get("payload.bin"),
    original = port.assets.get(qualificationFile(port.report.version));
  const result = await enrichDraftQualification(options);
  expect(result.acceptance.local).toBe("passed");
  expect(port.uploads).toEqual([qualificationFile(result.version), "SHA256SUMS"]);
  expect(port.assets.get("payload.bin")).toBe(payload);
  expect(
    await readFile(join(root, ".artifacts/backups", qualificationFile(result.version))),
  ).toEqual(original);
  expect(port.assets.get("SHA256SUMS")!.toString()).toBe(
    qualificationChecksums(result, port.assets.get(qualificationFile(result.version))!),
  );
});
it.each(["published", "immutable", "payload drift"])(
  "refuses %s before mutating any draft evidence",
  async (failure) => {
    const { port, options } = await draftFixture();
    if (failure === "published") port.draft = false;
    if (failure === "immutable") port.immutable = true;
    if (failure === "payload drift")
      port.assets.set("payload.bin", Buffer.from("unqualified replacement"));
    await expect(enrichDraftQualification(options)).rejects.toThrow();
    expect(port.uploads).toEqual([]);
  },
);
it.each(["wrong-native-image", "local-staged", "old-receipt"])(
  "refuses %s even when the uploaded report and checksums agree",
  async (failure) => {
    const { port, options } = await draftFixture();
    const changed = structuredClone(port.report);
    const native = changed.packaging.browserInstallers[0]!;
    if (failure === "wrong-native-image")
      Object.assign(native.preflight, { imageId: `sha256:${"f".repeat(64)}` });
    if (failure === "local-staged") Object.assign(native, { deliveryScope: "local-staged" });
    if (failure === "old-receipt") Object.assign(native, { schemaVersion: 2 });
    const content = bytes(changed);
    port.assets.set(qualificationFile(changed.version), content);
    port.assets.set("SHA256SUMS", Buffer.from(qualificationChecksums(changed, content)));
    await expect(enrichDraftQualification(options)).rejects.toThrow();
    expect(port.uploads).toEqual([]);
  },
);
it("verifies downloaded manifest bytes against GitHub's digest before trusting its native images", async () => {
  const { port, options } = await draftFixture();
  port.onDownload = (name): void => {
    if (name.endsWith("-container.json"))
      port.assets.set(name, Buffer.from("substituted manifest"));
  };
  await expect(enrichDraftQualification(options)).rejects.toThrow(/manifest download differs/u);
  expect(port.uploads).toEqual([]);
});
it.each(["publication", "checksum-upload"])(
  "fails explicitly with retained recovery files after concurrent %s",
  async (failure) => {
    const { port, options } = await draftFixture();
    port.onUpload = (name): void => {
      if (failure === "publication") port.draft = false;
      else if (name === "SHA256SUMS") throw new Error("simulated upload failed");
    };
    await expect(enrichDraftQualification(options)).rejects.toThrow(
      /Do not publish.*originals retained/u,
    );
    expect(await readFile(join(options.backupDirectory, "SHA256SUMS"), "utf8")).toContain(
      "payload.bin",
    );
    expect(port.uploads).toHaveLength(failure === "publication" ? 1 : 2);
  },
);

it("stops before replacing checksums if another writer changes the uploaded report", async () => {
  const { port, options } = await draftFixture();
  const originalChecksums = port.assets.get("SHA256SUMS");
  let inspections = 0;
  port.onInspect = (): void => {
    if (++inspections === 3)
      port.assets.set(qualificationFile(port.report.version), Buffer.from("concurrent report"));
  };
  await expect(enrichDraftQualification(options)).rejects.toThrow(/did not complete/u);
  expect(port.uploads).toEqual([qualificationFile(port.report.version)]);
  expect(port.assets.get("SHA256SUMS")).toBe(originalChecksums);
});

it("does not report success if publication occurs during final download verification", async () => {
  const { port, options } = await draftFixture();
  port.onDownload = (name): void => {
    if (name === "SHA256SUMS" && port.uploads.length === 2) port.draft = false;
  };
  await expect(enrichDraftQualification(options)).rejects.toThrow(/did not complete/u);
});

it("refuses release CI assembly without source evidence before creating any assets", () => {
  expect(() =>
    execFileSync(
      process.execPath,
      [
        "--import",
        "tsx",
        "tools/package/release.ts",
        "unused-assets",
        "1.2.3",
        "a".repeat(40),
        "unused-notes",
        "unused-output",
      ],
      { env: { ...process.env, GITHUB_ACTIONS: "true" }, stdio: ["ignore", "pipe", "pipe"] },
    ),
  ).toThrow(/Release CI requires source qualification evidence/u);
});
