import { execFile } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import {
  compareSemanticVersions,
  parseSemanticPluginVersion,
} from "../../src/plugins/compatibility";

import { githubReader, type GithubRead } from "./release-changelog";
import { reviewedReleaseCommentary } from "./release-policy";
import type { ReleaseComponent } from "./release-version";

const execute = promisify(execFile);
const COMPONENTS: readonly ReleaseComponent[] = ["desktop", "eda", "nsp"];
const SHA = /^[a-f0-9]{40}$/u;
const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][\dA-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][\dA-Za-z-]*))*))?(?:\+[\dA-Za-z-]+(?:\.[\dA-Za-z-]+)*)?$/u;
const COMMENTARY: Record<ReleaseComponent, string> = {
  desktop: "website/docs/releases/unreleased.md",
  eda: "plugins/eda/RELEASE_NOTES.md",
  nsp: "plugins/nsp/RELEASE_NOTES.md",
};

export const EMPTY_DESKTOP_COMMENTARY = `---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

<!-- development-release-status -->
Published releases provide the component baselines. Run \`npm run docs -- pending\`
to inspect the PR-derived inventory after those baselines; this page records
additional release highlights, not a complete list of pending changes.
<!-- /development-release-status -->

## Release highlights

No additional release highlights recorded.
`;
export const EMPTY_PLUGIN_COMMENTARY =
  "## Release highlights\n\nNo additional release highlights recorded.\n";

export interface ReconciliationOptions {
  root: string;
  repository: string;
  readGithub: GithubRead;
  sourceSha?: string;
}
export interface PublishedRelease {
  component: ReleaseComponent;
  version: string;
  tag: string;
  sourceSha: string;
  archivePath: string;
  body: string;
  publishedAt: string;
  prerelease: boolean;
  ancestor: boolean;
  distance: number | null;
  release: Record<string, unknown>;
}
export interface ReconciliationChange {
  path: string;
  previous: string | null;
  contents: string;
  reason: "archive" | "desktop-baseline" | "plugin-index" | "commentary";
}
export interface CommentaryStatus {
  component: ReleaseComponent;
  status: "empty" | "clear" | "preserved" | "no-release";
  tag?: string;
  message: string;
}
export interface ReconciliationPlan {
  schemaVersion: 1;
  sourceSha: string;
  published: PublishedRelease[];
  latestStableDesktop: PublishedRelease | null;
  changes: ReconciliationChange[];
  commentary: CommentaryStatus[];
  warnings: string[];
}

async function git(root: string, ...args: string[]): Promise<string> {
  return (await execute("git", args, { cwd: root, maxBuffer: 32 * 1024 ** 2 })).stdout.trimEnd();
}
async function optionalFile(path: string): Promise<string | null> {
  try {
    return await readFile(path, "utf8");
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT")
      return null;
    throw error;
  }
}
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("GitHub returned malformed release metadata.");
  return value as Record<string, unknown>;
}
function releaseComponent(tag: string): ReleaseComponent | null {
  if (tag.startsWith("plugins/eda/v")) return "eda";
  if (tag.startsWith("plugins/nsp/v")) return "nsp";
  return /^v\d/u.test(tag) ? "desktop" : null;
}
function releaseOrder(left: PublishedRelease, right: PublishedRelease): number {
  return (
    compareSemanticVersions(left.version.split("+")[0]!, right.version.split("+")[0]!) ||
    left.tag.localeCompare(right.tag)
  );
}
async function publishedReleases(
  options: ReconciliationOptions,
  sourceSha: string,
): Promise<PublishedRelease[]> {
  const result: PublishedRelease[] = [];
  const ids = new Set<number>();
  const tags = new Set<string>();
  const seenPages = new Set<string>();
  for (let page = 1; page <= 10_000; page++) {
    const data = await options.readGithub(
      `/repos/${options.repository}/releases?per_page=100&page=${page}`,
    );
    if (!Array.isArray(data) || data.length > 100)
      throw new Error("GitHub returned an invalid release page.");
    const signature = JSON.stringify(data);
    if (data.length === 100 && seenPages.has(signature))
      throw new Error("GitHub repeated a release page; refusing an incomplete archive.");
    seenPages.add(signature);
    for (const item of data as unknown[]) {
      const release = object(item);
      if (typeof release.tag_name !== "string") throw new Error("GitHub release tag is missing.");
      const tag = release.tag_name;
      const component = releaseComponent(tag);
      if (!component) continue;
      if (typeof release.draft !== "boolean" || typeof release.prerelease !== "boolean")
        throw new Error(`GitHub returned incomplete release status for ${tag}.`);
      if (release.draft) continue;
      const prefix = component === "desktop" ? "v" : `plugins/${component}/v`;
      const version = tag.slice(prefix.length);
      let validVersion = false;
      try {
        parseSemanticPluginVersion(version);
        validVersion = !/^0\.0\.0(?:[+-]|$)/u.test(version);
      } catch {
        // A historical metadata-bearing SemVer remains valid; development identities do not.
      }
      if (!validVersion) throw new Error(`Published release ${tag} has an invalid version.`);
      if (
        !Number.isSafeInteger(release.id) ||
        (release.id as number) <= 0 ||
        typeof release.published_at !== "string" ||
        !/^\d{4}-\d\d-\d\dT/u.test(release.published_at) ||
        !Number.isFinite(Date.parse(release.published_at)) ||
        new Date(release.published_at).toISOString().slice(0, 10) !==
          release.published_at.slice(0, 10)
      )
        throw new Error(`Published release ${tag} has incomplete publication metadata.`);
      if (release.immutable !== true)
        throw new Error(`Published release ${tag} must be immutable before reconciliation.`);
      if (
        typeof release.body !== "string" ||
        !/^# \S[^\r\n]*(?:\r?\n|$)/u.test(release.body.trimStart())
      )
        throw new Error(`Published release ${tag} needs its final Markdown heading and body.`);
      if (tags.has(tag) || ids.has(release.id as number))
        throw new Error(`GitHub returned a duplicate release identity for ${tag}.`);
      tags.add(tag);
      ids.add(release.id as number);
      // Local fetched tags and the current API must agree; target_commitish is never a source identity.
      const source = await git(options.root, "rev-parse", "--verify", `refs/tags/${tag}^{commit}`);
      const resolved = object(
        await options.readGithub(`/repos/${options.repository}/commits/${encodeURIComponent(tag)}`),
      );
      if (!SHA.test(source) || resolved.sha !== source)
        throw new Error(`Published tag ${tag} differs from its fetched source commit.`);
      let ancestor = true;
      try {
        await git(options.root, "merge-base", "--is-ancestor", source, sourceSha);
      } catch (error) {
        if (error && typeof error === "object" && "code" in error && error.code === 1)
          ancestor = false;
        else throw error;
      }
      result.push({
        component,
        version,
        tag,
        sourceSha: source,
        archivePath: `website/docs/releases/${tag}.md`,
        body: release.body,
        publishedAt: release.published_at,
        prerelease: release.prerelease || version.includes("-"),
        ancestor,
        distance: ancestor
          ? Number(await git(options.root, "rev-list", "--count", `${source}..${sourceSha}`))
          : null,
        release,
      });
    }
    if (data.length < 100) return result;
  }
  throw new Error("GitHub release pagination exceeded its safety limit.");
}
function archiveFields(source: string): { fields: string; body: string } {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n([\s\S]*)$/u.exec(source);
  if (!match?.[1] || match[2] === undefined)
    throw new Error("Release archive has invalid frontmatter.");
  return { fields: match[1], body: match[2] };
}
function exactArchive(source: string, release: PublishedRelease): void {
  const { fields, body } = archiveFields(source);
  // Existing pages include one blank line separating YAML from the authoritative body.
  // Historical pages with the body immediately after YAML are also valid; no body trimming is allowed.
  if (body !== release.body && body !== `\n${release.body}` && body !== `\r\n${release.body}`)
    throw new Error(
      `Archived body differs from published ${release.tag}; review a correction instead of overwriting history.`,
    );
  if ((fields.match(/^title:[ \t]*\S[^\r\n]*\r?$/gmu) ?? []).length !== 1)
    throw new Error(`Archived ${release.tag} needs a title.`);
  if (release.component === "desktop") {
    for (const [name, expected] of [
      ["release_version", release.version.split("+")[0]!],
      ["release_tag", release.tag],
    ]) {
      const matches = Array.from(
        fields.matchAll(new RegExp(`^${name}:[ \\t]*["']?([^"'\\s]+)["']?[ \\t]*\\r?$`, "gmu")),
        (match) => match[1],
      );
      if (matches.length !== 1 || matches[0] !== expected)
        throw new Error(`Archived ${release.tag} has incorrect ${name}.`);
    }
  }
}
function archiveTitle(release: PublishedRelease): string {
  return release.component === "desktop"
    ? `StreamSkope ${release.tag}`
    : `${release.component === "eda" ? "EDA" : "NSP"} Connector ${release.version}`;
}
function releaseSummary(body: string, fallback: string): string {
  const prose: string[] = [];
  let fence: string | undefined;
  for (const line of body.replace(/<!--[\s\S]*?(?:-->|$)/gu, "").split(/\r?\n/u)) {
    const marker = /^ {0,3}(`{3,}|~{3,})(.*)$/u.exec(line);
    if (marker) {
      if (!fence) fence = marker[1];
      else if (
        marker[1]![0] === fence[0] &&
        marker[1]!.length >= fence.length &&
        !marker[2]!.trim()
      )
        fence = undefined;
      prose.push("");
      continue;
    }
    prose.push(
      fence || /^\s*(?:#{1,6}\s|\||>|[-*+]\s|\d+[.)]\s|(?:-{3,}|\*{3,}|_{3,})\s*$)/u.test(line)
        ? ""
        : line,
    );
  }
  return (
    prose
      .join("\n")
      .split(/\n\s*\n/u)
      .find((paragraph) => paragraph.trim())
      ?.replace(/\s+/gu, " ")
      .replace(/[`*_]/gu, "")
      .trim()
      .slice(0, 180) || fallback
  );
}
function archiveContents(release: PublishedRelease): string {
  const title = archiveTitle(release);
  const fields = [`title: ${title}`];
  if (release.component === "desktop") {
    const summary = releaseSummary(release.body, title);
    fields.push(
      `release_version: ${release.version.split("+")[0]!}`,
      `release_tag: ${release.tag}`,
      `release_date: ${JSON.stringify(release.publishedAt.slice(0, 10))}`,
      `release_summary: ${JSON.stringify(summary)}`,
    );
  }
  return `---\n${fields.join("\n")}\n---\n\n${release.body}`;
}
function neutralCommentary(source: string, component: ReleaseComponent): boolean {
  if (component !== "desktop")
    return (
      reviewedReleaseCommentary(source) === "" ||
      /^## Unreleased changes\r?\n\s*No plugin changes are pending release\.\s*$/u.test(source)
    );
  const { body } = archiveFields(source);
  if (reviewedReleaseCommentary(body) === "# Unreleased changes") return true;
  return /^\s*# Unreleased changes\r?\n\s*No desktop changes are pending release\.\s*(?:Desktop versions are assigned during release CI\. Pending plugin changes remain\s+in each plugin's release commentary and are released independently\.\s*)?$/u.test(
    body,
  );
}

/** Calculate every update before writing; never erase commentary changed after its released source. */
export async function planReleaseReconciliation(
  options: ReconciliationOptions,
): Promise<ReconciliationPlan> {
  if (
    !/^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\/[A-Za-z0-9_][A-Za-z0-9_.-]*$/u.test(
      options.repository,
    )
  )
    throw new Error("Release reconciliation needs a valid GitHub repository.");
  if ((await git(options.root, "rev-parse", "--is-shallow-repository")) !== "false")
    throw new Error("Release reconciliation needs complete Git history and fetched tags.");
  const sourceSha = options.sourceSha ?? (await git(options.root, "rev-parse", "HEAD"));
  if (!SHA.test(sourceSha) || (await git(options.root, "rev-parse", "HEAD")) !== sourceSha)
    throw new Error("Release reconciliation must use the exact current checkout commit.");
  const published = await publishedReleases(options, sourceSha);
  const changes: ReconciliationChange[] = [];
  const warnings: string[] = [];
  for (const release of published) {
    const previous = await optionalFile(join(options.root, release.archivePath));
    if (previous !== null) exactArchive(previous, release);
    else
      changes.push({
        path: release.archivePath,
        previous,
        contents: archiveContents(release),
        reason: "archive",
      });
  }
  const latest = published
    .filter((item) => item.component === "desktop" && !item.prerelease)
    .sort((a, b) => releaseOrder(b, a))[0];
  if (latest) {
    if (!latest.ancestor)
      throw new Error(
        `Highest stable desktop ${latest.tag} is outside the current source ancestry.`,
      );
    const path = "website/zensical.toml";
    const previous = await optionalFile(join(options.root, path));
    const matches = previous?.match(/^desktop_release\s*=\s*"[^"]+"\s*$/gmu) ?? [];
    if (previous === null || matches.length !== 1)
      throw new Error("Zensical needs one declared desktop release baseline.");
    const current = /"([^"]+)"/u.exec(matches[0])![1]!;
    if (current !== latest.tag) {
      const currentVersion = current.replace(/^v/u, "");
      if (
        !SEMVER.test(currentVersion) ||
        currentVersion.includes("-") ||
        compareSemanticVersions(currentVersion.split("+")[0]!, latest.version.split("+")[0]!) > 0
      )
        throw new Error(`Refusing to downgrade or replace an invalid desktop baseline ${current}.`);
      changes.push({
        path,
        previous,
        contents: previous.replace(
          /^desktop_release\s*=\s*"[^"]+"\s*$/mu,
          `desktop_release = "${latest.tag}"`,
        ),
        reason: "desktop-baseline",
      });
    }
  }
  const pluginReleases = published
    .filter((item) => item.component !== "desktop")
    .sort((a, b) => releaseOrder(b, a) || a.component.localeCompare(b.component));
  if (pluginReleases.length) {
    const path = "website/docs/releases/index.md";
    const previous = await optionalFile(join(options.root, path));
    const pattern = /<!-- plugin-release-history -->[\s\S]*?<!-- \/plugin-release-history -->/gu;
    const matches = previous?.match(pattern) ?? [];
    if (previous === null || matches.length !== 1)
      throw new Error("Plugin release history needs one bounded marker section.");
    const existing = matches[0];
    const represented = existing.split("\n").flatMap((line) => {
      const archive = /\]\((plugins\/(?:eda|nsp)\/v[^)]+\.md)\)/u.exec(line)?.[1];
      const assets =
        /\]\(https:\/\/github\.com\/([^/]+\/[^/]+)\/releases\/tag\/(plugins\/(?:eda|nsp)\/v[^)]+)\)/u.exec(
          line,
        );
      return archive && assets ? [`${archive}|${assets[1]}|${assets[2]}`] : [];
    });
    if (
      represented.length !== pluginReleases.length ||
      pluginReleases.some(
        (item) => !represented.includes(`${item.tag}.md|${options.repository}|${item.tag}`),
      )
    ) {
      const table = [
        "<!-- plugin-release-history -->",
        "",
        "| Plugin release | Archived notes | Published assets |",
        "| --- | --- | --- |",
        ...pluginReleases.map(
          (item) =>
            `| ${archiveTitle(item)} | [Read the release notes](${item.tag}.md) | [Plugin release](https://github.com/${options.repository}/releases/tag/${item.tag}) |`,
        ),
        "",
        "<!-- /plugin-release-history -->",
      ].join("\n");
      changes.push({
        path,
        previous,
        contents: previous.replace(pattern, table),
        reason: "plugin-index",
      });
    }
  }
  const commentary: CommentaryStatus[] = [];
  for (const component of COMPONENTS) {
    const release = published
      .filter((item) => item.component === component && item.ancestor && !item.prerelease)
      .sort((a, b) => a.distance! - b.distance! || releaseOrder(b, a))[0];
    const path = COMMENTARY[component];
    const previous = await optionalFile(join(options.root, path));
    if (previous === null) throw new Error(`Release commentary is missing: ${path}.`);
    if (neutralCommentary(previous, component)) {
      commentary.push({
        component,
        status: "empty",
        ...(release ? { tag: release.tag } : {}),
        message:
          "No additional release highlights recorded; pending changes are derived from PR history.",
      });
      continue;
    }
    if (!release) {
      commentary.push({
        component,
        status: "no-release",
        message: "No ancestral published component release; commentary is preserved.",
      });
      continue;
    }
    let shipped: string;
    try {
      // Keep exact file bytes, including final newlines; the git helper trims stdout intentionally elsewhere.
      shipped = (
        await execute("git", ["show", `${release.sourceSha}:${path}`], { cwd: options.root })
      ).stdout;
    } catch {
      throw new Error(
        `Cannot establish the released commentary for ${release.tag}; preserve source and review.`,
      );
    }
    if (previous === shipped) {
      changes.push({
        path,
        previous,
        contents: component === "desktop" ? EMPTY_DESKTOP_COMMENTARY : EMPTY_PLUGIN_COMMENTARY,
        reason: "commentary",
      });
      commentary.push({
        component,
        status: "clear",
        tag: release.tag,
        message: `Clear only highlights unchanged since ${release.tag}; newer pending PRs remain in the derived inventory.`,
      });
    } else {
      const message = `${component} highlights changed after ${release.tag}; preserved in full. Review whether they still include shipped commentary.`;
      commentary.push({ component, status: "preserved", tag: release.tag, message });
      warnings.push(message);
    }
  }
  return {
    schemaVersion: 1,
    sourceSha,
    published,
    latestStableDesktop: latest ?? null,
    changes,
    commentary,
    warnings,
  };
}

/** Refuse stale plans and changed files before the first mutation. */
export async function applyReleaseReconciliation(
  plan: ReconciliationPlan,
  root: string,
): Promise<void> {
  if ((await git(root, "rev-parse", "HEAD")) !== plan.sourceSha)
    throw new Error("Checkout changed after reconciliation planning; retry.");
  for (const change of plan.changes)
    if ((await optionalFile(join(root, change.path))) !== change.previous)
      throw new Error(`File changed after reconciliation planning: ${change.path}; retry.`);
  for (const change of plan.changes) {
    await mkdir(dirname(join(root, change.path)), { recursive: true });
    await writeFile(join(root, change.path), change.contents, "utf8");
  }
}

/** A new component release cannot conceal incomplete archival of its previous publication. */
export async function checkReleaseReconciliation(
  options: ReconciliationOptions,
  component: ReleaseComponent,
): Promise<ReconciliationPlan> {
  const plan = await planReleaseReconciliation(options);
  const missing = plan.changes.filter(
    (change) =>
      change.reason === "archive" &&
      plan.published.some(
        (release) => release.component === component && release.archivePath === change.path,
      ),
  );
  const stale = plan.commentary.find(
    (item) => item.component === component && item.status === "clear",
  );
  const pendingIndex =
    component !== "desktop" &&
    plan.published.some((release) => release.component === component) &&
    plan.changes.some((change) => change.reason === "plugin-index");
  const pendingDesktopBaseline =
    component === "desktop" && plan.changes.some((change) => change.reason === "desktop-baseline");
  if (missing.length || stale || pendingIndex || pendingDesktopBaseline)
    throw new Error(
      `Previous release finalization is incomplete for ${component}: ${missing.length} missing archives${stale ? ", shipped highlights still pending" : ""}${pendingIndex ? ", incomplete plugin release history" : ""}${pendingDesktopBaseline ? ", stale published desktop baseline" : ""}. Merge the release documentation PR before starting another release.`,
    );
  return plan;
}

async function main(): Promise<void> {
  const [mode, component, ...extra] = process.argv.slice(2);
  if (
    !["plan", "apply", "check"].includes(mode ?? "") ||
    extra.length ||
    (component !== undefined && !COMPONENTS.includes(component as ReleaseComponent)) ||
    (mode === "check" && component === undefined)
  )
    throw new Error("Usage: release-reconciliation.ts <plan|apply|check> [desktop|eda|nsp]");
  const { GITHUB_REPOSITORY: repository, GH_TOKEN: token } = process.env;
  if (!repository || !token)
    throw new Error("Set GITHUB_REPOSITORY and GH_TOKEN for release reconciliation.");
  const options = {
    root: process.cwd(),
    repository,
    readGithub: githubReader(token, process.env.GITHUB_API_URL),
  };
  const plan =
    mode === "check"
      ? await checkReleaseReconciliation(options, component as ReleaseComponent)
      : await planReleaseReconciliation(options);
  if (mode === "apply") await applyReleaseReconciliation(plan, options.root);
  if (mode === "check")
    process.stdout.write(
      `Verified archived publication and commentary for ${component}.\n${plan.warnings.map((warning) => `${warning}\n`).join("")}`,
    );
  else process.stdout.write(`${JSON.stringify(plan, null, 2)}\n`);
}
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void main().catch((error: unknown) => {
    process.stderr.write(
      `Release reconciliation failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
