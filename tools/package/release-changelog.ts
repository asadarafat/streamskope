import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { releaseIdentity, type ReleaseComponent } from "./release-version";

const execute = promisify(execFile);
const SHA = /^[a-f0-9]{40}$/u;
const SEMVER =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][\dA-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][\dA-Za-z-]*))*))?(?:\+[\dA-Za-z-]+(?:\.[\dA-Za-z-]+)*)?$/u;
const COMPONENTS: readonly ReleaseComponent[] = ["desktop", "eda", "nsp"];
const CATEGORIES = ["Breaking changes", "Features", "Fixes", "Security", "Other changes"] as const;
type Category = (typeof CATEGORIES)[number];
export type GithubRead = (path: string) => Promise<unknown>;

interface Baseline {
  tag: string;
  commit: string;
  publishedAt: string;
}
interface Change {
  kind: "pr" | "commit";
  id: number | string;
  commit: string;
  commits: string[];
  title: string;
  category: Category;
  components: readonly ReleaseComponent[];
  selectionBasis: "labels" | "paths";
  included: boolean;
  skipped: boolean;
  paths: string[];
  labels: string[];
}
export interface ReleaseChangelog {
  markdown: string;
  evidence: {
    schemaVersion: 1;
    repository: string;
    component: ReleaseComponent;
    version: string;
    tag: string;
    sourceSha: string;
    markdownSha256: string;
    baseline: Baseline | null;
    commits: string[];
    changes: Change[];
  };
}
export interface ChangelogOptions {
  root: string;
  repository: string;
  sourceSha: string;
  component: ReleaseComponent;
  version: string;
  readGithub: GithubRead;
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("GitHub returned malformed release-note evidence.");
  return value as Record<string, unknown>;
}
function string(value: unknown): string {
  if (typeof value !== "string" || !value.trim())
    throw new Error("GitHub returned missing release-note evidence.");
  return value;
}
function date(value: unknown): string {
  const text = string(value);
  if (!/^\d{4}-\d\d-\d\dT/u.test(text) || !Number.isFinite(Date.parse(text)))
    throw new Error("GitHub returned an invalid publication or merge date.");
  return text;
}
async function git(root: string, ...args: string[]): Promise<string> {
  return (await execute("git", args, { cwd: root, maxBuffer: 32 * 1024 ** 2 })).stdout.trimEnd();
}
async function pages(read: GithubRead, path: string): Promise<unknown[]> {
  const result: unknown[] = [];
  const seen = new Set<string>();
  for (let page = 1; page <= 10_000; page++) {
    const data = await read(`${path}?per_page=100&page=${page}`);
    if (!Array.isArray(data) || data.length > 100)
      throw new Error(`GitHub returned an invalid page for ${path}.`);
    const signature = JSON.stringify(data);
    if (data.length === 100 && seen.has(signature))
      throw new Error(`GitHub repeated a page for ${path}; refusing incomplete notes.`);
    seen.add(signature);
    result.push(...(data as unknown[]));
    if (data.length < 100) return result;
  }
  throw new Error(`GitHub pagination exceeded its safety limit for ${path}.`);
}

async function baseline(options: ChangelogOptions, prerelease: boolean): Promise<Baseline | null> {
  const { root, repository, component, sourceSha, readGithub } = options;
  const prefix = component === "desktop" ? "v" : `plugins/${component}/v`;
  const candidates: Array<Baseline & { distance: number }> = [];
  for (const item of await pages(readGithub, `/repos/${repository}/releases`)) {
    const release = object(item);
    const tag = string(release.tag_name);
    if (!tag.startsWith(prefix)) continue;
    const parsed = SEMVER.exec(tag.slice(prefix.length));
    if (!parsed) continue;
    if (typeof release.draft !== "boolean" || typeof release.prerelease !== "boolean")
      throw new Error(`GitHub returned incomplete release metadata for ${tag}.`);
    if (release.draft) continue;
    if (release.published_at === null) continue;
    const publishedAt = date(release.published_at);
    if (!prerelease && (release.prerelease || parsed[4])) continue;
    // A published release must have a fetched tag. target_commitish is deliberately ignored.
    const commit = await git(root, "rev-parse", "--verify", `refs/tags/${tag}^{commit}`);
    if (!SHA.test(commit)) throw new Error(`Release tag ${tag} does not resolve to a commit.`);
    try {
      await git(root, "merge-base", "--is-ancestor", commit, sourceSha);
    } catch (error) {
      if (error && typeof error === "object" && "code" in error && error.code === 1) continue;
      throw error;
    }
    const distance = Number(await git(root, "rev-list", "--count", `${commit}..${sourceSha}`));
    candidates.push({ tag, commit, publishedAt, distance });
  }
  candidates.sort(
    (a, b) =>
      a.distance - b.distance ||
      b.publishedAt.localeCompare(a.publishedAt) ||
      a.tag.localeCompare(b.tag),
  );
  const selected = candidates[0];
  return selected
    ? { tag: selected.tag, commit: selected.commit, publishedAt: selected.publishedAt }
    : null;
}

function category(title: string, labels: string[]): Category {
  const conventional = /^([a-z]+)(?:\(([^\r\n)]+)\))?(!)?:/u.exec(title);
  if (
    conventional?.[3] ||
    labels.some((label) => ["breaking-change", "type:breaking"].includes(label))
  )
    return "Breaking changes";
  if (
    conventional?.[1] === "security" ||
    conventional?.[2] === "security" ||
    labels.some((label) => ["security", "type:security"].includes(label))
  )
    return "Security";
  if (
    conventional?.[1] === "feat" ||
    labels.some((label) => ["enhancement", "type:feature"].includes(label))
  )
    return "Features";
  if (conventional?.[1] === "fix" || labels.some((label) => ["bug", "type:fix"].includes(label)))
    return "Fixes";
  return "Other changes";
}
function components(
  paths: string[],
  labels: string[],
): { components: readonly ReleaseComponent[]; selectionBasis: "labels" | "paths" } {
  const selected = labels.filter((label) => label.startsWith("component:"));
  for (const label of selected)
    if (!["component:shared", ...COMPONENTS.map((item) => `component:${item}`)].includes(label))
      throw new Error(`Unknown release component label: ${label}.`);
  if (selected.length)
    return {
      components: selected.includes("component:shared")
        ? COMPONENTS
        : COMPONENTS.filter((item) => selected.includes(`component:${item}`)),
      selectionBasis: "labels",
    };
  const inferred = new Set<ReleaseComponent>();
  for (const path of paths) {
    const plugin =
      /^plugins\/(eda|nsp)\//u.exec(path)?.[1] ??
      /^test\/(?:unit|integration|architecture|support)\/(eda|nsp)-/u.exec(path)?.[1] ??
      /^website\/docs\/plugins\/(eda|nsp)\.md$/u.exec(path)?.[1] ??
      /^website\/docs\/guide\/(eda|nsp)\//u.exec(path)?.[1] ??
      (/^vendors\/streamskope\/apps\//u.test(path) ? "eda" : undefined);
    if (plugin === "eda" || plugin === "nsp") inferred.add(plugin);
    else return { components: COMPONENTS, selectionBasis: "paths" };
  }
  return {
    components: inferred.size ? COMPONENTS.filter((item) => inferred.has(item)) : COMPONENTS,
    selectionBasis: "paths",
  };
}
function escapeMarkdown(text: string): string {
  return Array.from(text, (char) => {
    if (char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127) return " ";
    if (char === "&") return "&amp;";
    return "<>`*_[]{}()!#|\\".includes(char) ? `&#${char.charCodeAt(0)};` : char;
  }).join("");
}

/** Read exact Git ancestry and GitHub metadata; no tags, releases or repository files are changed. */
export async function generateReleaseChangelog(
  options: ChangelogOptions,
): Promise<ReleaseChangelog> {
  const { root, repository, sourceSha, component, version, readGithub } = options;
  const identity = releaseIdentity(component, version);
  if (
    !/^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\/[A-Za-z0-9_][A-Za-z0-9_.-]*$/u.test(repository) ||
    !SHA.test(sourceSha)
  )
    throw new Error("Release notes require a repository and exact 40-character source commit.");
  if ((await git(root, "rev-parse", "--is-shallow-repository")) !== "false")
    throw new Error("Release notes require complete Git history and fetched release tags.");
  if ((await git(root, "rev-parse", "--verify", `${sourceSha}^{commit}`)) !== sourceSha)
    throw new Error("Release source must identify an available commit.");
  const previous = await baseline(options, identity.prerelease);
  const history = await git(
    root,
    "rev-list",
    "--first-parent",
    "--reverse",
    previous ? `${previous.commit}..${sourceSha}` : sourceSha,
  );
  const commits = history ? history.split("\n") : [];
  const scoped = new Set(commits);
  const pulls = new Map<number, Record<string, unknown>>();
  const membership = new Map<number, Set<string>>();
  for (const commit of commits) {
    for (const value of await pages(readGithub, `/repos/${repository}/commits/${commit}/pulls`)) {
      const pull = object(value);
      if (pull.merged_at === null) continue;
      date(pull.merged_at);
      const base = object(pull.base);
      // Merged stack layers retain their intermediate base branch. Their trunk
      // identifies the destination; repository and mainline membership still apply.
      const target = pull.stack == null ? base : object(object(pull.stack).base);
      if (
        string(target.ref) !== "main" ||
        string(object(base.repo).full_name).toLowerCase() !== repository.toLowerCase()
      )
        continue;
      const merged = string(pull.merge_commit_sha);
      if (!SHA.test(merged)) throw new Error("GitHub returned an invalid PR merge commit.");
      if (!scoped.has(merged)) continue;
      if (!Number.isSafeInteger(pull.number) || (pull.number as number) <= 0)
        throw new Error("GitHub returned an invalid PR number.");
      const number = pull.number as number;
      const existing = pulls.get(number);
      if (existing && JSON.stringify(existing) !== JSON.stringify(pull))
        throw new Error(
          `PR #${number} changed while collecting notes; retry for a consistent snapshot.`,
        );
      pulls.set(number, pull);
      const members = membership.get(number) ?? new Set<string>();
      members.add(commit);
      members.add(merged);
      membership.set(number, members);
    }
  }
  const pathsByCommit = new Map<string, string[]>();
  for (const commit of commits) {
    const parent = (await git(root, "rev-list", "--parents", "-n", "1", commit)).split(" ")[1];
    const changed = parent
      ? await git(root, "diff", "--no-renames", "--name-only", "-z", parent, commit)
      : await git(
          root,
          "diff-tree",
          "--no-renames",
          "--root",
          "--no-commit-id",
          "--name-only",
          "-r",
          "-z",
          commit,
        );
    pathsByCommit.set(commit, changed.split("\0").filter(Boolean).sort());
  }
  const covered = new Set([...membership.values()].flatMap((members) => [...members]));
  const changes: Change[] = [];
  for (const commit of commits) {
    const associated = [...pulls.values()]
      .filter((pull) => pull.merge_commit_sha === commit)
      .sort((a, b) => (a.number as number) - (b.number as number));
    if (!associated.length && covered.has(commit)) continue;
    const entries = associated.length ? associated : [null];
    for (const pull of entries) {
      const title = pull
        ? string(pull.title)
        : await git(root, "show", "-s", "--format=%s", commit);
      if (pull && !Array.isArray(pull.labels))
        throw new Error("GitHub returned missing PR labels.");
      const labels = pull
        ? (pull.labels as unknown[]).map((label) => string(object(label).name)).sort()
        : [];
      const members = pull
        ? commits.filter((sha) => membership.get(pull.number as number)?.has(sha))
        : [commit];
      const paths = [...new Set(members.flatMap((sha) => pathsByCommit.get(sha) ?? []))].sort();
      const selection = components(paths, labels);
      const skipped = labels.includes("release-notes:skip");
      changes.push({
        kind: pull ? "pr" : "commit",
        id: pull ? (pull.number as number) : commit,
        commit,
        commits: members,
        title,
        category: category(title, labels),
        ...selection,
        included: !skipped && selection.components.includes(component),
        skipped,
        paths,
        labels,
      });
    }
  }
  const included = changes.filter((change) => change.included);
  const url = `https://github.com/${repository}`;
  const lines = [
    "## Changes from merged pull requests",
    "",
    previous
      ? `Since [${escapeMarkdown(previous.tag)}](${url}/releases/tag/${encodeURIComponent(previous.tag)}), through [${sourceSha.slice(0, 7)}](${url}/commit/${sourceSha}).`
      : `No eligible prior ${component} release; full reachable history through [${sourceSha.slice(0, 7)}](${url}/commit/${sourceSha}).`,
    "",
  ];
  for (const group of CATEGORIES) {
    const items = included.filter((change) => change.kind === "pr" && change.category === group);
    if (items.length)
      lines.push(
        `### ${group}`,
        "",
        ...items.map(
          (change) =>
            `- ${escapeMarkdown(change.title)} ([#${change.id}](${url}/pull/${change.id})).`,
        ),
        "",
      );
  }
  if (!included.some((change) => change.kind === "pr"))
    lines.push("No merged pull requests selected for this component.", "");
  const direct = included.filter((change) => change.kind === "commit");
  if (direct.length)
    lines.push(
      "### Direct commits",
      "",
      ...direct.map(
        (change) =>
          `- ${escapeMarkdown(change.title)} ([${change.commit.slice(0, 7)}](${url}/commit/${change.commit})).`,
      ),
      "",
    );
  lines.push(
    "Component labels select changes when present. Otherwise paths infer plugin-only changes; shared or unknown paths are included conservatively. Review this changelog and the workflow selection evidence before publishing.",
    "",
  );
  if (previous)
    lines.push(
      `[Repository-wide comparison](${url}/compare/${previous.commit}...${sourceSha}) includes all components.`,
      "",
    );
  lines.push(
    "This changelog records source changes, not test or environment qualification results.",
    "",
  );
  const markdown = lines.join("\n");
  return {
    markdown,
    evidence: {
      schemaVersion: 1,
      repository,
      component,
      version,
      tag: identity.tag,
      sourceSha,
      markdownSha256: createHash("sha256").update(markdown).digest("hex"),
      baseline: previous,
      commits,
      changes,
    },
  };
}

export function githubReader(token: string, apiUrl = "https://api.github.com"): GithubRead {
  const api = new URL(apiUrl);
  if (api.protocol !== "https:" || api.username || api.password || api.search || api.hash)
    throw new Error(
      "GitHub API URL must be an HTTPS endpoint without credentials or query parameters.",
    );
  return async (path) => {
    const response = await fetch(`${api.toString().replace(/\/$/u, "")}${path}`, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      redirect: "error",
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) throw new Error(`GitHub API returned HTTP ${response.status} for ${path}.`);
    return response.json();
  };
}

async function main(): Promise<void> {
  const [component, version, output, ...extra] = process.argv.slice(2);
  if (!component || !version || !output || extra.length)
    throw new Error("Usage: release-changelog.ts <desktop|eda|nsp> <version> <output-file>");
  const identity = releaseIdentity(component, version);
  const { GITHUB_REPOSITORY: repository, GITHUB_SHA: sourceSha, GH_TOKEN: token } = process.env;
  if (!repository || !sourceSha || !token)
    throw new Error("Set GITHUB_REPOSITORY, GITHUB_SHA and GH_TOKEN to generate release notes.");
  // Reject an existing output before spending API requests; wx also closes concurrent-write races.
  for (const file of [output, `${output}.json`]) {
    try {
      await readFile(file);
    } catch (error) {
      if (error && typeof error === "object" && "code" in error && error.code === "ENOENT")
        continue;
      throw error;
    }
    throw new Error(`Refusing to overwrite release-note evidence: ${file}.`);
  }
  const result = await generateReleaseChangelog({
    root: process.cwd(),
    repository,
    sourceSha,
    component: identity.component,
    version,
    readGithub: githubReader(token, process.env.GITHUB_API_URL),
  });
  await writeFile(`${output}.json`, `${JSON.stringify(result.evidence, null, 2)}\n`, {
    flag: "wx",
  });
  await writeFile(output, result.markdown, { flag: "wx" });
  process.stdout.write(
    `Generated ${identity.tag} changelog from ${result.evidence.changes.filter((change) => change.included).length} selected changes.\n`,
  );
}
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void main().catch((error: unknown) => {
    process.stderr.write(
      `Release changelog failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
