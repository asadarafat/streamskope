import { execFile } from "node:child_process";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";

import type { GithubRead } from "../../tools/package/release-changelog";
import {
  RELEASE_DOCS_BRANCH,
  RELEASE_DOCS_MARKER,
  type GithubWrite,
  type ReleaseDocumentationOptions,
} from "../../tools/package/release-documentation";
import { EMPTY_PLUGIN_COMMENTARY } from "../../tools/package/release-reconciliation";

const execute = promisify(execFile);
export const repository = "owner/project";
const sha = "a".repeat(40);
const title = "docs(release): synchronize published release records";

export function ownedPull(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    number: 12,
    node_id: "PR_test",
    title,
    body: RELEASE_DOCS_MARKER,
    state: "open",
    auto_merge: null,
    merge_commit_sha: "b".repeat(40),
    base: { ref: "main", sha, repo: { full_name: repository } },
    head: { ref: RELEASE_DOCS_BRANCH, sha, repo: { full_name: repository } },
    labels: [{ name: "release-notes:skip" }, { name: "component:shared" }],
    changed_files: 1,
    ...overrides,
  };
}

export interface ReleaseDocumentationFixture extends ReleaseDocumentationOptions {
  options: ReleaseDocumentationOptions;
  git: (...args: string[]) => Promise<string>;
  write: (path: string, contents: string) => Promise<void>;
  remote: string;
  directory: string;
  writes: Array<{ method: string; path: string; body: Record<string, unknown> }>;
  runs: Array<Record<string, unknown>>;
  published: string;
  getPull: () => Record<string, unknown> | null;
}
export async function createReleaseDocumentationFixture(
  register: (directory: string) => void,
  changedHighlights = false,
): Promise<ReleaseDocumentationFixture> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-release-docs-"));
  register(directory);
  const root = join(directory, "source");
  const remote = join(directory, "remote.git");
  await mkdir(root);
  const git = async (...args: string[]): Promise<string> =>
    (await execute("git", args, { cwd: root })).stdout.trimEnd();
  const write = async (path: string, contents: string): Promise<void> => {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), contents);
  };
  await git("init", "-b", "main");
  await git("config", "user.name", "Test maintainer");
  await git("config", "user.email", "maintainer@example.invalid");
  await execute("git", ["init", "--bare", remote]);
  await git("remote", "add", "origin", remote);
  await write("website/zensical.toml", 'desktop_release = "v0.0.0"\n');
  await write(
    "website/docs/releases/index.md",
    "# Releases\n\n<!-- plugin-release-history -->\n<!-- /plugin-release-history -->\n",
  );
  const commentary =
    "---\ntitle: Unreleased changes\nunreleased: true\n---\n\n# Unreleased changes\n\nShipped highlight.\n";
  await write("website/docs/releases/unreleased.md", commentary);
  for (const component of ["eda", "nsp"])
    await write(`plugins/${component}/RELEASE_NOTES.md`, EMPTY_PLUGIN_COMMENTARY);
  await git("add", ".");
  await git("commit", "-m", "feat: published behavior");
  const published = await git("rev-parse", "HEAD");
  await git("tag", "v0.1.0");
  if (changedHighlights) {
    await write("website/docs/releases/unreleased.md", `${commentary}\nNewer highlight.\n`);
    await git("add", ".");
    await git("commit", "-m", "feat: newer unpublished work");
  }
  await git("push", "origin", "main", "--tags");
  const release = {
    id: 1,
    tag_name: "v0.1.0",
    body: "# StreamSkope v0.1.0\n\nPublished behavior.\n",
    draft: false,
    prerelease: false,
    immutable: true,
    published_at: "2026-10-07T00:00:00Z",
  };
  const writes: Array<{ method: string; path: string; body: Record<string, unknown> }> = [];
  let pull: Record<string, unknown> | null = null;
  const runs: Array<Record<string, unknown>> = [];
  const currentPull = async (): Promise<Record<string, unknown>> => {
    const head = (
      await execute("git", ["rev-parse", `refs/heads/${RELEASE_DOCS_BRANCH}`], { cwd: remote })
    ).stdout.trim();
    const base = await git("rev-parse", "main");
    const files = (await git("diff", "--no-renames", "--name-only", `${base}...${head}`))
      .split("\n")
      .filter(Boolean);
    return {
      ...pull,
      head: { ref: RELEASE_DOCS_BRANCH, sha: head, repo: { full_name: repository } },
      base: { ref: "main", sha: base, repo: { full_name: repository } },
      changed_files: files.length,
    };
  };
  const readGithub: GithubRead = async (path) => {
    if (path.includes("/releases?")) return [release];
    if (path.includes("/commits/")) return { sha: published };
    if (/\/actions\/runs\/\d+$/u.test(path))
      return runs.find((run) => String(run.id) === path.split("/").at(-1));
    if (path.includes("/actions/")) return { workflow_runs: runs };
    if (path.includes("/files?")) {
      const current = await currentPull();
      return (
        await git(
          "diff",
          "--no-renames",
          "--name-only",
          `${String((current.base as Record<string, unknown>).sha)}...${String((current.head as Record<string, unknown>).sha)}`,
        )
      )
        .split("\n")
        .filter(Boolean)
        .map((filename) => ({ filename }));
    }
    if (path.includes("/pulls?")) return pull && pull.state === "open" ? [await currentPull()] : [];
    if (/\/pulls\/12$/u.test(path)) return currentPull();
    throw new Error(`Unexpected mock read ${path}`);
  };
  const writeGithub: GithubWrite = async (method, path, value) => {
    const body = value as Record<string, unknown>;
    writes.push({ method, path, body });
    if (method === "PUT" && path.endsWith("/merge")) {
      const current = await currentPull();
      if (body.sha !== (current.head as Record<string, unknown>).sha)
        throw new Error("Qualified head changed before merge.");
      pull = { ...pull, state: "closed", merged: true, merge_commit_sha: "d".repeat(40) };
      return { merged: true, sha: "d".repeat(40) };
    }
    if (path === "/graphql") {
      const enabled = String(body.query).includes("enablePullRequestAutoMerge");
      pull = { ...pull, auto_merge: enabled ? { enabled: true } : null };
      return {
        data: {
          [enabled ? "enablePullRequestAutoMerge" : "disablePullRequestAutoMerge"]: {
            pullRequest: { number: 12 },
          },
        },
      };
    }
    if (path.endsWith("/labels")) {
      pull = { ...pull, labels: (body.labels as string[]).map((name) => ({ name })) };
      return [];
    }
    if (path.includes("/pulls")) {
      pull = { ...(pull ?? ownedPull({ labels: [] })), ...body };
      if (body.state !== "closed") {
        const current = await currentPull();
        const head = current.head as Record<string, unknown>;
        if (!runs.some((item) => item.head_sha === head.sha))
          runs.push({
            id: runs.length + 100,
            workflow_id: 1,
            head_sha: head.sha,
            head_branch: RELEASE_DOCS_BRANCH,
            event: "pull_request",
            pull_requests: [{ number: 12, head, base: { ref: "main" } }],
            status: "queued",
            conclusion: null,
          });
      }
      return { number: 12 };
    }
    throw new Error(`Unexpected mock write ${path}`);
  };
  const options = {
    root,
    repository,
    readGithub,
    writeGithub,
    updateQualification: (): Promise<void> => Promise.resolve(),
    pause: (): Promise<void> => Promise.resolve(),
    automaticCi: true,
  };
  return {
    ...options,
    options,
    git,
    write,
    root,
    remote,
    directory,
    writes,
    runs,
    published,
    getPull: (): Record<string, unknown> | null => pull,
  };
}
