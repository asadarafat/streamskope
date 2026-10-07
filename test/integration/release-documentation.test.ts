import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";

import { check, resolveConfig } from "prettier";
import { afterEach, expect, it } from "vitest";

import type { GithubRead } from "../../tools/package/release-changelog";
import {
  allowedReleaseDocumentationPath,
  githubWriter,
  reconcileReleaseDocumentation,
  RELEASE_DOCS_BRANCH,
  RELEASE_DOCS_MARKER,
  validateReleaseDocumentationPullRequest,
  type GithubWrite,
  type ReleaseDocumentationOptions,
} from "../../tools/package/release-documentation";
import { EMPTY_PLUGIN_COMMENTARY } from "../../tools/package/release-reconciliation";

const execute = promisify(execFile);
const directories: string[] = [];
const repository = "owner/project";
const sha = "a".repeat(40);
const title = "docs(release): synchronize published release records";
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});
function ownedPull(overrides: Record<string, unknown> = {}): Record<string, unknown> {
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
function validate(readGithub: GithubRead, overrides = {}): Promise<void> {
  return validateReleaseDocumentationPullRequest({
    repository,
    sourceSha: sha,
    pullRequest: 12,
    readGithub,
    ...overrides,
  });
}
function validationApi(
  pull = ownedPull(),
  files: unknown[] = [{ filename: "website/docs/releases/v0.1.0.md" }],
): GithubRead {
  return (path) => Promise.resolve(path.includes("/files?") ? files : pull);
}

it("validates only an owned exact-head main-target documentation PR", async () => {
  await expect(validate(validationApi())).resolves.toBeUndefined();
});
it.each([
  { name: "invalid source", options: { sourceSha: "main" } },
  { name: "invalid repository", options: { repository: "owner/repo?bad" } },
  { name: "missing PR", options: { pullRequest: 0 } },
])("rejects validation inputs: $name", async ({ options }) => {
  const read = (): Promise<unknown> => {
    throw new Error("API must not be called");
  };
  await expect(validate(read, options)).rejects.toThrow("exact SHA");
});
it.each([
  { name: "closed PR", pull: { state: "closed" } },
  { name: "non-owned body", pull: { body: "manual PR" } },
  { name: "missing skip label", pull: { labels: [{ name: "component:shared" }] } },
  { name: "foreign title", pull: { title: "feat: change behavior" } },
  {
    name: "fork head",
    pull: { head: { ref: RELEASE_DOCS_BRANCH, sha, repo: { full_name: "foreign/project" } } },
  },
  {
    name: "different head SHA",
    pull: {
      head: { ref: RELEASE_DOCS_BRANCH, sha: "b".repeat(40), repo: { full_name: repository } },
    },
  },
  { name: "different base", pull: { base: { ref: "other", repo: { full_name: repository } } } },
  { name: "different PR number", pull: { number: 99 } },
])("rejects changed or unowned PR metadata: $name", async ({ pull }) => {
  await expect(validate(validationApi(ownedPull(pull)))).rejects.toThrow();
});
it.each(
  [
    [{ filename: "src/backend.ts" }],
    [{ filename: "website/docs/releases/../v0.1.0.md" }],
    [{ filename: "website/docs/releases/v0.1.0.md", previous_filename: "package.json" }],
    [
      { filename: "website/docs/releases/v0.1.0.md" },
      { filename: "website/docs/releases/v0.1.0.md" },
    ],
    [{}],
    [],
  ].map((files) => ({ files })),
)("rejects unexpected or incomplete changed paths %#", async ({ files }) => {
  await expect(validate(validationApi(ownedPull(), files))).rejects.toThrow();
});
it("reads every changed-file page and detects incomplete or repeating pages", async () => {
  const files = Array.from({ length: 100 }, (_, index) => ({
    filename: `website/docs/releases/v1.0.${index}.md`,
  }));
  let calls = 0;
  await expect(
    validate((path) => {
      if (!path.includes("/files?")) return Promise.resolve(ownedPull({ changed_files: 101 }));
      calls++;
      return Promise.resolve(
        path.endsWith("page=1") ? files : [{ filename: "plugins/eda/RELEASE_NOTES.md" }],
      );
    }),
  ).resolves.toBeUndefined();
  expect(calls).toBe(2);
  await expect(
    validate((path) =>
      Promise.resolve(path.includes("/files?") ? files : ownedPull({ changed_files: 201 })),
    ),
  ).rejects.toThrow("repeated");
  await expect(validate(validationApi(ownedPull({ changed_files: 2 })))).rejects.toThrow(
    "incomplete",
  );
});
it("accepts historical component archives and rejects unrelated paths", () => {
  expect(allowedReleaseDocumentationPath("website/docs/releases/v0.1.0+build.1.md")).toBe(true);
  expect(allowedReleaseDocumentationPath("website/docs/releases/plugins/nsp/v0.2.0-rc.1.md")).toBe(
    true,
  );
  expect(allowedReleaseDocumentationPath("website/docs/releases/plugins/unknown/v0.1.0.md")).toBe(
    false,
  );
  expect(allowedReleaseDocumentationPath("website/docs/releases/v01.0.0.md")).toBe(false);
  expect(allowedReleaseDocumentationPath(".github/workflows/ci.yml")).toBe(false);
});
it("validates secure endpoints before sending write credentials", () => {
  expect(() => githubWriter("private", "http://api.github.com")).toThrow("HTTPS");
  expect(() => githubWriter("private", "https://user:password@api.github.com")).toThrow("HTTPS");
});

interface Fixture extends ReleaseDocumentationOptions {
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
async function fixture(changedHighlights = false): Promise<Fixture> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-release-docs-"));
  directories.push(directory);
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
    const files = (await git("diff", "--no-renames", "--name-only", base, head))
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
    if (path.includes("/actions/")) return { workflow_runs: runs };
    if (path.includes("/files?")) {
      const current = await currentPull();
      return (
        await git(
          "diff",
          "--no-renames",
          "--name-only",
          String((current.base as Record<string, unknown>).sha),
          String((current.head as Record<string, unknown>).sha),
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
      pull = { ...pull, state: "closed", merged: true };
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
    if (path.endsWith("/rerun")) {
      const id = Number(/\/runs\/(\d+)\//u.exec(path)?.[1]);
      const run = runs.find((item) => item.id === id)!;
      run.status = "queued";
      run.conclusion = null;
      return null;
    }
    if (path.includes("/pulls")) {
      pull = { ...(pull ?? ownedPull({ labels: [] })), ...body };
      if (body.state !== "closed") {
        const current = await currentPull();
        const head = current.head as Record<string, unknown>;
        if (!runs.some((item) => item.head_sha === head.sha))
          runs.push({
            id: runs.length + 100,
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

it("creates one owned documentation PR with normal App-triggered CI and reuses it without duplicate runs", async () => {
  const repo = await fixture();
  const first = await reconcileReleaseDocumentation(repo.options);
  expect(first).toMatchObject({
    status: "pull-request",
    pullRequest: 12,
    ciAction: "existing",
    autoMergeEnabled: true,
    warnings: [],
  });
  expect(await readFile(join(repo.root, "website/docs/releases/v0.1.0.md"), "utf8")).toContain(
    "Published behavior.",
  );
  await repo.git("checkout", "main");
  const repeated = await reconcileReleaseDocumentation(repo.options);
  expect(repeated.headSha).toBe(first.headSha);
  expect(repeated.ciAction).toBe("existing");
  expect(repo.writes.filter((item) => item.path === `/repos/${repository}/pulls`)).toHaveLength(1);
  expect(repo.runs).toHaveLength(1);
  repo.runs[0]!.status = "completed";
  repo.runs[0]!.conclusion = "success";
  await repo.git("checkout", "main");
  const qualified = await reconcileReleaseDocumentation(repo.options);
  expect(qualified.ciAction).toBe("existing");
  expect(repo.writes.filter((item) => item.path === "/graphql")).toHaveLength(1);
  repo.runs[0]!.status = "completed";
  repo.runs[0]!.conclusion = "failure";
  await repo.git("checkout", "main");
  const retry = await reconcileReleaseDocumentation(repo.options);
  expect(retry.ciAction).toBe("rerun");
  expect(repo.writes.find((item) => item.path.endsWith("/rerun"))?.path).toBe(
    `/repos/${repository}/actions/runs/100/rerun`,
  );
  expect(retry.headSha).toBe(first.headSha);
});
it("preserves newer commentary and requires review instead of enabling auto-merge", async () => {
  const repo = await fixture(true);
  const result = await reconcileReleaseDocumentation(repo.options);
  expect(result.ciAction).toBe("existing");
  expect(result.autoMergeEnabled).toBe(false);
  expect(result.warnings).toHaveLength(1);
  expect(await readFile(join(repo.root, "website/docs/releases/unreleased.md"), "utf8")).toContain(
    "Newer highlight.",
  );
  expect(repo.writes.some((item) => item.path === "/graphql")).toBe(false);
});
it("formats generated editable pages for CI while retaining published body bytes", async () => {
  const repo = await fixture();
  const body = "# StreamSkope v0.1.0\n\n| a | longer heading |\n|---|---|\n| x | y |\n";
  const readGithub: GithubRead = async (path) => {
    const result = await repo.readGithub(path);
    return path.includes("/releases?")
      ? (result as Record<string, unknown>[]).map((release) => ({ ...release, body }))
      : result;
  };
  await reconcileReleaseDocumentation({
    ...repo.options,
    readGithub,
    updateQualification: async () => {
      const index = "website/docs/releases/index.md";
      await repo.write(index, `${await readFile(join(repo.root, index), "utf8")}\n${body}`);
      await repo.write(
        "website/docs/guide/qualification.md",
        "# Qualification\n<!-- publication-qualification -->\n## Published release: v0.1.0\n\nRecorded limitations.\n<!-- /publication-qualification -->\n",
      );
    },
  });
  for (const relative of [
    "website/docs/releases/unreleased.md",
    "website/docs/releases/index.md",
    "website/docs/guide/qualification.md",
  ]) {
    const file = join(repo.root, relative);
    expect(
      await check(await readFile(file, "utf8"), { ...(await resolveConfig(file)), filepath: file }),
    ).toBe(true);
  }
  const archived = await readFile(join(repo.root, "website/docs/releases/v0.1.0.md"), "utf8");
  expect(archived.endsWith(body)).toBe(true);
  expect(await check(body, { parser: "markdown" })).toBe(false);
});
it("merges an already qualified fallback PR through the protected endpoint with its exact head", async () => {
  const repo = await fixture();
  const first = await reconcileReleaseDocumentation({
    ...repo.options,
    automaticCi: false,
    readGithub: async (path) => {
      const data = await repo.readGithub(path);
      if (!path.includes("/actions/")) return data;
      return {
        workflow_runs: (data as { workflow_runs: Record<string, unknown>[] }).workflow_runs.map(
          (run) => ({ ...run, status: "completed", conclusion: "action_required" }),
        ),
      };
    },
  });
  expect(first.ciAction).toBe("approval-required");
  // Model maintainer approval and completed normal PR CI before retrying.
  repo.getPull()!.mergeable_state = "clean";
  repo.runs[0]!.status = "completed";
  repo.runs[0]!.conclusion = "success";
  repo.writes.splice(0);
  await repo.git("checkout", "main");
  const result = await reconcileReleaseDocumentation({ ...repo.options, automaticCi: false });
  expect(result.headSha).toBe(first.headSha);
  expect(result.ciAction).toBe("existing");
  expect(repo.writes.find((item) => item.method === "PUT")).toEqual({
    method: "PUT",
    path: `/repos/${repository}/pulls/12/merge`,
    body: {
      sha: first.headSha,
      merge_method: "squash",
      commit_title: "docs(release): synchronize published release records",
    },
  });
  expect(repo.writes.some((item) => item.path === "/graphql")).toBe(false);
});
it("leaves a qualified PR unmerged when GitHub rejects the protected merge", async () => {
  const repo = await fixture();
  await reconcileReleaseDocumentation(repo.options);
  repo.getPull()!.auto_merge = null;
  repo.getPull()!.mergeable_state = "clean";
  repo.runs[0]!.status = "completed";
  repo.runs[0]!.conclusion = "success";
  await repo.git("checkout", "main");
  await expect(
    reconcileReleaseDocumentation({
      ...repo.options,
      writeGithub: async (method, path, body) => {
        if (method === "PUT" && path.endsWith("/merge"))
          throw new Error("Branch protection requires an up-to-date head.");
        return repo.writeGithub(method, path, body);
      },
    }),
  ).rejects.toThrow("Branch protection requires an up-to-date head");
  expect(repo.getPull()!.state).toBe("open");
});
it("does not merge a clean PR when preserved highlights still require review", async () => {
  const repo = await fixture(true);
  await reconcileReleaseDocumentation(repo.options);
  repo.getPull()!.mergeable_state = "clean";
  repo.runs[0]!.status = "completed";
  repo.runs[0]!.conclusion = "success";
  await repo.git("checkout", "main");
  const result = await reconcileReleaseDocumentation(repo.options);
  expect(result.autoMergeEnabled).toBe(false);
  expect(repo.writes.some((item) => item.method === "PUT")).toBe(false);
});
it("performs no PR or CI writes when main already matches immutable publication records", async () => {
  const repo = await fixture();
  await reconcileReleaseDocumentation(repo.options);
  await repo.git("checkout", "main");
  await repo.git("merge", "--ff-only", RELEASE_DOCS_BRANCH);
  repo.writes.splice(0);
  const result = await reconcileReleaseDocumentation(repo.options);
  expect(result.status).toBe("unchanged");
  expect(
    repo.writes.some(
      (item) =>
        item.path.endsWith("/approve") ||
        item.path.endsWith("/rerun") ||
        (item.path.endsWith("/pulls") && item.method === "POST"),
    ),
  ).toBe(false);
});
it("rejects a managed branch edited manually before overwriting any remote history", async () => {
  const repo = await fixture();
  await reconcileReleaseDocumentation(repo.options);
  await repo.write("website/docs/releases/v0.1.0.md", "Manual archive edit.\n");
  await repo.git("add", ".");
  await repo.git("commit", "-m", "docs: manual branch edit");
  await repo.git("push", "origin", RELEASE_DOCS_BRANCH);
  const head = await repo.git("rev-parse", "HEAD");
  await repo.git("checkout", "main");
  await expect(reconcileReleaseDocumentation(repo.options)).rejects.toThrow(
    "edited outside automation",
  );
  expect(
    (await repo.git("ls-remote", "origin", `refs/heads/${RELEASE_DOCS_BRANCH}`)).startsWith(head),
  ).toBe(true);
});
it("requires the triggering immutable publication to appear before declaring finalization", async () => {
  const repo = await fixture();
  await expect(
    reconcileReleaseDocumentation({
      ...repo.options,
      expectedPublication: { tag: "v0.2.0", id: 2 },
    }),
  ).rejects.toThrow("not visible");
  expect(repo.writes).toEqual([]);
  expect(await repo.git("status", "--porcelain")).toBe("");
});

it("uses only the associated owned PR run even when CI records a verified merge SHA", async () => {
  const repo = await fixture();
  const readGithub: GithubRead = async (path) => {
    const data = await repo.readGithub(path);
    if (!path.includes("/actions/")) return data;
    const runs = (data as { workflow_runs: Record<string, unknown>[] }).workflow_runs;
    return {
      workflow_runs: [
        {
          ...runs[0],
          id: 999,
          pull_requests: [
            {
              number: 99,
              head: { sha: "b".repeat(40), ref: RELEASE_DOCS_BRANCH },
              base: { ref: "main" },
            },
          ],
        },
        ...runs.map((run) => ({ ...run, head_sha: "b".repeat(40) })),
      ],
    };
  };
  const result = await reconcileReleaseDocumentation({ ...repo.options, readGithub });
  expect(result.ciAction).toBe("existing");
  expect(
    repo.writes.filter((item) => item.path.endsWith("/approve")).map((item) => item.path),
  ).toEqual([]);
});

it("leaves finalization pending for maintainer approval when only the built-in token is available", async () => {
  const repo = await fixture();
  const result = await reconcileReleaseDocumentation({
    ...repo.options,
    automaticCi: false,
    readGithub: async (path) => {
      const data = await repo.readGithub(path);
      if (!path.includes("/actions/")) return data;
      return {
        workflow_runs: (data as { workflow_runs: Record<string, unknown>[] }).workflow_runs.map(
          (run) => ({ ...run, status: "completed", conclusion: "action_required" }),
        ),
      };
    },
  });
  expect(result.ciAction).toBe("approval-required");
  expect(result.autoMergeEnabled).toBe(false);
  expect(repo.getPull()?.state).toBe("open");
  expect(repo.writes.some((item) => item.path === "/graphql")).toBe(false);
  expect(repo.writes.some((item) => item.path.endsWith("/approve"))).toBe(false);
});

it("never approves an unrelated normal CI run when the owned head has no run", async () => {
  const repo = await fixture();
  let polls = 0;
  await expect(
    reconcileReleaseDocumentation({
      ...repo.options,
      readGithub: async (path) => {
        if (!path.includes("/actions/")) return repo.readGithub(path);
        polls++;
        return {
          workflow_runs: [
            {
              id: 999,
              event: "pull_request",
              head_branch: RELEASE_DOCS_BRANCH,
              head_sha: "b".repeat(40),
              status: "completed",
              conclusion: "action_required",
              pull_requests: [
                {
                  number: 99,
                  head: { sha: "b".repeat(40), ref: RELEASE_DOCS_BRANCH },
                  base: { ref: "main" },
                },
              ],
            },
          ],
        };
      },
    }),
  ).rejects.toThrow("has not appeared");
  expect(polls).toBe(45);
  expect(repo.writes.some((item) => item.path.endsWith("/approve"))).toBe(false);
});

it("rejects an older CI source even if its PR association now describes the current head", async () => {
  const repo = await fixture();
  await expect(
    reconcileReleaseDocumentation({
      ...repo.options,
      readGithub: async (path) => {
        const data = await repo.readGithub(path);
        if (!path.includes("/actions/")) return data;
        return {
          workflow_runs: (data as { workflow_runs: Record<string, unknown>[] }).workflow_runs.map(
            (run) => ({ ...run, head_sha: "c".repeat(40) }),
          ),
        };
      },
    }),
  ).rejects.toThrow("has not appeared");
  expect(repo.writes.some((item) => item.path.endsWith("/approve"))).toBe(false);
});

it("fails closed on malformed normal CI status rather than treating it as active", async () => {
  const repo = await fixture();
  await expect(
    reconcileReleaseDocumentation({
      ...repo.options,
      readGithub: async (path) => {
        const data = await repo.readGithub(path);
        if (!path.includes("/actions/")) return data;
        return {
          workflow_runs: (data as { workflow_runs: Record<string, unknown>[] }).workflow_runs.map(
            (run) => ({ ...run, status: "unknown" }),
          ),
        };
      },
    }),
  ).rejects.toThrow("invalid status");
  expect(repo.writes.some((item) => item.path.endsWith("/approve"))).toBe(false);
});

it("disables previously enabled auto-merge when newer highlights require review", async () => {
  const repo = await fixture();
  const first = await reconcileReleaseDocumentation(repo.options);
  await repo.git("checkout", "main");
  const path = "website/docs/releases/unreleased.md";
  await repo.write(path, `${await readFile(join(repo.root, path), "utf8")}\nNewer highlight.\n`);
  await repo.git("add", "--", path);
  await repo.git("commit", "-m", "feat: newer work");
  const next = await reconcileReleaseDocumentation(repo.options);
  expect(next.headSha).not.toBe(first.headSha);
  expect(next.autoMergeEnabled).toBe(false);
  expect(
    repo.writes.some(
      (item) =>
        item.path === "/graphql" && String(item.body.query).includes("disablePullRequestAutoMerge"),
    ),
  ).toBe(true);
});

it("force-with-lease preserves a managed remote branch changed after the ownership read", async () => {
  const repo = await fixture();
  const first = await reconcileReleaseDocumentation(repo.options);
  await repo.git("checkout", "main");
  await repo.write("README.md", "New main source.\n");
  await repo.git("add", "--", "README.md");
  await repo.git("commit", "-m", "docs: advance main");
  let concurrentHead = "";
  await expect(
    reconcileReleaseDocumentation({
      ...repo.options,
      updateQualification: async () => {
        const tree = await repo.git("rev-parse", `${first.headSha}^{tree}`);
        concurrentHead = (
          await execute(
            "git",
            [
              "-c",
              "user.name=Other maintainer",
              "-c",
              "user.email=other@example.invalid",
              "commit-tree",
              tree,
              "-p",
              first.headSha!,
              "-m",
              "docs: concurrent change",
            ],
            { cwd: repo.remote },
          )
        ).stdout.trim();
        await execute(
          "git",
          ["update-ref", `refs/heads/${RELEASE_DOCS_BRANCH}`, concurrentHead, first.headSha!],
          { cwd: repo.remote },
        );
      },
    }),
  ).rejects.toThrow(/rejected|stale info/u);
  expect(
    (await repo.git("ls-remote", "origin", `refs/heads/${RELEASE_DOCS_BRANCH}`)).startsWith(
      concurrentHead,
    ),
  ).toBe(true);
});
