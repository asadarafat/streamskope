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
  finalizeReleaseDocumentation,
  reconcileReleaseDocumentation,
  RELEASE_DOCS_BRANCH,
  validateReleaseDocumentationPullRequest,
  writeReleaseDocumentationFinalization,
} from "../../tools/package/release-documentation";
import {
  createReleaseDocumentationFixture,
  ownedPull,
  repository,
  type ReleaseDocumentationFixture,
} from "../support/release-documentation-fixture";

const execute = promisify(execFile);
const directories: string[] = [];
const sha = "a".repeat(40);
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});

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

const fixture = (changed = false): Promise<ReleaseDocumentationFixture> =>
  createReleaseDocumentationFixture((directory) => {
    directories.push(directory);
  }, changed);

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
  await expect(reconcileReleaseDocumentation(repo.options)).rejects.toThrow("ended with failure");
  expect(repo.writes.some((item) => item.path.endsWith("/rerun"))).toBe(false);
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
it("hands a qualified fallback PR to its maintainer without requesting a merge", async () => {
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
  expect(result.autoMergeEnabled).toBe(false);
  expect(repo.writes.some((item) => item.method === "PUT")).toBe(false);
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

it.each([true, false])(
  "keeps terminal CI outcomes as errors with automaticCi=%s",
  async (automaticCi) => {
    for (const conclusion of [
      "failure",
      "cancelled",
      "timed_out",
      "skipped",
      "neutral",
      "stale",
      "startup_failure",
    ]) {
      const repo = await fixture();
      await expect(
        finalizeReleaseDocumentation({
          ...repo.options,
          automaticCi,
          readGithub: async (path) => {
            const value = await repo.readGithub(path);
            return path.includes("/actions/")
              ? {
                  workflow_runs: (
                    value as { workflow_runs: Record<string, unknown>[] }
                  ).workflow_runs.map((run) => ({ ...run, status: "completed", conclusion })),
                }
              : value;
          },
        }),
      ).rejects.toThrow(`ended with ${conclusion}`);
      expect(
        repo.writes.some(
          (write) =>
            write.path.includes("/actions/") || write.path === "/graphql" || write.method === "PUT",
        ),
      ).toBe(false);
    }
  },
);

it.each(["queued", "success"])(
  "hands manual %s CI to the maintainer without polling or merge requests",
  async (status) => {
    const repo = await fixture();
    const result = await finalizeReleaseDocumentation({
      ...repo.options,
      automaticCi: false,
      pause: () => {
        throw new Error("Manual handoff must not poll");
      },
      readGithub: async (path) => {
        const value = await repo.readGithub(path);
        return path.includes("/actions/") && status === "success"
          ? {
              workflow_runs: (
                value as { workflow_runs: Record<string, unknown>[] }
              ).workflow_runs.map((run) => ({
                ...run,
                status: "completed",
                conclusion: "success",
              })),
            }
          : value;
      },
    });
    expect(result).toMatchObject({ finalization: "awaiting-maintainer", autoMergeEnabled: false });
    expect(repo.writes.some((write) => write.path === "/graphql" || write.method === "PUT")).toBe(
      false,
    );
  },
);

it("waits through CI and observes the exact merge, allowing a custom App author", async () => {
  const repo = await fixture();
  let pauses = 0;
  const result = await finalizeReleaseDocumentation({
    ...repo.options,
    readGithub: async (path) => {
      const value = await repo.readGithub(path);
      return /\/pulls\/12$/u.test(path)
        ? { ...(value as object), user: { login: "streamskope-release[bot]", type: "Bot" } }
        : value;
    },
    pause: () => {
      pauses++;
      Object.assign(repo.runs[0]!, { status: "completed", conclusion: "success" });
      Object.assign(repo.getPull()!, {
        state: "closed",
        merged: true,
        merge_commit_sha: "d".repeat(40),
      });
      return Promise.resolve();
    },
  });
  expect(pauses).toBe(1);
  expect(result).toMatchObject({
    finalization: "merged",
    mergeSha: "d".repeat(40),
    ciAction: "existing",
  });
});

it.each(["closed", "head", "files", "ci", "api"])(
  "fails after a pending observation when %s evidence changes",
  async (change) => {
    const repo = await fixture();
    let changed = false;
    await expect(
      finalizeReleaseDocumentation({
        ...repo.options,
        pause: () => {
          changed = true;
          return Promise.resolve();
        },
        readGithub: async (path) => {
          const value = await repo.readGithub(path);
          if (!changed) return value;
          if (change === "api") throw new Error("API unavailable");
          if (change === "files" && path.includes("/files?"))
            return [{ filename: "src/unowned.ts" }];
          if (change === "ci" && path.includes("/actions/"))
            return {
              workflow_runs: repo.runs.map((run) => ({
                ...run,
                status: "completed",
                conclusion: "failure",
              })),
            };
          if (/\/pulls\/12$/u.test(path)) {
            if (change === "closed")
              return { ...(value as object), state: "closed", merged: false };
            if (change === "head")
              return {
                ...(value as object),
                head: {
                  ref: RELEASE_DOCS_BRANCH,
                  sha: "c".repeat(40),
                  repo: { full_name: repository },
                },
              };
          }
          return value;
        },
      }),
    ).rejects.toThrow();
    expect(repo.writes.some((write) => write.path.includes("/rerun"))).toBe(false);
  },
);

it("requires successful CI even when a merge is reported", async () => {
  const repo = await fixture();
  await expect(
    finalizeReleaseDocumentation({
      ...repo.options,
      pause: () => {
        Object.assign(repo.getPull()!, {
          state: "closed",
          merged: true,
          merge_commit_sha: "d".repeat(40),
        });
        return Promise.resolve();
      },
    }),
  ).rejects.toThrow("no successful exact-head CI");
});

it.each([false, true])(
  "bounds pending finalization with a %s advancing clock",
  async (advancing) => {
    const repo = await fixture();
    let time = 0;
    let pauses = 0;
    await expect(
      finalizeReleaseDocumentation({
        ...repo.options,
        now: () => time,
        pause: () => {
          pauses++;
          if (advancing) time += 25 * 60_000;
          return Promise.resolve();
        },
      }),
    ).rejects.toThrow("bounded finalization");
    expect(pauses).toBe(advancing ? 1 : 149);
  },
);

it.each(["--assume-unchanged", "--skip-worktree"])(
  "refuses hidden worktree flags %s",
  async (flag) => {
    const repo = await fixture();
    await repo.git("update-index", flag, "website/zensical.toml");
    await expect(finalizeReleaseDocumentation(repo.options)).rejects.toThrow("clean main");
    expect(repo.writes).toEqual([]);
  },
);

it.each(["README.md", "package-lock.json", "tools/package/release-documentation.ts"])(
  "refreshes only when loaded inputs are unchanged: %s",
  async (path) => {
    const repo = await fixture();
    let paused = false;
    const result = finalizeReleaseDocumentation({
      ...repo.options,
      pause: async () => {
        if (paused) throw new Error("Refreshed approval must return immediately");
        paused = true;
        const other = join(repo.directory, "other");
        await execute("git", ["clone", "--branch", "main", repo.remote, other]);
        await mkdir(dirname(join(other, path)), { recursive: true });
        await writeFile(join(other, path), "New main content.\n");
        await execute("git", ["add", "--", path], { cwd: other });
        await execute(
          "git",
          [
            "-c",
            "user.name=Other maintainer",
            "-c",
            "user.email=other@example.invalid",
            "commit",
            "-m",
            "docs: advance main",
          ],
          { cwd: other },
        );
        await execute("git", ["push", "origin", "main"], { cwd: other });
        repo.getPull()!.mergeable_state = "behind";
      },
      writeGithub: async (method, endpoint, value) => {
        const output = await repo.writeGithub(method, endpoint, value);
        if (paused && method === "PATCH" && endpoint.endsWith("/pulls/12")) {
          repo.getPull()!.mergeable_state = "blocked";
          Object.assign(repo.runs.at(-1)!, { status: "completed", conclusion: "action_required" });
        }
        return output;
      },
    });
    if (path === "README.md") {
      expect(await result).toMatchObject({
        finalization: "approval-required",
        autoMergeEnabled: false,
      });
      expect(repo.runs).toHaveLength(2);
      expect(repo.getPull()!.auto_merge).toBeNull();
    } else {
      await expect(result).rejects.toThrow("toolchain or dependency inputs changed");
      expect(repo.runs).toHaveLength(1);
      expect(await repo.git("branch", "--show-current")).toBe(RELEASE_DOCS_BRANCH);
    }
  },
);

it("refuses a dirty owned checkout before a behind refresh", async () => {
  const repo = await fixture();
  await expect(
    finalizeReleaseDocumentation({
      ...repo.options,
      pause: async () => {
        await repo.write("README.md", "Uncommitted user content.\n");
        repo.getPull()!.mergeable_state = "behind";
      },
    }),
  ).rejects.toThrow("exact clean owned checkout");
  expect(await readFile(join(repo.root, "README.md"), "utf8")).toBe("Uncommitted user content.\n");
});

it("retains the verified CI run after GitHub clears its PR association", async () => {
  const repo = await fixture();
  const result = await finalizeReleaseDocumentation({
    ...repo.options,
    pause: () => {
      Object.assign(repo.runs[0]!, {
        status: "completed",
        conclusion: "success",
        pull_requests: [],
      });
      Object.assign(repo.getPull()!, {
        state: "closed",
        merged: true,
        merge_commit_sha: "d".repeat(40),
      });
      return Promise.resolve();
    },
  });
  expect(result.finalization).toBe("merged");
});

it.each(["id", "head_sha", "workflow_id", "event", "head_branch"])(
  "rejects substituted CI %s after merge",
  async (field) => {
    const repo = await fixture();
    let merged = false;
    await expect(
      finalizeReleaseDocumentation({
        ...repo.options,
        pause: () => {
          merged = true;
          Object.assign(repo.getPull()!, {
            state: "closed",
            merged: true,
            merge_commit_sha: "d".repeat(40),
          });
          return Promise.resolve();
        },
        readGithub: async (path) => {
          const value = await repo.readGithub(path);
          return merged && /\/actions\/runs\/\d+$/u.test(path)
            ? {
                ...(value as object),
                status: "completed",
                conclusion: "success",
                pull_requests: [],
                [field]: field === "id" || field === "workflow_id" ? 999 : "unseen",
              }
            : value;
        },
      }),
    ).rejects.toThrow("run identity changed");
  },
);

it.each([
  { status: ["queued"], conclusion: null },
  { status: "completed", conclusion: ["success"] },
])("rejects coercible CI primitives %#", async (state) => {
  const repo = await fixture();
  await expect(
    finalizeReleaseDocumentation({
      ...repo.options,
      readGithub: async (path) => {
        const value = await repo.readGithub(path);
        return path.includes("/actions/")
          ? {
              workflow_runs: (
                value as { workflow_runs: Record<string, unknown>[] }
              ).workflow_runs.map((run) => ({ ...run, ...state })),
            }
          : value;
      },
    }),
  ).rejects.toThrow("invalid status");
});

it("rejects contradictory open and merged PR metadata", async () => {
  const repo = await fixture();
  await expect(
    finalizeReleaseDocumentation({
      ...repo.options,
      pause: () => {
        repo.getPull()!.merged = true;
        return Promise.resolve();
      },
    }),
  ).rejects.toThrow("foreign, closed or changed");
});

it("reports already archived records complete while preserving new highlights as advisory", async () => {
  const repo = await fixture();
  await reconcileReleaseDocumentation(repo.options);
  await repo.git("switch", "main");
  await repo.git("merge", "--ff-only", RELEASE_DOCS_BRANCH);
  repo.getPull()!.state = "closed";
  await repo.write(
    "website/docs/releases/unreleased.md",
    "---\ntitle: Unreleased changes\nunreleased: true\n---\n\n# Unreleased changes\n\nNew future behavior.\n",
  );
  await repo.git("add", ".");
  await repo.git("commit", "-m", "feat: future behavior");
  const result = await finalizeReleaseDocumentation(repo.options);
  expect(result).toMatchObject({ finalization: "unchanged", status: "unchanged" });
  expect(result.warnings).toHaveLength(1);
  const summary = await writeReleaseDocumentationFinalization(result, repository, {});
  expect(summary).toContain("already synchronized");
  expect(summary).toContain("Advisory:");
  expect(summary).not.toContain("merge the owned PR");
});

it.each([
  "unchanged",
  "merged",
  "approval-required",
  "review-required",
  "awaiting-maintainer",
] as const)("persists truthful %s outputs and the legacy handoff guard", async (finalization) => {
  const root = await mkdtemp(join(tmpdir(), "streamskope-release-output-"));
  directories.push(root);
  const output = join(root, "output");
  const summaryPath = join(root, "summary");
  const complete = finalization === "unchanged" || finalization === "merged";
  const summary = await writeReleaseDocumentationFinalization(
    {
      status: finalization === "unchanged" ? "unchanged" : "pull-request",
      finalization,
      ...(finalization !== "unchanged" ? { pullRequest: 12, headSha: sha } : {}),
      ...(finalization === "merged" ? { mergeSha: "d".repeat(40) } : {}),
      ciAction: finalization === "approval-required" ? "approval-required" : "existing",
      autoMergeEnabled: finalization === "merged",
      warnings: [],
    },
    repository,
    { output, summary: summaryPath },
  );
  expect(await readFile(output, "utf8")).toContain(
    `finalization_status=${finalization}\narchive_complete=${complete}\n`,
  );
  expect(await readFile(output, "utf8")).toContain(
    `review_required=${!complete}\napproval_required=${finalization === "approval-required"}\n`,
  );
  expect(await readFile(summaryPath, "utf8")).toBe(summary);
  if (!complete) expect(summary).toContain("Archive incomplete:");
});

it.each([false, true])(
  "returns immediate approval handoff for action-required CI, automaticCi=%s",
  async (automaticCi) => {
    const repo = await fixture();
    const result = await finalizeReleaseDocumentation({
      ...repo.options,
      automaticCi,
      pause: () => {
        throw new Error("Approval handoff must not wait");
      },
      readGithub: async (path) => {
        const value = await repo.readGithub(path);
        return path.includes("/actions/")
          ? {
              workflow_runs: repo.runs.map((run) => ({
                ...run,
                status: "completed",
                conclusion: "action_required",
              })),
            }
          : value;
      },
    });
    expect(result).toMatchObject({ finalization: "approval-required", autoMergeEnabled: false });
    expect(repo.writes.some((write) => write.path === "/graphql" || write.method === "PUT")).toBe(
      false,
    );
  },
);

it("returns immediate manual approval handoff when no owned CI run exists", async () => {
  const repo = await fixture();
  const result = await finalizeReleaseDocumentation({
    ...repo.options,
    automaticCi: false,
    pause: () => {
      throw new Error("Missing manual CI must not wait");
    },
    readGithub: (path) =>
      path.includes("/actions/") ? Promise.resolve({ workflow_runs: [] }) : repo.readGithub(path),
  });
  expect(result).toMatchObject({ finalization: "approval-required", autoMergeEnabled: false });
  expect(repo.writes.some((write) => write.path === "/graphql" || write.method === "PUT")).toBe(
    false,
  );
});

it.each([
  { status: "action_required", conclusion: "failure" },
  { status: "action_required", conclusion: "success" },
  { status: "in_progress", conclusion: "action_required" },
  { status: "queued", conclusion: "failure" },
])("rejects contradictory CI status/conclusion pairs %#", async (state) => {
  const repo = await fixture();
  await expect(
    finalizeReleaseDocumentation({
      ...repo.options,
      readGithub: async (path) => {
        const value = await repo.readGithub(path);
        return path.includes("/actions/")
          ? { workflow_runs: repo.runs.map((run) => ({ ...run, ...state })) }
          : value;
      },
    }),
  ).rejects.toThrow("inconsistent conclusion");
  expect(repo.writes.some((write) => write.path === "/graphql" || write.method === "PUT")).toBe(
    false,
  );
});
