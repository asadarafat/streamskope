import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

import {
  generateReleaseChangelog,
  githubReader,
  type GithubRead,
  type ReleaseChangelog,
} from "../../tools/package/release-changelog";
import type { ReleaseComponent } from "../../tools/package/release-version";

const execute = promisify(execFile);
const directories: string[] = [];
const repository = "owner/project";
const publishedAt = "2026-10-01T00:00:00Z";
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

interface Graph {
  root: string;
  git: (...args: string[]) => Promise<string>;
  commit: (path?: string, title?: string) => Promise<string>;
  initial: string;
  tag: (name: string, commit: string) => Promise<void>;
}
async function graph(): Promise<Graph> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-release-changelog-"));
  directories.push(root);
  const git = async (...args: string[]): Promise<string> =>
    (await execute("git", args, { cwd: root })).stdout.trim();
  await git("init", "-b", "main");
  await git("config", "user.name", "Release test");
  await git("config", "user.email", "release@example.invalid");
  let counter = 0;
  const commit = async (path = "src/shared.ts", title = "fix: source change"): Promise<string> => {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), `change ${++counter}\n`);
    await git("add", "--", path);
    await git("commit", "-m", title);
    return git("rev-parse", "HEAD");
  };
  const initial = await commit("README.md", "chore: initial source");
  const tag = async (name: string, commit: string): Promise<void> => {
    await git("tag", "-a", name, commit, "-m", name);
  };
  return { root, git, commit, initial, tag };
}
function release(
  tag_name: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    tag_name,
    draft: false,
    prerelease: false,
    published_at: publishedAt,
    target_commitish: "main",
    ...overrides,
  };
}
function pull(
  number: number,
  commit: string,
  labels: string[] = [],
  title = "fix: user-visible change",
): Record<string, unknown> {
  return {
    number,
    title,
    merged_at: publishedAt,
    merge_commit_sha: commit,
    base: { ref: "main", repo: { full_name: repository } },
    labels: labels.map((name) => ({ name })),
  };
}
function api(releases: unknown[], pulls: Record<string, unknown[]> = {}): GithubRead {
  return (path) => {
    const url = new URL(`https://api.github.com${path}`);
    const page = Number(url.searchParams.get("page"));
    expect(url.searchParams.get("per_page")).toBe("100");
    const commit = /\/commits\/([a-f0-9]+)\/pulls$/u.exec(url.pathname)?.[1];
    const values = commit ? (pulls[commit] ?? []) : releases;
    return Promise.resolve(values.slice((page - 1) * 100, page * 100));
  };
}
async function generate(
  root: string,
  sourceSha: string,
  readGithub: GithubRead,
  component: ReleaseComponent = "desktop",
  version = "0.2.0",
): Promise<ReleaseChangelog> {
  return generateReleaseChangelog({ root, sourceSha, repository, component, version, readGithub });
}

it("stable releases compare against the nearest published stable component tag, including historical build metadata", async () => {
  const repo = await graph();
  await repo.tag("v0.1.0+build.1", repo.initial);
  const rc = await repo.commit();
  await repo.tag("v0.2.0-rc.1", rc);
  await repo.tag("v0.1.1", rc);
  await repo.tag("v0.1.9", rc); // A tag without a published release is not a baseline.
  await repo.tag("plugins/eda/v9.0.0", rc);
  const source = await repo.commit();
  const result = await generate(
    repo.root,
    source,
    api([
      release("plugins/eda/v9.0.0"),
      release("v0.2.0-rc.1"),
      release("v0.1.1", { prerelease: true }),
      release("v0.1.0+build.1"),
      release("v0.1.8", { draft: true }),
    ]),
  );
  expect(result.evidence.baseline?.tag).toBe("v0.1.0+build.1");
  expect(result.evidence.commits).toEqual([rc, source]);
  expect(result.evidence.markdownSha256).toBe(
    createHash("sha256").update(result.markdown).digest("hex"),
  );
});

it("prereleases can use a preceding RC and ignore newer nonancestor or future tags", async () => {
  const repo = await graph();
  await repo.tag("v0.1.0", repo.initial);
  const rc = await repo.commit();
  await repo.tag("v0.2.0-rc.1", rc);
  const source = await repo.commit();
  const future = await repo.commit();
  await repo.tag("v0.2.0-rc.3", future);
  await repo.git("checkout", "-b", "side", repo.initial);
  const side = await repo.commit("side.ts");
  await repo.tag("v0.2.0-rc.2", side);
  const result = await generate(
    repo.root,
    source,
    api([
      release("v0.1.0"),
      release("v0.2.0-rc.1", { prerelease: true }),
      release("v0.2.0-rc.2", { prerelease: true }),
      release("v0.2.0-rc.3", { prerelease: true }),
    ]),
    "desktop",
    "0.2.0-rc.4",
  );
  expect(result.evidence.baseline?.commit).toBe(rc);
  expect(result.evidence.commits).toEqual([source]);
});

it("first plugin releases retain initial and direct commits with conservative path inference", async () => {
  const repo = await graph();
  const nsp = await repo.commit("plugins/nsp/backend/index.ts", "feat: NSP only");
  const eda = await repo.commit("test/integration/eda-capture.test.ts", "test: EDA only");
  const shared = await repo.commit("src/plugin-host.ts", "feat: shared host");
  const result = await generate(
    repo.root,
    shared,
    api([release("v0.1.0", { draft: true })]),
    "eda",
  );
  expect(result.evidence.baseline).toBeNull();
  expect(result.evidence.changes.filter((item) => item.included).map((item) => item.id)).toEqual([
    repo.initial,
    eda,
    shared,
  ]);
  expect(result.evidence.changes.find((item) => item.id === nsp)?.included).toBe(false);
  expect(result.evidence.changes.every((item) => item.selectionBasis === "paths")).toBe(true);
  expect(result.markdown).toContain("No eligible prior eda release");
  expect(result.markdown).toContain("### Direct commits");
});

it("component labels override paths, allow multiple components and exclude intentional opt-outs", async () => {
  const repo = await graph();
  await repo.tag("plugins/eda/v0.1.0", repo.initial);
  const desktop = await repo.commit("plugins/eda/ui.ts");
  const multi = await repo.commit();
  const shared = await repo.commit();
  const skipped = await repo.commit();
  const result = await generate(
    repo.root,
    skipped,
    api([release("plugins/eda/v0.1.0")], {
      [desktop]: [pull(1, desktop, ["component:desktop"])],
      [multi]: [pull(2, multi, ["component:nsp", "component:eda"], "feat: combined work")],
      [shared]: [pull(3, shared, ["component:shared"], "fix(security): fix validation")],
      [skipped]: [pull(4, skipped, ["component:eda", "release-notes:skip"])],
    }),
    "eda",
  );
  expect(result.evidence.changes.filter((item) => item.included).map((item) => item.id)).toEqual([
    2, 3,
  ]);
  expect(result.evidence.changes.every((item) => item.kind === "pr")).toBe(true);
  expect(result.markdown).toContain("### Features");
  expect(result.markdown).toContain("### Security");
  expect(result.markdown).not.toContain("/pull/4");
  expect(result.markdown).toContain("Repository-wide comparison");
});

it("collects actual mainline merge commits and deduplicates PR associations without including side branch commits", async () => {
  const repo = await graph();
  await repo.tag("v0.1.0", repo.initial);
  await repo.git("checkout", "-b", "feature");
  const side = await repo.commit("feature.ts");
  await repo.git("checkout", "main");
  const main = await repo.commit("main.ts");
  await repo.git("merge", "--no-ff", "feature", "-m", "Merge PR");
  const merge = await repo.git("rev-parse", "HEAD");
  const pr = pull(12, merge, [], "feat(api)!: new behavior");
  const result = await generate(
    repo.root,
    merge,
    api([release("v0.1.0")], { [main]: [pr], [merge]: [pr], [side]: [pull(99, side)] }),
  );
  expect(result.evidence.commits).toEqual([main, merge]);
  expect(
    result.evidence.changes.filter((item) => item.kind === "pr").map((item) => item.id),
  ).toEqual([12]);
  expect(result.evidence.changes.find((item) => item.id === 12)?.paths).toEqual([
    "feature.ts",
    "main.ts",
  ]);
  expect(result.markdown).toContain("### Breaking changes");
});

it("ignores future, unmerged, foreign-repository and other-base PRs and retains their scoped commits", async () => {
  const repo = await graph();
  await repo.tag("v0.1.0", repo.initial);
  const source = await repo.commit();
  const future = await repo.commit();
  const result = await generate(
    repo.root,
    source,
    api([release("v0.1.0")], {
      [source]: [
        pull(1, future),
        { merged_at: null },
        { ...pull(3, source), base: { ref: "other", repo: { full_name: repository } } },
        { ...pull(4, source), base: { ref: "main", repo: { full_name: "foreign/repo" } } },
      ],
    }),
  );
  expect(result.evidence.changes).toMatchObject([{ kind: "commit", id: source, included: true }]);
});

it("fetches all release and associated-PR pages before choosing a baseline and rendering escaped titles", async () => {
  const repo = await graph();
  await repo.tag("v0.1.0", repo.initial);
  const source = await repo.commit();
  const result = await generate(
    repo.root,
    source,
    api(
      [
        ...Array.from({ length: 100 }, (_, index) => release(`unrelated-${index}`)),
        release("v0.1.0"),
      ],
      {
        [source]: [
          ...Array.from({ length: 100 }, (_, index) => ({ number: index + 1, merged_at: null })),
          pull(999, source, [], "fix: <script>[link](https://evil.invalid)\n# injected"),
        ],
      },
    ),
  );
  expect(result.evidence.baseline?.commit).toBe(repo.initial);
  expect(result.evidence.changes[0]?.id).toBe(999);
  expect(result.markdown).not.toContain("<script>");
  expect(result.markdown).not.toContain("\n# injected");
  expect(result.markdown).toContain("[#999](https://github.com/owner/project/pull/999)");
});

it.each([
  [
    "unknown component",
    (commit: string): Record<string, unknown> => pull(1, commit, ["component:edaa"]),
  ],
  [
    "missing labels",
    (commit: string): Record<string, unknown> => ({ ...pull(1, commit), labels: undefined }),
  ],
  [
    "missing merge date",
    (commit: string): Record<string, unknown> => ({ ...pull(1, commit), merged_at: undefined }),
  ],
  [
    "invalid PR number",
    (commit: string): Record<string, unknown> => ({ ...pull(1, commit), number: -1 }),
  ],
])("refuses incomplete evidence: %s", async (_name, malformed) => {
  const repo = await graph();
  await expect(
    generate(repo.root, repo.initial, api([], { [repo.initial]: [malformed(repo.initial)] })),
  ).rejects.toThrow();
});

it("fails on API errors, malformed/repeating pagination and missing published tags", async () => {
  const repo = await graph();
  await expect(
    generate(repo.root, repo.initial, () => Promise.reject(new Error("HTTP 403"))),
  ).rejects.toThrow("HTTP 403");
  await expect(
    generate(repo.root, repo.initial, () => Promise.resolve({ message: "error" })),
  ).rejects.toThrow("invalid page");
  const full = Array.from({ length: 100 }, (_, index) => release(`unrelated-${index}`));
  await expect(generate(repo.root, repo.initial, () => Promise.resolve(full))).rejects.toThrow(
    "repeated a page",
  );
  await expect(generate(repo.root, repo.initial, api([release("v0.1.0")]))).rejects.toThrow();
});

it("refuses shallow history and invalid source or repository identifiers", async () => {
  const repo = await graph();
  const clone = join(repo.root, "shallow");
  await execute("git", ["clone", "--depth=1", `file://${repo.root}`, clone]);
  await expect(generate(clone, repo.initial, api([]))).rejects.toThrow("complete Git history");
  await expect(generate(repo.root, "main", api([]))).rejects.toThrow("exact 40-character");
  await expect(
    generateReleaseChangelog({
      root: repo.root,
      sourceSha: repo.initial,
      repository: "owner/repo?bad",
      component: "desktop",
      version: "0.2.0",
      readGithub: api([]),
    }),
  ).rejects.toThrow("repository");
});

it("requires secure API endpoints before sending authentication", () => {
  expect(() => githubReader("token", "http://api.github.com")).toThrow("HTTPS");
  expect(() => githubReader("token", "https://user:password@api.github.com")).toThrow("HTTPS");
});

it.each([
  {
    name: "skip applies to every rebased commit",
    labels: ["release-notes:skip"],
    included: false,
    inferred: true,
  },
  {
    name: "component exclusion applies to every rebased commit",
    labels: ["component:nsp"],
    included: false,
    inferred: false,
  },
  {
    name: "multiple component labels include the PR once",
    labels: ["component:eda", "component:desktop"],
    included: true,
    inferred: false,
  },
  { name: "path inference unions all rebased commits", labels: [], included: true, inferred: true },
])("rebased PR: $name", async ({ labels, included, inferred }) => {
  const repo = await graph();
  await repo.tag("plugins/eda/v0.1.0", repo.initial);
  const first = await repo.commit("plugins/nsp/first.ts", "feat: first rebased change");
  const last = await repo.commit("src/shared.ts", "fix: last rebased change");
  const rebased = pull(42, last, labels);
  const result = await generate(
    repo.root,
    last,
    api([release("plugins/eda/v0.1.0")], {
      [first]: [rebased],
      [last]: [rebased],
    }),
    "eda",
  );
  expect(result.evidence.changes).toHaveLength(1);
  expect(result.evidence.changes[0]).toMatchObject({
    kind: "pr",
    id: 42,
    commit: last,
    commits: [first, last],
    paths: ["plugins/nsp/first.ts", "src/shared.ts"],
    included,
    selectionBasis: inferred ? "paths" : "labels",
  });
  if (inferred) expect(result.evidence.changes[0]?.components).toEqual(["desktop", "eda", "nsp"]);
  expect(result.markdown).not.toContain("### Direct commits");
  expect((result.markdown.match(/\[#42\]/gu) ?? []).length).toBe(included ? 1 : 0);
});

it("retains both sides of a cross-component rename when inferring affected components", async () => {
  const repo = await graph();
  const previous = await repo.commit("src/shared.ts", "feat: shared source");
  await repo.tag("v0.1.0", previous);
  await mkdir(join(repo.root, "plugins/eda"), { recursive: true });
  await repo.git("mv", "src/shared.ts", "plugins/eda/shared.ts");
  await repo.git("commit", "-m", "refactor: move integration into plugin");
  const source = await repo.git("rev-parse", "HEAD");
  const result = await generate(
    repo.root,
    source,
    api([release("v0.1.0")], {
      [source]: [pull(7, source)],
    }),
  );
  expect(result.evidence.changes[0]).toMatchObject({
    kind: "pr",
    id: 7,
    included: true,
    components: ["desktop", "eda", "nsp"],
    paths: ["plugins/eda/shared.ts", "src/shared.ts"],
  });
});
