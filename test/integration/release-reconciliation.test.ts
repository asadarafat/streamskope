import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";

import { afterEach, expect, it } from "vitest";

import type { GithubRead } from "../../tools/package/release-changelog";
import {
  applyReleaseReconciliation,
  checkReleaseReconciliation,
  EMPTY_DESKTOP_COMMENTARY,
  EMPTY_PLUGIN_COMMENTARY,
  planReleaseReconciliation,
  type ReconciliationOptions,
  type ReconciliationPlan,
} from "../../tools/package/release-reconciliation";

const execute = promisify(execFile);
const directories: string[] = [];
const repository = "owner/project";
const desktopHighlights =
  "---\ntitle: Unreleased changes\nunreleased: true\n---\n\n# Unreleased changes\n\n## Release highlights\n\nAn implemented desktop feature.\n";
const pluginHighlights = "# Release highlights\n\nAn implemented plugin feature.\n";
const notes = "\n# Published release\n\nReviewed exact Markdown, including the final newline.\n";
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

interface Fixture {
  root: string;
  initial: string;
  git: (...args: string[]) => Promise<string>;
  file: (path: string, contents: string) => Promise<void>;
  commit: (title?: string) => Promise<string>;
  tag: (tag: string, source?: string) => Promise<void>;
  options: (releases: unknown[], overrides?: Record<string, unknown>) => ReconciliationOptions;
}
async function fixture(): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-release-reconciliation-"));
  directories.push(root);
  const git = async (...args: string[]): Promise<string> =>
    (await execute("git", args, { cwd: root })).stdout.trim();
  const file = async (path: string, contents: string): Promise<void> => {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), contents);
  };
  await git("init", "-b", "main");
  await git("config", "user.name", "Release test");
  await git("config", "user.email", "release@example.invalid");
  await file("website/docs/releases/unreleased.md", desktopHighlights);
  await file("plugins/eda/RELEASE_NOTES.md", pluginHighlights);
  await file("plugins/nsp/RELEASE_NOTES.md", pluginHighlights);
  await file("website/zensical.toml", '[project.extra]\ndesktop_release = "v0.1.0"\n');
  await file(
    "website/docs/releases/index.md",
    "# Release history\n\n<!-- plugin-release-history -->\n\n<!-- /plugin-release-history -->\n\nOther reviewed content stays intact.\n",
  );
  const commit = async (title = "feat: reviewed change"): Promise<string> => {
    await git("add", ".");
    await git("commit", "--allow-empty", "-m", title);
    return git("rev-parse", "HEAD");
  };
  const initial = await commit();
  const tag = async (name: string, source = initial): Promise<void> => {
    await git("tag", "-a", name, source, "-m", name);
  };
  const options = (
    releases: unknown[],
    overrides: Record<string, unknown> = {},
  ): ReconciliationOptions => {
    const readGithub: GithubRead = async (path) => {
      const url = new URL(`https://api.github.com${path}`);
      if (url.pathname.endsWith("/releases")) {
        const page = Number(url.searchParams.get("page"));
        expect(url.searchParams.get("per_page")).toBe("100");
        return releases.slice((page - 1) * 100, page * 100);
      }
      const matched = /\/commits\/([^/]+)$/u.exec(url.pathname)?.[1];
      if (!matched) throw new Error(`Unexpected API request ${path}.`);
      const name = decodeURIComponent(matched);
      return overrides[name] ?? { sha: await git("rev-parse", `refs/tags/${name}^{commit}`) };
    };
    return { root, repository, readGithub };
  };
  return { root, initial, git, file, commit, tag, options };
}
function release(
  tag_name: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    id: [...tag_name].reduce((result, char, index) => result + char.charCodeAt(0) * (index + 1), 0),
    tag_name,
    draft: false,
    prerelease: false,
    immutable: true,
    published_at: "2026-10-01T00:00:00Z",
    target_commitish: "a misleading mutable branch",
    body: notes,
    ...overrides,
  };
}
async function reconcile(options: ReconciliationOptions): Promise<ReconciliationPlan> {
  const plan = await planReleaseReconciliation(options);
  await applyReleaseReconciliation(plan, options.root);
  return plan;
}

it("reconciles simultaneous desktop/plugin publications, preserves authoritative body bytes, and repeats without writes", async () => {
  const repo = await fixture();
  const names = ["v0.2.0", "plugins/eda/v0.1.0", "plugins/nsp/v0.1.0"];
  for (const name of names) await repo.tag(name);
  const options = repo.options(names.map((name) => release(name)));
  const plan = await reconcile(options);
  expect(plan.latestStableDesktop?.sourceSha).toBe(repo.initial);
  expect(plan.latestStableDesktop?.tag).toBe("v0.2.0");
  expect(plan.commentary.map((item) => item.status)).toEqual(["clear", "clear", "clear"]);
  for (const name of names) {
    const archive = await readFile(join(repo.root, `website/docs/releases/${name}.md`), "utf8");
    expect(archive.slice(archive.indexOf("\n---\n\n") + "\n---\n\n".length)).toBe(notes);
    expect(archive).toContain(name.startsWith("v") ? "release_date:" : "title: ");
    if (!name.startsWith("v")) expect(archive).not.toContain("release_version:");
  }
  expect(await readFile(join(repo.root, "website/docs/releases/unreleased.md"), "utf8")).toBe(
    EMPTY_DESKTOP_COMMENTARY,
  );
  expect(await readFile(join(repo.root, "plugins/eda/RELEASE_NOTES.md"), "utf8")).toBe(
    EMPTY_PLUGIN_COMMENTARY,
  );
  expect(await readFile(join(repo.root, "website/docs/releases/index.md"), "utf8")).toContain(
    "Other reviewed content stays intact.",
  );
  const repeated = await planReleaseReconciliation(options);
  expect(repeated.changes).toEqual([]);
  expect(repeated.warnings).toEqual([]);
  await expect(checkReleaseReconciliation(options, "desktop")).resolves.toMatchObject({
    changes: [],
  });
});

it("keeps newer commentary in full and never clears another component without its own publication", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  await repo.file(
    "website/docs/releases/unreleased.md",
    `${desktopHighlights}\nA newer change after the released source.\n`,
  );
  await repo.commit();
  const options = repo.options([release("v0.2.0")]);
  const before = await readFile(join(repo.root, "website/docs/releases/unreleased.md"), "utf8");
  const plan = await reconcile(options);
  expect(plan.commentary.find((item) => item.component === "desktop")?.status).toBe("preserved");
  expect(plan.warnings).toHaveLength(1);
  expect(await readFile(join(repo.root, "website/docs/releases/unreleased.md"), "utf8")).toBe(
    before,
  );
  expect(await readFile(join(repo.root, "plugins/nsp/RELEASE_NOTES.md"), "utf8")).toBe(
    pluginHighlights,
  );
  expect((await checkReleaseReconciliation(options, "desktop")).warnings).toHaveLength(1);
});

it("a late old release event cannot downgrade the highest stable version or replace highlights with RC state", async () => {
  const repo = await fixture();
  await repo.tag("v0.1.0");
  const stable = await repo.commit();
  await repo.tag("v0.10.0", stable);
  const rc = await repo.commit();
  await repo.tag("v0.11.0-rc.1", rc);
  const options = repo.options([
    release("v0.1.0", { published_at: "2026-10-07T00:00:00Z" }),
    release("v0.11.0-rc.1", { prerelease: true }),
    release("v0.10.0", { published_at: "2026-10-02T00:00:00Z" }),
  ]);
  const plan = await reconcile(options);
  expect(plan.latestStableDesktop?.tag).toBe("v0.10.0");
  expect(plan.commentary.find((item) => item.component === "desktop")?.tag).toBe("v0.10.0");
  expect(await readFile(join(repo.root, "website/zensical.toml"), "utf8")).toContain(
    'desktop_release = "v0.10.0"',
  );
  expect(plan.published.map((item) => item.tag)).toContain("v0.11.0-rc.1");
});

it("prereleases are archived without claiming stable availability or clearing release highlights", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0-rc.1");
  const plan = await reconcile(repo.options([release("v0.2.0-rc.1", { prerelease: true })]));
  expect(plan.latestStableDesktop).toBeNull();
  expect(plan.commentary.find((item) => item.component === "desktop")?.status).toBe("no-release");
  expect(await readFile(join(repo.root, "website/docs/releases/unreleased.md"), "utf8")).toBe(
    desktopHighlights,
  );
});

it("archives nonancestral plugin sources but preserves main commentary; a foreign stable desktop fails closed", async () => {
  const repo = await fixture();
  await repo.git("checkout", "-b", "side");
  const side = await repo.commit();
  await repo.tag("plugins/eda/v0.1.0", side);
  await repo.tag("v0.2.0", side);
  await repo.git("checkout", "main");
  const plugin = await reconcile(repo.options([release("plugins/eda/v0.1.0")]));
  expect(plugin.published[0]?.ancestor).toBe(false);
  expect(plugin.commentary.find((item) => item.component === "eda")?.status).toBe("no-release");
  expect(await readFile(join(repo.root, "plugins/eda/RELEASE_NOTES.md"), "utf8")).toBe(
    pluginHighlights,
  );
  await expect(planReleaseReconciliation(repo.options([release("v0.2.0")]))).rejects.toThrow(
    "outside the current source ancestry",
  );
});

it("an existing archive mismatch cannot be silently corrected even when publication content changes", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  await reconcile(repo.options([release("v0.2.0")]));
  const archive = await readFile(join(repo.root, "website/docs/releases/v0.2.0.md"), "utf8");
  await expect(
    planReleaseReconciliation(
      repo.options([release("v0.2.0", { body: `${notes}Different final content.\n` })]),
    ),
  ).rejects.toThrow("Archived body differs");
  expect(await readFile(join(repo.root, "website/docs/releases/v0.2.0.md"), "utf8")).toBe(archive);
});

it("ignores drafts and unrelated app releases, and refuses malformed, mutable, duplicate or moved publication evidence", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  const valid = release("v0.2.0");
  expect(
    (
      await planReleaseReconciliation(
        repo.options([
          release("v0.3.0", { draft: true, body: null }),
          release("eda-app/v26.8.2", { immutable: false }),
        ]),
      )
    ).published,
  ).toEqual([]);
  for (const invalid of [
    { immutable: false },
    { immutable: undefined },
    { immutable: "true" },
    { body: "Missing title" },
    { body: "#     \n\nMissing title text.\n" },
    { id: "42" },
    { published_at: "tomorrow" },
    { published_at: "2026-02-30T00:00:00Z" },
    { prerelease: undefined },
    { tag_name: "v0.2.0-invalid..version" },
  ])
    await expect(
      planReleaseReconciliation(repo.options([{ ...valid, ...invalid }])),
    ).rejects.toThrow();
  await expect(planReleaseReconciliation(repo.options([valid, valid]))).rejects.toThrow(
    "duplicate release identity",
  );
  await expect(
    planReleaseReconciliation(repo.options([valid], { "v0.2.0": { sha: "a".repeat(40) } })),
  ).rejects.toThrow("differs from its fetched source commit");
});

it("the next-release gate blocks unarchived component publication and unchanged shipped highlights", async () => {
  const repo = await fixture();
  await repo.tag("plugins/eda/v0.1.0");
  const options = repo.options([release("plugins/eda/v0.1.0")]);
  await expect(checkReleaseReconciliation(options, "eda")).rejects.toThrow(
    "1 missing archives, shipped highlights still pending",
  );
  await expect(checkReleaseReconciliation(options, "nsp")).resolves.toBeDefined();
  const plan = await planReleaseReconciliation(options);
  await applyReleaseReconciliation(
    { ...plan, changes: plan.changes.filter((item) => item.reason !== "commentary") },
    repo.root,
  );
  await expect(checkReleaseReconciliation(options, "eda")).rejects.toThrow(
    "0 missing archives, shipped highlights still pending",
  );
  await reconcile(options);
  await expect(checkReleaseReconciliation(options, "eda")).resolves.toBeDefined();
});

it("refuses a stale preimage before any write and refuses unknown higher source baseline", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  const options = repo.options([release("v0.2.0")]);
  const plan = await planReleaseReconciliation(options);
  await repo.file("website/zensical.toml", 'desktop_release = "v9.0.0"\n');
  await expect(applyReleaseReconciliation(plan, repo.root)).rejects.toThrow(
    "File changed after reconciliation planning",
  );
  await expect(
    readFile(join(repo.root, "website/docs/releases/v0.2.0.md"), "utf8"),
  ).rejects.toThrow();
  await expect(planReleaseReconciliation(options)).rejects.toThrow("Refusing to downgrade");
});

it("supports old build metadata archives and treats the prior neutral templates as empty commentary", async () => {
  const repo = await fixture();
  await repo.file(
    "website/docs/releases/unreleased.md",
    "---\ntitle: Unreleased changes\nunreleased: true\n---\n\n# Unreleased changes\n\nNo desktop changes are pending release.\n\nDesktop versions are assigned during release CI. Pending plugin changes remain\nin each plugin's release commentary and are released independently.\n",
  );
  await repo.file(
    "plugins/eda/RELEASE_NOTES.md",
    "## Unreleased changes\n\nNo plugin changes are pending release.\n",
  );
  await repo.commit();
  await repo.tag("v0.1.0+build.1");
  const plan = await reconcile(repo.options([release("v0.1.0+build.1", { prerelease: true })]));
  expect(plan.commentary.find((item) => item.component === "desktop")?.status).toBe("empty");
  expect(plan.commentary.find((item) => item.component === "eda")?.status).toBe("empty");
  expect(plan.latestStableDesktop).toBeNull();
});

it("repairs a plugin history link without touching released notes and blocks the next plugin release until repaired", async () => {
  const repo = await fixture();
  await repo.tag("plugins/eda/v0.1.0");
  const options = repo.options([release("plugins/eda/v0.1.0")]);
  await reconcile(options);
  const path = "website/docs/releases/index.md";
  const index = await readFile(join(repo.root, path), "utf8");
  await repo.file(
    path,
    index.replace("releases/tag/plugins/eda/v0.1.0", "releases/tag/plugins/nsp/v0.1.0"),
  );
  await expect(checkReleaseReconciliation(options, "eda")).rejects.toThrow(
    "incomplete plugin release history",
  );
  const repair = await reconcile(options);
  expect(repair.changes.map((item) => item.reason)).toEqual(["plugin-index"]);
  expect((await planReleaseReconciliation(options)).changes).toEqual([]);
});

it("fails before writing when the checkout moves or GitHub cannot provide authoritative tags", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  const options = repo.options([release("v0.2.0")]);
  const plan = await planReleaseReconciliation(options);
  await repo.commit();
  await expect(applyReleaseReconciliation(plan, repo.root)).rejects.toThrow("Checkout changed");
  await expect(readFile(join(repo.root, "website/docs/releases/v0.2.0.md"))).rejects.toThrow();
  await repo.git("tag", "-d", "v0.2.0");
  await expect(planReleaseReconciliation(options)).rejects.toThrow();
  await expect(
    planReleaseReconciliation({
      ...options,
      readGithub: () => Promise.reject(new Error("API unavailable")),
    }),
  ).rejects.toThrow("API unavailable");
});

it("plugin finalization is independent of an unarchived desktop release and its stale documentation baseline", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  await repo.tag("plugins/eda/v0.1.0");
  const options = repo.options([release("v0.2.0"), release("plugins/eda/v0.1.0")]);
  const plan = await planReleaseReconciliation(options);
  await applyReleaseReconciliation(
    {
      ...plan,
      changes: plan.changes.filter(
        (change) =>
          change.reason === "plugin-index" ||
          change.path.startsWith("website/docs/releases/plugins/") ||
          change.path === "plugins/eda/RELEASE_NOTES.md",
      ),
    },
    repo.root,
  );
  const plugin = await checkReleaseReconciliation(options, "eda");
  expect(plugin.changes.some((change) => change.reason === "desktop-baseline")).toBe(true);
  await expect(checkReleaseReconciliation(options, "desktop")).rejects.toThrow(
    "stale published desktop baseline",
  );
});

it("neutral source templates with updated development guidance are empty without needing to have been shipped", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  await repo.tag("plugins/eda/v0.1.0");
  const neutral = EMPTY_DESKTOP_COMMENTARY.replace(
    "Published releases provide the component baselines.",
    "Inspect the maintained component baselines before drafting a release.",
  );
  await repo.file("website/docs/releases/unreleased.md", neutral.replaceAll("\n", "\r\n"));
  await repo.file("plugins/eda/RELEASE_NOTES.md", `\n${EMPTY_PLUGIN_COMMENTARY}\n`);
  await repo.commit();
  const plan = await planReleaseReconciliation(
    repo.options([release("v0.2.0"), release("plugins/eda/v0.1.0")]),
  );
  expect(
    plan.commentary.filter((item) => item.component !== "nsp").map((item) => item.status),
  ).toEqual(["empty", "empty"]);
  expect(plan.changes.some((item) => item.reason === "commentary")).toBe(false);
  expect(plan.warnings).toEqual([]);
});

it("with no ancestral stable component release the gate preserves commentary and requires only archival", async () => {
  const repo = await fixture();
  await repo.tag("plugins/eda/v0.2.0-rc.1");
  await repo.git("checkout", "-b", "side");
  const side = await repo.commit();
  await repo.tag("plugins/eda/v0.1.0", side);
  await repo.git("checkout", "main");
  const options = repo.options([
    release("plugins/eda/v0.2.0-rc.1", { prerelease: true }),
    release("plugins/eda/v0.1.0"),
  ]);
  await expect(checkReleaseReconciliation(options, "eda")).rejects.toThrow("2 missing archives");
  const archived = await reconcile(options);
  expect(archived.commentary.find((item) => item.component === "eda")?.status).toBe("no-release");
  expect(await readFile(join(repo.root, "plugins/eda/RELEASE_NOTES.md"), "utf8")).toBe(
    pluginHighlights,
  );
  await expect(checkReleaseReconciliation(options, "eda")).resolves.toBeDefined();
});

it("validates archived metadata without changing its body or rejecting CRLF frontmatter", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  const options = repo.options([release("v0.2.0")]);
  await reconcile(options);
  const path = "website/docs/releases/v0.2.0.md";
  const archive = await readFile(join(repo.root, path), "utf8");
  for (const invalid of [
    archive.replace("title: StreamSkope v0.2.0", "title:\n"),
    archive.replace("title: StreamSkope v0.2.0", "title: StreamSkope v0.2.0\ntitle: Duplicate"),
    archive.replace("release_version: 0.2.0", "release_version:\n0.2.0"),
  ]) {
    await repo.file(path, invalid);
    await expect(planReleaseReconciliation(options)).rejects.toThrow("Archived v0.2.0");
  }
  const boundary = archive.indexOf("\n---\n\n") + "\n---\n\n".length;
  await repo.file(
    path,
    archive.slice(0, boundary).replaceAll("\n", "\r\n") + archive.slice(boundary),
  );
  const plan = await planReleaseReconciliation(options);
  expect(plan.changes).toEqual([]);
});

it("development version namespaces cannot masquerade as published component releases", async () => {
  const repo = await fixture();
  for (const tag of ["v0.0.0-dev", "plugins/eda/v0.0.0", "plugins/nsp/v0.0.0+build.1"]) {
    await repo.tag(tag);
    await expect(planReleaseReconciliation(repo.options([release(tag)]))).rejects.toThrow(
      "has an invalid version",
    );
  }
});

it("release history summaries select prose after distribution headings, tables, comments and fenced examples", async () => {
  const repo = await fixture();
  await repo.tag("v0.2.0");
  const body =
    "# Published release\n\n## Distribution\n\n| Platform | File |\n| --- | --- |\n| Linux | installer |\n\n<!-- Internal presentation note.\n\nNot a public summary. -->\n\n```sh\nexample command\n\nMore example content.\n```\n\nInstall the browser workbench with its encrypted vault.\nThe image supports two host architectures.\n\n## Changes\n\nAdditional details.\n";
  const plan = await planReleaseReconciliation(repo.options([release("v0.2.0", { body })]));
  const archive = plan.changes.find((change) => change.reason === "archive")!.contents;
  expect(archive).toContain(
    'release_summary: "Install the browser workbench with its encrypted vault. The image supports two host architectures."',
  );
  expect(archive.endsWith(body)).toBe(true);
  const headingsOnly = await planReleaseReconciliation(
    repo.options([release("v0.2.0", { body: "# Published release\n\n## Distribution\n" })]),
  );
  expect(headingsOnly.changes.find((change) => change.reason === "archive")!.contents).toContain(
    'release_summary: "StreamSkope v0.2.0"',
  );
});
