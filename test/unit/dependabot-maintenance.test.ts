import { describe, expect, it } from "vitest";

import {
  assertRoutineDependencyUpdate,
  maintainDependabot,
  type GithubApi,
} from "../../tools/check/dependabot";

type Lock = { lockfileVersion: number; packages: Record<string, Record<string, unknown>> };

function locked(lock: Lock, name: string): Record<string, unknown> {
  const entry = lock.packages[`node_modules/${name}`];
  if (!entry) throw new Error(`Missing test package ${name}`);
  return entry;
}

function fixture(): {
  before: Record<string, unknown> & { devDependencies: Record<string, string> };
  after: Record<string, unknown> & { devDependencies: Record<string, string> };
  oldLock: { lockfileVersion: number; packages: Record<string, Record<string, unknown>> };
  newLock: { lockfileVersion: number; packages: Record<string, Record<string, unknown>> };
} {
  const before = {
    name: "streamskope",
    version: "0.0.0-dev",
    scripts: { check: "bash tools/check.sh" },
    dependencies: { "node-forge": "1.4.0" },
    overrides: { handlebars: "4.7.10" },
    devDependencies: {
      "@types/node": "24.10.1",
      "typescript-eslint": "8.71.0",
      prettier: "3.9.9",
      react: "19.3.0",
    },
  };
  const after = structuredClone(before);
  after.devDependencies["@types/node"] = "24.19.1";
  after.devDependencies["typescript-eslint"] = "8.71.1";
  const oldLock: Lock = {
    lockfileVersion: 3,
    packages: {
      "": { ...before },
      ...Object.fromEntries(
        Object.entries({
          ...before.dependencies,
          ...before.devDependencies,
          "undici-types": "7.16.0",
          "@typescript-eslint/parser": "8.71.0",
          handlebars: "4.7.10",
        }).map(([name, version]) => [
          `node_modules/${name}`,
          {
            version,
            dev: name !== "node-forge",
            license: "MIT",
            resolved: `https://registry.npmjs.org/${name}/-/${name.split("/").at(-1)}-${version}.tgz`,
            integrity: `sha512-${Buffer.alloc(64, 1).toString("base64")}`,
          },
        ]),
      ),
    },
  };
  const newLock = structuredClone(oldLock);
  newLock.packages[""] = { ...after };
  for (const [name, version] of Object.entries({
    "@types/node": "24.19.1",
    "typescript-eslint": "8.71.1",
    "undici-types": "7.24.6",
    "@typescript-eslint/parser": "8.71.1",
  }))
    Object.assign(locked(newLock, name), {
      version,
      resolved: `https://registry.npmjs.org/${name}/-/${name.split("/").at(-1)}-${version}.tgz`,
      integrity: `sha512-${Buffer.alloc(64, 2).toString("base64")}`,
    });
  return { before, after, oldLock, newLock };
}

function classify(value: ReturnType<typeof fixture>): string[] {
  return assertRoutineDependencyUpdate(value.before, value.after, value.oldLock, value.newLock);
}

describe("routine dependency boundary", () => {
  it("accepts Node 24 typings and lint patches with their existing transitive families", () => {
    expect(classify(fixture())).toEqual(["@types/node", "typescript-eslint"]);
  });

  it("accepts a Prettier patch without treating other development dependencies as safe", () => {
    const value = fixture();
    value.after.devDependencies.prettier = "3.9.10";
    Object.assign(locked(value.newLock, "prettier"), {
      version: "3.9.10",
      resolved: "https://registry.npmjs.org/prettier/-/prettier-3.9.10.tgz",
    });
    expect(classify(value)).toContain("prettier");
  });

  it.each([
    [
      "new manifest script",
      (v: ReturnType<typeof fixture>): void => {
        v.after.scripts = { postinstall: "untrusted" };
      },
    ],
    [
      "mitigation override",
      (v: ReturnType<typeof fixture>): void => {
        v.after.overrides = { handlebars: "4.7.9" };
      },
    ],
    [
      "runtime dependency",
      (v: ReturnType<typeof fixture>): void => {
        v.after.dependencies = { "node-forge": "1.5.0" };
      },
    ],
    [
      "bundled development dependency",
      (v: ReturnType<typeof fixture>): void => {
        v.after.devDependencies.react = "19.3.1";
      },
    ],
    [
      "new dependency",
      (v: ReturnType<typeof fixture>): void => {
        v.after.devDependencies.unknown = "1.0.0";
      },
    ],
    [
      "removed dependency",
      (v: ReturnType<typeof fixture>): void => {
        delete v.after.devDependencies.react;
      },
    ],
    [
      "Node major",
      (v: ReturnType<typeof fixture>): void => {
        v.after.devDependencies["@types/node"] = "25.0.0";
      },
    ],
    [
      "lint minor",
      (v: ReturnType<typeof fixture>): void => {
        v.after.devDependencies["typescript-eslint"] = "8.72.0";
      },
    ],
    [
      "downgrade",
      (v: ReturnType<typeof fixture>): void => {
        v.after.devDependencies["typescript-eslint"] = "8.70.9";
      },
    ],
    [
      "prerelease",
      (v: ReturnType<typeof fixture>): void => {
        v.after.devDependencies["typescript-eslint"] = "8.71.1-beta.1";
      },
    ],
    [
      "range",
      (v: ReturnType<typeof fixture>): void => {
        v.after.devDependencies["@types/node"] = "^24.19.1";
      },
    ],
    [
      "lockfile version",
      (v: ReturnType<typeof fixture>): void => {
        v.newLock.lockfileVersion = 2;
      },
    ],
    [
      "runtime transitive drift",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "node-forge").integrity = "tampered";
      },
    ],
    [
      "mitigated transitive drift",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "handlebars").version = "4.7.11";
      },
    ],
    [
      "new transitive package",
      (v: ReturnType<typeof fixture>): void => {
        v.newLock.packages["node_modules/extra"] = { version: "1.0.0", dev: true };
      },
    ],
    [
      "removed transitive package",
      (v: ReturnType<typeof fixture>): void => {
        delete v.newLock.packages["node_modules/undici-types"];
      },
    ],
    [
      "runtime promotion",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "undici-types").dev = false;
      },
    ],
    [
      "install script",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "undici-types").hasInstallScript = true;
      },
    ],
    [
      "license change",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "undici-types").license = "unknown";
      },
    ],
    [
      "engine change",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "undici-types").engines = { node: ">=26" };
      },
    ],
    [
      "registry lookalike",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "undici-types").resolved =
          "https://registry.npmjs.org.attacker.invalid/a.tgz";
      },
    ],
    [
      "missing integrity",
      (v: ReturnType<typeof fixture>): void => {
        delete locked(v.newLock, "undici-types").integrity;
      },
    ],
    [
      "transitive major",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "undici-types").version = "8.0.0";
      },
    ],
    [
      "unchanged-version repack",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "undici-types").version = "7.16.0";
      },
    ],
    [
      "manifest/lock mismatch",
      (v: ReturnType<typeof fixture>): void => {
        locked(v.newLock, "typescript-eslint").version = "8.71.2";
      },
    ],
  ])("requires review for %s", (_name, mutate) => {
    const value = fixture();
    mutate(value);
    expect(() => classify(value)).toThrow();
  });

  it("refuses a changed internal lint package when its direct owner did not change", () => {
    const value = fixture();
    value.after.devDependencies["typescript-eslint"] = "8.71.0";
    value.newLock.packages["node_modules/typescript-eslint"] = locked(
      value.oldLock,
      "typescript-eslint",
    );
    expect(() => classify(value)).toThrow("manual review");
  });
});

const BASE = "a".repeat(40),
  HEAD = "b".repeat(40);
const PREFIX = "/repos/asadarafat/streamskope";
function githubFixture(): {
  api: GithubApi;
  responses: Map<string, unknown>;
  reads: string[];
  writes: { path: string; body: Record<string, unknown> }[];
} {
  const data = fixture();
  const responses = new Map<string, unknown>([
    [
      `${PREFIX}/actions/runs/101`,
      {
        event: "pull_request",
        path: ".github/workflows/ci.yml",
        status: "completed",
        conclusion: "success",
        head_sha: HEAD,
        pull_requests: [{ number: 140 }],
      },
    ],
    [
      `${PREFIX}/pulls/140`,
      {
        state: "open",
        draft: false,
        user: { login: "dependabot[bot]" },
        head: {
          sha: HEAD,
          ref: "dependabot/npm_and_yarn/routine-tooling",
          repo: { full_name: "asadarafat/streamskope" },
        },
        base: { sha: BASE, ref: "main", repo: { full_name: "asadarafat/streamskope" } },
        mergeable_state: "clean",
        changed_files: 2,
      },
    ],
    [`${PREFIX}/git/ref/heads/main`, { object: { sha: BASE } }],
    [
      `${PREFIX}/rules/branches/main`,
      [
        { type: "pull_request" },
        {
          type: "required_status_checks",
          parameters: {
            strict_required_status_checks_policy: true,
            required_status_checks: [{ context: "CI", integration_id: 15368 }],
          },
        },
      ],
    ],
    [
      `${PREFIX}/actions/runs/101/jobs?filter=latest&per_page=100`,
      {
        total_count: 4,
        jobs: [
          "Static, unit and integration",
          "Documentation",
          "Real providers and browser",
          "CI",
        ].map((name) => ({ name, status: "completed", conclusion: "success" })),
      },
    ],
    [
      `${PREFIX}/pulls/140/files?per_page=100`,
      ["package.json", "package-lock.json"].map((filename) => ({ filename, status: "modified" })),
    ],
  ]);
  for (const [name, sha, content] of [
    ["package.json", BASE, data.before],
    ["package.json", HEAD, data.after],
    ["package-lock.json", BASE, data.oldLock],
    ["package-lock.json", HEAD, data.newLock],
  ] as const)
    responses.set(`${PREFIX}/contents/${name}?ref=${sha}`, {
      type: "file",
      encoding: "base64",
      content: Buffer.from(JSON.stringify(content)).toString("base64"),
    });
  const reads: string[] = [],
    writes: { path: string; body: Record<string, unknown> }[] = [];
  const api: GithubApi = (path, body) => {
    if (body) {
      writes.push({ path, body });
      return Promise.resolve({ merged: true });
    }
    reads.push(path);
    if (!responses.has(path)) throw new Error(`Unexpected API read: ${path}`);
    return Promise.resolve(structuredClone(responses.get(path)));
  };
  return { api, responses, reads, writes };
}

describe("protected Dependabot maintenance", () => {
  it("defaults to a read-only decision against the complete source-bound diff", async () => {
    const fixture = githubFixture();
    expect(await maintainDependabot(fixture.api, 101)).toMatchObject({
      outcome: "eligible",
      number: 140,
    });
    expect(fixture.writes).toEqual([]);
  });

  it("requests only a protected squash merge bound to the qualified head", async () => {
    const fixture = githubFixture();
    expect(await maintainDependabot(fixture.api, 101, true)).toMatchObject({
      outcome: "merged",
      dependencies: ["@types/node", "typescript-eslint"],
    });
    expect(fixture.writes).toEqual([
      {
        path: `${PREFIX}/pulls/140/merge`,
        body: {
          sha: HEAD,
          merge_method: "squash",
          commit_title: "chore(deps-dev): update routine tooling (#140)",
          commit_message:
            "Qualified by https://github.com/asadarafat/streamskope/actions/runs/101.",
        },
      },
    ]);
    expect(fixture.reads.filter((path) => path === `${PREFIX}/pulls/140`)).toHaveLength(2);
  });

  it.each([
    ["failed CI", "/actions/runs/101", { conclusion: "failure" }],
    ["cancelled CI", "/actions/runs/101", { conclusion: "cancelled" }],
    ["release qualification", "/actions/runs/101", { event: "workflow_dispatch" }],
    ["unrelated workflow", "/actions/runs/101", { path: ".github/workflows/other.yml" }],
    ["old head CI", "/actions/runs/101", { head_sha: "c".repeat(40) }],
    ["missing PR association", "/actions/runs/101", { pull_requests: [] }],
    ["human PR", "/pulls/140", { user: { login: "maintainer" } }],
    ["closed PR", "/pulls/140", { state: "closed" }],
    ["draft", "/pulls/140", { draft: true }],
    ["behind branch", "/pulls/140", { mergeable_state: "behind" }],
    ["mixed files", "/pulls/140", { changed_files: 3 }],
    [
      "fork",
      "/pulls/140",
      { head: { sha: HEAD, ref: "dependabot/npm/x", repo: { full_name: "other/streamskope" } } },
    ],
    ["changed main", "/git/ref/heads/main", { object: { sha: "c".repeat(40) } }],
    ["incomplete jobs", "/actions/runs/101/jobs?filter=latest&per_page=100", { total_count: 5 }],
  ])("does not mutate for %s", async (_name, path, override) => {
    const fixture = githubFixture();
    fixture.responses.set(`${PREFIX}${path}`, {
      ...(fixture.responses.get(`${PREFIX}${path}`) as Record<string, unknown>),
      ...override,
    });
    expect((await maintainDependabot(fixture.api, 101, true)).outcome).not.toBe("merged");
    expect(fixture.writes).toEqual([]);
  });

  it.each(["failure", "cancelled", "skipped", "neutral"])(
    "rejects a %s lane even if the run says success",
    async (conclusion) => {
      const fixture = githubFixture();
      const path = `${PREFIX}/actions/runs/101/jobs?filter=latest&per_page=100`;
      const jobs = fixture.responses.get(path) as { jobs: { conclusion: string }[] };
      jobs.jobs[0]!.conclusion = conclusion;
      expect((await maintainDependabot(fixture.api, 101, true)).outcome).toBe("manual");
      expect(fixture.writes).toEqual([]);
    },
  );

  it.each([
    [],
    [{ type: "pull_request" }],
    [
      { type: "pull_request" },
      {
        type: "required_status_checks",
        parameters: {
          strict_required_status_checks_policy: false,
          required_status_checks: [{ context: "CI", integration_id: 15368 }],
        },
      },
    ],
    [
      { type: "pull_request" },
      {
        type: "required_status_checks",
        parameters: {
          strict_required_status_checks_policy: true,
          required_status_checks: [{ context: "CI", integration_id: 999 }],
        },
      },
    ],
  ])("refuses weakened protection %#", async (...rules) => {
    const fixture = githubFixture();
    fixture.responses.set(`${PREFIX}/rules/branches/main`, rules);
    expect((await maintainDependabot(fixture.api, 101, true)).outcome).toBe("manual");
    expect(fixture.writes).toEqual([]);
  });

  it("does not merge a head replaced after classification", async () => {
    const fixture = githubFixture();
    let reads = 0;
    const api: GithubApi = async (path, body) => {
      const result = await fixture.api(path, body);
      if (path === `${PREFIX}/pulls/140` && ++reads === 2)
        return { ...(result as Record<string, unknown>), head: { sha: "c".repeat(40) } };
      return result;
    };
    expect((await maintainDependabot(api, 101, true)).outcome).toBe("deferred");
    expect(fixture.writes).toEqual([]);
  });

  it("does not merge a PR retargeted after classification", async () => {
    const fixture = githubFixture();
    let reads = 0;
    const api: GithubApi = async (path, body) => {
      const result = await fixture.api(path, body);
      if (path === `${PREFIX}/pulls/140` && ++reads === 2)
        return { ...(result as Record<string, unknown>), base: { sha: BASE, ref: "unprotected" } };
      return result;
    };
    expect((await maintainDependabot(api, 101, true)).outcome).toBe("deferred");
    expect(fixture.writes).toEqual([]);
  });

  it("does not read or execute PR sources outside the two dependency files", async () => {
    const fixture = githubFixture();
    fixture.responses.set(`${PREFIX}/pulls/140/files?per_page=100`, [
      { filename: "package.json", status: "modified" },
      { filename: ".github/workflows/ci.yml", status: "modified" },
    ]);
    expect((await maintainDependabot(fixture.api, 101, true)).outcome).toBe("manual");
    expect(fixture.writes).toEqual([]);
    expect(fixture.reads.some((path) => path.includes("/contents/"))).toBe(false);
  });

  it("preserves GitHub's refusal to merge instead of bypassing protection", async () => {
    const fixture = githubFixture();
    const api: GithubApi = (path, body) =>
      body ? Promise.resolve({ merged: false }) : fixture.api(path);
    expect((await maintainDependabot(api, 101, true)).outcome).toBe("deferred");
    expect(fixture.writes).toEqual([]);
  });

  it("fails without a write when a required API request fails", async () => {
    const fixture = githubFixture();
    const api: GithubApi = (path, body) => {
      if (path.includes("/rules/")) throw new Error("GitHub unavailable");
      return fixture.api(path, body);
    };
    await expect(maintainDependabot(api, 101, true)).rejects.toThrow("GitHub unavailable");
    expect(fixture.writes).toEqual([]);
  });
});
