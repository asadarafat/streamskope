import { isDeepStrictEqual } from "node:util";
import { pathToFileURL } from "node:url";

const REPOSITORY = "asadarafat/streamskope";
const DIRECT = new Set(["@types/node", "typescript-eslint", "prettier"]);
const ESLINT_PACKAGES = new Set(
  [
    "eslint-plugin",
    "parser",
    "project-service",
    "scope-manager",
    "tsconfig-utils",
    "type-utils",
    "types",
    "typescript-estree",
    "utils",
    "visitor-keys",
  ].map((name) => `@typescript-eslint/${name}`),
);

type Json = Record<string, unknown>;
export type GithubApi = (path: string, body?: Json) => Promise<unknown>;

function object(value: unknown): Json {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Expected a JSON object.");
  return value as Json;
}

function without(value: Json, keys: readonly string[]): Json {
  return Object.fromEntries(Object.entries(value).filter(([key]) => !keys.includes(key)));
}

function same(left: unknown, right: unknown, reason: string): void {
  if (!isDeepStrictEqual(left, right)) throw new Error(reason);
}

function version(value: unknown): [number, number, number] {
  if (typeof value !== "string" || !/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/u.test(value))
    throw new Error("Only exact stable versions are eligible.");
  const parts = value.split(".").map(Number);
  if (!parts.every(Number.isSafeInteger)) throw new Error("Invalid version.");
  return parts as [number, number, number];
}

function upgrade(name: string, before: unknown, after: unknown): void {
  const [major, minor, patch] = version(before);
  const [nextMajor, nextMinor, nextPatch] = version(after);
  const allowMinor = name === "@types/node" || name === "undici-types";
  if (
    major !== nextMajor ||
    (name === "@types/node" && major !== 24) ||
    (!allowMinor && minor !== nextMinor) ||
    !(nextMinor > minor || (nextMinor === minor && nextPatch > patch))
  )
    throw new Error(`${name} needs manual version review.`);
}

/** Deliberately narrow: new dependencies, package moves and unrelated transitive drift need review. */
export function assertRoutineDependencyUpdate(
  baseManifest: unknown,
  headManifest: unknown,
  baseLock: unknown,
  headLock: unknown,
): string[] {
  const before = object(baseManifest),
    after = object(headManifest);
  same(
    without(before, ["devDependencies"]),
    without(after, ["devDependencies"]),
    "Non-tooling manifest changes need review.",
  );
  const oldDev = object(before.devDependencies),
    newDev = object(after.devDependencies);
  same(
    Object.keys(oldDev).sort(),
    Object.keys(newDev).sort(),
    "Added or removed dependencies need review.",
  );
  const changed = Object.keys(oldDev).filter((name) => oldDev[name] !== newDev[name]);
  if (!changed.length) throw new Error("No routine tooling update.");
  for (const name of changed) {
    if (!DIRECT.has(name)) throw new Error(`${name} is outside the routine allowlist.`);
    upgrade(name, oldDev[name], newDev[name]);
  }

  const oldLock = object(baseLock),
    newLock = object(headLock);
  if (oldLock.lockfileVersion !== 3 || newLock.lockfileVersion !== 3)
    throw new Error("Only lockfile version 3 is eligible.");
  same(
    without(oldLock, ["packages"]),
    without(newLock, ["packages"]),
    "Lockfile metadata changes need review.",
  );
  const oldPackages = object(oldLock.packages),
    newPackages = object(newLock.packages);
  same(
    Object.keys(oldPackages).sort(),
    Object.keys(newPackages).sort(),
    "Added, removed or moved locked packages need review.",
  );
  const oldRoot = object(oldPackages[""]),
    newRoot = object(newPackages[""]);
  same(
    without(oldRoot, ["devDependencies"]),
    without(newRoot, ["devDependencies"]),
    "Lockfile root changes need review.",
  );
  same(oldRoot.devDependencies, oldDev, "Base manifest and lockfile disagree.");
  same(newRoot.devDependencies, newDev, "Updated manifest and lockfile disagree.");
  for (const name of changed) {
    same(
      object(oldPackages[`node_modules/${name}`]).version,
      oldDev[name],
      "Base direct version disagrees.",
    );
    same(
      object(newPackages[`node_modules/${name}`]).version,
      newDev[name],
      "Updated direct version disagrees.",
    );
  }
  for (const [path, value] of Object.entries(oldPackages)) {
    if (path === "" || isDeepStrictEqual(value, newPackages[path])) continue;
    const oldPackage = object(value),
      newPackage = object(newPackages[path]);
    const name = path.split("node_modules/").at(-1) ?? "";
    const allowed =
      changed.includes(name) ||
      (changed.includes("@types/node") && name === "undici-types") ||
      (changed.includes("typescript-eslint") && ESLINT_PACKAGES.has(name));
    if (
      !allowed ||
      oldPackage.dev !== true ||
      newPackage.dev !== true ||
      newPackage.hasInstallScript === true
    )
      throw new Error(`Locked package ${path} needs manual review.`);
    upgrade(name, oldPackage.version, newPackage.version);
    same(
      without(oldPackage, ["version", "resolved", "integrity", "dependencies", "peerDependencies"]),
      without(newPackage, ["version", "resolved", "integrity", "dependencies", "peerDependencies"]),
      `Package metadata changed for ${path}.`,
    );
    const archive = `${name.split("/").at(-1)}-${String(newPackage.version)}.tgz`;
    if (
      newPackage.resolved !== `https://registry.npmjs.org/${name}/-/${archive}` ||
      typeof newPackage.integrity !== "string" ||
      !/^sha512-[A-Za-z0-9+/]{86}==$/u.test(newPackage.integrity)
    )
      throw new Error(`Unreviewed registry or integrity for ${path}.`);
  }
  return changed.sort();
}

interface Decision {
  outcome: "manual" | "eligible" | "merged" | "deferred";
  reason: string;
  number?: number;
  dependencies?: string[];
}

/** Only trusted main invokes this after CI. PR files are parsed as data, never executed. */
export async function maintainDependabot(
  api: GithubApi,
  runId: number,
  apply = false,
): Promise<Decision> {
  if (!Number.isSafeInteger(runId) || runId < 1) throw new Error("Invalid CI run ID.");
  const prefix = `/repos/${REPOSITORY}`;
  const run = object(await api(`${prefix}/actions/runs/${runId}`));
  const pulls = run.pull_requests;
  if (
    run.event !== "pull_request" ||
    run.path !== ".github/workflows/ci.yml" ||
    run.status !== "completed" ||
    run.conclusion !== "success" ||
    !Array.isArray(pulls) ||
    pulls.length !== 1
  )
    return { outcome: "manual", reason: "A successful ordinary PR CI run is required." };
  const number = object(pulls[0]).number;
  if (!Number.isSafeInteger(number) || Number(number) < 1) throw new Error("Invalid PR number.");
  const prPath = `${prefix}/pulls/${String(number)}`;
  const pr = object(await api(prPath));
  const head = object(pr.head),
    base = object(pr.base);
  if (
    pr.state !== "open" ||
    pr.draft !== false ||
    object(pr.user).login !== "dependabot[bot]" ||
    object(head.repo).full_name !== REPOSITORY ||
    object(base.repo).full_name !== REPOSITORY ||
    base.ref !== "main" ||
    typeof head.ref !== "string" ||
    !head.ref.startsWith("dependabot/") ||
    typeof head.sha !== "string" ||
    !/^[a-f0-9]{40}$/u.test(head.sha) ||
    run.head_sha !== head.sha
  )
    return { outcome: "manual", reason: "PR identity or current head is not eligible." };
  const result = { number: Number(number) };
  const currentBase = object(object(await api(`${prefix}/git/ref/heads/main`)).object).sha;
  if (base.sha !== currentBase || pr.mergeable_state !== "clean")
    return {
      ...result,
      outcome: "deferred",
      reason: "PR must be current and mergeable under main protection.",
    };
  const rules = await api(`${prefix}/rules/branches/main`);
  if (
    !Array.isArray(rules) ||
    !rules.some((rule: unknown) => object(rule).type === "pull_request") ||
    !rules.some((rule: unknown) => {
      const data = object(rule);
      if (data.type !== "required_status_checks") return false;
      const parameters = object(data.parameters);
      return (
        parameters.strict_required_status_checks_policy === true &&
        Array.isArray(parameters.required_status_checks) &&
        parameters.required_status_checks.some((check: unknown) => {
          const entry = object(check);
          return entry.context === "CI" && entry.integration_id === 15368;
        })
      );
    })
  )
    return {
      ...result,
      outcome: "manual",
      reason: "Protected PRs and strict GitHub Actions CI are required.",
    };
  const jobs = object(await api(`${prefix}/actions/runs/${runId}/jobs?filter=latest&per_page=100`));
  const required = [
    "Static, unit and integration",
    "Documentation",
    "Real providers and browser",
    "CI",
  ];
  if (
    !Array.isArray(jobs.jobs) ||
    jobs.total_count !== jobs.jobs.length ||
    !required.every((name) => {
      const matching = (jobs.jobs as unknown[]).map(object).filter((job) => job.name === name);
      return (
        matching.length === 1 &&
        matching[0]?.status === "completed" &&
        matching[0]?.conclusion === "success"
      );
    })
  )
    return {
      ...result,
      outcome: "manual",
      reason: "All three lanes and the CI evidence gate must pass.",
    };

  const files = await api(`${prPath}/files?per_page=100`);
  if (
    pr.changed_files !== 2 ||
    !Array.isArray(files) ||
    files.length !== 2 ||
    !["package.json", "package-lock.json"].every((name) =>
      files.some((file: unknown) => {
        const entry = object(file);
        return entry.filename === name && entry.status === "modified" && !entry.previous_filename;
      }),
    )
  )
    return {
      ...result,
      outcome: "manual",
      reason: "Only root manifest and lockfile updates are eligible.",
    };
  const readJson = async (name: string, sha: string): Promise<unknown> => {
    const file = object(await api(`${prefix}/contents/${name}?ref=${sha}`));
    if (file.type !== "file" || file.encoding !== "base64" || typeof file.content !== "string")
      throw new Error("Expected a complete regular JSON file.");
    return JSON.parse(Buffer.from(file.content, "base64").toString("utf8")) as unknown;
  };
  const [oldManifest, newManifest, oldLock, newLock] = await Promise.all([
    readJson("package.json", String(currentBase)),
    readJson("package.json", head.sha),
    readJson("package-lock.json", String(currentBase)),
    readJson("package-lock.json", head.sha),
  ]);
  let dependencies: string[];
  try {
    dependencies = assertRoutineDependencyUpdate(oldManifest, newManifest, oldLock, newLock);
  } catch (error) {
    return {
      ...result,
      outcome: "manual",
      reason: error instanceof Error ? error.message : "Dependency review required.",
    };
  }
  if (!apply)
    return {
      ...result,
      dependencies,
      outcome: "eligible",
      reason: "Routine tooling; protected CI passed. Read-only check.",
    };
  // Re-read after classification. The merge API also checks the head SHA and current branch rules atomically.
  const latest = object(await api(prPath));
  if (
    latest.state !== "open" ||
    latest.draft !== false ||
    object(latest.base).ref !== "main" ||
    object(latest.head).sha !== head.sha ||
    object(latest.base).sha !== currentBase ||
    latest.mergeable_state !== "clean"
  )
    return {
      ...result,
      dependencies,
      outcome: "deferred",
      reason: "PR changed during qualification.",
    };
  const merged = object(
    await api(`${prPath}/merge`, {
      sha: head.sha,
      merge_method: "squash",
      commit_title: `chore(deps-dev): update routine tooling (#${String(number)})`,
      commit_message: `Qualified by https://github.com/${REPOSITORY}/actions/runs/${runId}.`,
    }),
  );
  return {
    ...result,
    dependencies,
    outcome: merged.merged === true ? "merged" : "deferred",
    reason: "Protected merge API outcome.",
  };
}

async function main(): Promise<void> {
  const [mode, id, ...extra] = process.argv.slice(2);
  if (!["--check", "--apply"].includes(mode ?? "") || !/^\d+$/u.test(id ?? "") || extra.length)
    throw new Error("Usage: node tools/check/dependabot.ts <--check|--apply> <CI-run-id>");
  const token = process.env.GH_TOKEN;
  if (!token || process.env.GITHUB_REPOSITORY !== REPOSITORY)
    throw new Error("GH_TOKEN and the expected GITHUB_REPOSITORY are required.");
  const api: GithubApi = async (path, body) => {
    const response = await fetch(`https://api.github.com${path}`, {
      method: body ? "PUT" : "GET",
      redirect: "error",
      signal: AbortSignal.timeout(30_000),
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        ...(body ? { "Content-Type": "application/json" } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (body && (response.status === 405 || response.status === 409)) return { merged: false };
    if (!response.ok)
      throw new Error(
        `GitHub API returned ${response.status}; no qualification bypass is attempted.`,
      );
    return response.json();
  };
  const decision = await maintainDependabot(api, Number(id), mode === "--apply");
  process.stdout.write(`${JSON.stringify(decision)}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  void main().catch((error: unknown) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : "Dependency maintenance failed."}\n`,
    );
    process.exitCode = 1;
  });
