import { execFile } from "node:child_process";
import { appendFile, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { format, resolveConfig } from "prettier";

import { githubReader, type GithubRead } from "./release-changelog";
import {
  applyReleaseReconciliation,
  checkReleaseReconciliation,
  planReleaseReconciliation,
  type ReconciliationPlan,
} from "./release-reconciliation";
import type { ReleaseComponent } from "./release-version";

const execute = promisify(execFile);
const SHA = /^[a-f0-9]{40}$/u;
const REPOSITORY = /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\/[A-Za-z0-9_][A-Za-z0-9_.-]*$/u;
const VERSION =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][\dA-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][\dA-Za-z-]*))*))?(?:\+[\dA-Za-z-]+(?:\.[\dA-Za-z-]+)*)?$/u;
export const RELEASE_DOCS_BRANCH = "automation/release-docs";
export const RELEASE_DOCS_MARKER = "<!-- streamskope-release-documentation -->";
const TITLE = "docs(release): synchronize published release records";
const BOT_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com";
const LABELS = ["release-notes:skip", "component:shared"];
export type GithubWrite = (
  method: "POST" | "PATCH" | "PUT",
  path: string,
  body: unknown,
) => Promise<unknown>;
export type QualificationUpdater = (
  root: string,
  plan: ReconciliationPlan,
  check?: boolean,
) => Promise<void>;
export interface ReleaseDocumentationOptions {
  root: string;
  repository: string;
  readGithub: GithubRead;
  writeGithub: GithubWrite;
  updateQualification?: QualificationUpdater;
  expectedPublication?: { tag: string; id: number };
  pause?: (milliseconds: number) => Promise<void>;
  automaticCi?: boolean;
  now?: () => number;
  /** Refresh remote refs only; the finalizer guards and switches the owned checkout. */
  refreshMain?: () => Promise<void>;
}
export interface ReleaseDocumentationResult {
  status: "unchanged" | "pull-request";
  pullRequest?: number;
  headSha?: string;
  ciAction: "approval-required" | "existing" | "none";
  ciRun?: CiRunIdentity;
  autoMergeEnabled: boolean;
  warnings: string[];
}
export interface ReleaseDocumentationFinalization extends ReleaseDocumentationResult {
  finalization:
    "unchanged" | "merged" | "approval-required" | "review-required" | "awaiting-maintainer";
  mergeSha?: string;
}
export interface ReleaseDocumentationValidation {
  repository: string;
  sourceSha: string;
  pullRequest: number;
  readGithub: GithubRead;
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("GitHub returned malformed release documentation metadata.");
  return value as Record<string, unknown>;
}
async function git(root: string, ...args: string[]): Promise<string> {
  return (
    await execute("git", args, { cwd: root, maxBuffer: 8 * 1024 ** 2, timeout: 60_000 })
  ).stdout.trimEnd();
}
async function cleanCheckout(root: string): Promise<boolean> {
  return (
    !(await git(root, "status", "--porcelain", "--untracked-files=all")) &&
    !(await git(root, "ls-files", "-v", "-z")).split("\0").some((entry) => /^[a-zS]/u.test(entry))
  );
}
async function pages(read: GithubRead, path: string, field?: string): Promise<unknown[]> {
  const values: unknown[] = [];
  const seen = new Set<string>();
  for (let page = 1; page <= 10_000; page++) {
    const result = await read(`${path}${path.includes("?") ? "&" : "?"}per_page=100&page=${page}`);
    const data = field ? object(result)[field] : result;
    if (!Array.isArray(data) || data.length > 100)
      throw new Error("GitHub returned an invalid release documentation page.");
    const signature = JSON.stringify(data);
    if (data.length === 100 && seen.has(signature))
      throw new Error(
        "GitHub repeated a release documentation page; refusing incomplete evidence.",
      );
    seen.add(signature);
    values.push(...(data as unknown[]));
    if (data.length < 100) return values;
  }
  throw new Error("Release documentation pagination exceeded its safety limit.");
}

export function allowedReleaseDocumentationPath(path: string): boolean {
  if (
    [
      "website/zensical.toml",
      "website/docs/releases/index.md",
      "website/docs/releases/unreleased.md",
      "website/docs/guide/qualification.md",
      "plugins/eda/RELEASE_NOTES.md",
      "plugins/nsp/RELEASE_NOTES.md",
    ].includes(path)
  )
    return true;
  const version = /^website\/docs\/releases\/(?:plugins\/(?:eda|nsp)\/)?v([^/]+)\.md$/u.exec(
    path,
  )?.[1];
  return version !== undefined && VERSION.test(version);
}
function ownedPull(
  value: unknown,
  repository: string,
  expectedSha?: string,
  allowMerged = false,
): Record<string, unknown> {
  const pull = object(value);
  const head = object(pull.head);
  const base = object(pull.base);
  const names = Array.isArray(pull.labels) ? pull.labels.map((item) => object(item).name) : [];
  if (
    !Number.isSafeInteger(pull.number) ||
    (pull.number as number) <= 0 ||
    (pull.state !== "open" &&
      !(
        allowMerged &&
        pull.state === "closed" &&
        pull.merged === true &&
        typeof pull.merge_commit_sha === "string" &&
        SHA.test(pull.merge_commit_sha)
      )) ||
    (pull.state === "open" && pull.merged === true) ||
    base.ref !== "main" ||
    head.ref !== RELEASE_DOCS_BRANCH ||
    object(base.repo).full_name !== repository ||
    object(head.repo).full_name !== repository ||
    typeof pull.body !== "string" ||
    !pull.body.includes(RELEASE_DOCS_MARKER) ||
    pull.title !== TITLE ||
    !LABELS.every((label) => names.includes(label)) ||
    typeof head.sha !== "string" ||
    !SHA.test(head.sha) ||
    (expectedSha !== undefined && head.sha !== expectedSha)
  )
    throw new Error(
      "Release documentation PR is foreign, closed or changed; refusing managed automation.",
    );
  return pull;
}

/** Verify the one owned PR before relying on normal pull_request CI. */
export async function validateReleaseDocumentationPullRequest(
  options: ReleaseDocumentationValidation,
): Promise<void> {
  if (
    !REPOSITORY.test(options.repository) ||
    !SHA.test(options.sourceSha) ||
    !Number.isSafeInteger(options.pullRequest) ||
    options.pullRequest <= 0
  )
    throw new Error("Release documentation validation needs its exact SHA and PR number.");
  const path = `/repos/${options.repository}/pulls/${options.pullRequest}`;
  const pull = ownedPull(await options.readGithub(path), options.repository, options.sourceSha);
  if (pull.number !== options.pullRequest)
    throw new Error("GitHub returned the wrong release documentation PR.");
  await validateChangedPaths(options.readGithub, path, pull);
}

async function validateChangedPaths(
  read: GithubRead,
  path: string,
  pull: Record<string, unknown>,
): Promise<void> {
  const files = await pages(read, `${path}/files`);
  if (!files.length) throw new Error("Release documentation PR has no changed files.");
  const seen = new Set<string>();
  for (const item of files) {
    const file = object(item);
    if (
      typeof file.filename !== "string" ||
      !allowedReleaseDocumentationPath(file.filename) ||
      seen.has(file.filename) ||
      (file.previous_filename !== undefined &&
        (typeof file.previous_filename !== "string" ||
          !allowedReleaseDocumentationPath(file.previous_filename)))
    )
      throw new Error("Release documentation PR contains unexpected or duplicate changed paths.");
    seen.add(file.filename);
  }
  if (!Number.isSafeInteger(pull.changed_files) || pull.changed_files !== files.length)
    throw new Error("Release documentation changed-file evidence is incomplete.");
}

async function updateQualification(
  root: string,
  plan: ReconciliationPlan,
  check = false,
): Promise<void> {
  await new Promise<void>((accept, reject) => {
    const child = execFile(
      "python3",
      [
        "-c",
        "import json,sys;sys.path.insert(0,'tools/docs');from publication import archived_qualification;archived_qualification(sys.argv[1],json.load(sys.stdin),check=sys.argv[2]=='true')",
        root,
        String(check),
      ],
      { cwd: root, maxBuffer: 2 * 1024 ** 2, timeout: 30_000 },
      (error, _stdout, stderr) => {
        if (error)
          reject(
            new Error(
              `Release qualification reconciliation failed: ${stderr.trim() || error.message}`,
            ),
          );
        else accept();
      },
    );
    child.stdin?.end(JSON.stringify(plan));
  });
}

async function remoteHead(root: string): Promise<string | null> {
  const result = await git(
    root,
    "ls-remote",
    "--heads",
    "origin",
    `refs/heads/${RELEASE_DOCS_BRANCH}`,
  );
  if (!result) return null;
  const records = result.split("\n");
  const [sha, ref] = records[0]!.split("\t");
  if (records.length !== 1 || !sha || !SHA.test(sha) || ref !== `refs/heads/${RELEASE_DOCS_BRANCH}`)
    throw new Error("Managed documentation branch has invalid remote evidence.");
  return sha;
}
async function ownedBranch(root: string, head: string, source: string): Promise<void> {
  await git(root, "fetch", "--no-tags", "origin", `refs/heads/${RELEASE_DOCS_BRANCH}`);
  if ((await git(root, "rev-parse", "FETCH_HEAD")) !== head)
    throw new Error("Managed documentation branch changed while reading; retry.");
  const metadata = (await git(root, "show", "-s", "--format=%ae%n%ce%n%s%n%b", head)).split("\n");
  const parents = (await git(root, "show", "-s", "--format=%P", head)).split(" ");
  if (
    metadata[0] !== BOT_EMAIL ||
    metadata[1] !== BOT_EMAIL ||
    metadata[2] !== TITLE ||
    !metadata.slice(3).join("\n").includes(RELEASE_DOCS_MARKER) ||
    parents.length !== 1 ||
    !["0", "1"].includes(await git(root, "rev-list", "--count", `${source}..${head}`))
  )
    throw new Error(
      "Managed documentation branch was edited outside automation; preserve it for review.",
    );
  await git(root, "merge-base", "--is-ancestor", parents[0]!, source);
}
async function autoMerge(
  write: GithubWrite,
  repository: string,
  pull: Record<string, unknown>,
  enabled: boolean,
): Promise<void> {
  if (typeof pull.node_id !== "string" || !pull.node_id)
    throw new Error("Release documentation PR needs a GraphQL identity.");
  const mutation = enabled ? "enablePullRequestAutoMerge" : "disablePullRequestAutoMerge";
  if (!enabled && !pull.auto_merge) return;
  if (enabled && pull.auto_merge) return;
  // GitHub rejects auto-merge for an already mergeable PR. The ordinary merge
  // endpoint still enforces branch protection and pins the qualified head.
  if (enabled && pull.mergeable_state === "clean") {
    const head = object(pull.head);
    if (typeof head.sha !== "string" || !SHA.test(head.sha))
      throw new Error("Release documentation merge needs an exact head identity.");
    const merged = object(
      await write("PUT", `/repos/${repository}/pulls/${String(pull.number)}/merge`, {
        sha: head.sha,
        merge_method: "squash",
        commit_title: TITLE,
      }),
    );
    if (merged.merged !== true || typeof merged.sha !== "string" || !SHA.test(merged.sha))
      throw new Error("GitHub did not confirm the protected release documentation merge.");
    return;
  }
  const result = object(
    await write("POST", "/graphql", {
      query: `mutation($id:ID!){${mutation}(input:{pullRequestId:$id${enabled ? ",mergeMethod:SQUASH" : ""}}){pullRequest{number}}}`,
      variables: { id: pull.node_id },
    }),
  );
  if (result.errors !== undefined)
    throw new Error("GitHub refused release documentation auto-merge.");
  const operation = object(object(result.data)[mutation]);
  if (object(operation.pullRequest).number !== pull.number)
    throw new Error("GitHub returned the wrong auto-merge PR.");
}

/** Apply exact publication records, then qualify one owned PR; never bypass branch protection. */
export async function reconcileReleaseDocumentation(
  options: ReleaseDocumentationOptions,
): Promise<ReleaseDocumentationResult> {
  const { root, repository, readGithub, writeGithub } = options;
  if (
    !REPOSITORY.test(repository) ||
    (await git(root, "branch", "--show-current")) !== "main" ||
    !(await cleanCheckout(root))
  )
    throw new Error("Release documentation reconciliation requires a clean main checkout.");
  const plan = await planReleaseReconciliation(options);
  if (
    options.expectedPublication &&
    !plan.published.some(
      (item) =>
        item.tag === options.expectedPublication!.tag &&
        item.release.id === options.expectedPublication!.id,
    )
  )
    throw new Error(
      "The triggering publication is not visible in immutable release records yet; retry finalization.",
    );
  const pulls = await pages(
    readGithub,
    `/repos/${repository}/pulls?state=open&head=${encodeURIComponent(`${repository.split("/")[0]}:${RELEASE_DOCS_BRANCH}`)}&base=main`,
  );
  if (pulls.length > 1) throw new Error("More than one managed release documentation PR exists.");
  const existing = pulls[0] === undefined ? null : ownedPull(pulls[0], repository);
  const previousHead = await remoteHead(root);
  if (existing && object(existing.head).sha !== previousHead)
    throw new Error("Managed PR and remote branch disagree; retry without overwriting.");
  if (previousHead) await ownedBranch(root, previousHead, plan.sourceSha);
  // A retry must withdraw old merge permission before publishing a head needing human review.
  if (existing && plan.warnings.length) await autoMerge(writeGithub, repository, existing, false);
  await applyReleaseReconciliation(plan, root);
  await (options.updateQualification ?? updateQualification)(root, plan);
  const changed = [
    ...new Set(
      [
        ...(await git(root, "diff", "--no-renames", "--name-only", "-z")).split("\0"),
        ...(await git(root, "ls-files", "--others", "--exclude-standard", "-z")).split("\0"),
      ].filter(Boolean),
    ),
  ].sort();
  if (changed.some((path) => !allowedReleaseDocumentationPath(path)))
    throw new Error("Reconciliation changed an unexpected path; refusing to stage it.");
  // Format editable pages for normal PR CI; archived publication bodies are exact.
  for (const path of changed) {
    if (
      !path.endsWith(".md") ||
      /^website\/docs\/releases\/(?:plugins\/(?:eda|nsp)\/)?v[^/]+\.md$/u.test(path)
    )
      continue;
    const file = resolve(root, path);
    const contents = await readFile(file, "utf8");
    const formatted = await format(contents, { ...(await resolveConfig(file)), filepath: file });
    if (formatted !== contents) await writeFile(file, formatted, "utf8");
  }
  if (!changed.length) {
    if (existing) {
      await autoMerge(writeGithub, repository, existing, false);
      await writeGithub("PATCH", `/repos/${repository}/pulls/${String(existing.number)}`, {
        state: "closed",
      });
    }
    return {
      status: "unchanged",
      ciAction: "none",
      autoMergeEnabled: false,
      warnings: plan.warnings,
    };
  }
  await git(root, "add", "--", ...changed);
  const tree = await git(root, "write-tree");
  const reuse =
    previousHead !== null &&
    (await git(root, "show", "-s", "--format=%P", previousHead)) === plan.sourceSha &&
    (await git(root, "rev-parse", `${previousHead}^{tree}`)) === tree;
  await git(root, "checkout", "-B", RELEASE_DOCS_BRANCH, reuse ? previousHead : plan.sourceSha);
  if (!reuse) {
    await git(
      root,
      "-c",
      "user.name=github-actions[bot]",
      "-c",
      `user.email=${BOT_EMAIL}`,
      "commit",
      "-m",
      TITLE,
      "-m",
      RELEASE_DOCS_MARKER,
    );
    await git(
      root,
      "push",
      `--force-with-lease=refs/heads/${RELEASE_DOCS_BRANCH}:${previousHead ?? ""}`,
      "origin",
      `HEAD:refs/heads/${RELEASE_DOCS_BRANCH}`,
    );
  }
  const headSha = await git(root, "rev-parse", "HEAD");
  const body = [
    RELEASE_DOCS_MARKER,
    "",
    "Archive authoritative immutable release records and synchronize published documentation without assigning development versions.",
    "",
    `Source: ${plan.sourceSha}`,
    "",
    "Validation: normal pull request CI qualifies this exact documentation head; automatic merging remains subject to required checks and an up-to-date branch.",
    "",
    ...plan.warnings.map((warning) => `Review required: ${warning}`),
  ].join("\n");
  const edited = object(
    await writeGithub(
      existing ? "PATCH" : "POST",
      `/repos/${repository}/pulls${existing ? `/${String(existing.number)}` : ""}`,
      existing
        ? { title: TITLE, body }
        : { title: TITLE, body, base: "main", head: RELEASE_DOCS_BRANCH },
    ),
  );
  if (!Number.isSafeInteger(edited.number) || (edited.number as number) <= 0)
    throw new Error("GitHub returned an invalid release documentation PR number.");
  const number = edited.number as number;
  await writeGithub("POST", `/repos/${repository}/issues/${number}/labels`, { labels: LABELS });
  await validateReleaseDocumentationPullRequest({
    repository,
    sourceSha: headSha,
    pullRequest: number,
    readGithub,
  });
  const ci = await ensurePullRequestCi(options, number, headSha);
  const autoMergeEnabled =
    options.automaticCi === true && plan.warnings.length === 0 && ci.action !== "approval-required";
  const pull = ownedPull(
    await readGithub(`/repos/${repository}/pulls/${number}`),
    repository,
    headSha,
  );
  await autoMerge(writeGithub, repository, pull, autoMergeEnabled);
  return {
    status: "pull-request",
    pullRequest: number,
    headSha,
    ciAction: ci.action,
    ...(ci.run ? { ciRun: ci.run } : {}),
    autoMergeEnabled,
    warnings: plan.warnings,
  };
}

interface CiRunIdentity {
  id: number;
  workflowId: number;
  sourceSha: string;
}
interface CiEvidence {
  action: "approval-required" | "existing";
  run?: CiRunIdentity;
  passed: boolean;
}

async function ensurePullRequestCi(
  options: ReleaseDocumentationOptions,
  number: number,
  headSha: string,
  observedPull?: Record<string, unknown>,
  previousRun?: CiRunIdentity,
): Promise<CiEvidence> {
  const { repository, readGithub } = options;
  const now = options.now ?? Date.now;
  const deadline = now() + 90_000;
  for (let attempt = 0; attempt < 45; attempt++) {
    const currentPull =
      observedPull ??
      ownedPull(await readGithub(`/repos/${repository}/pulls/${number}`), repository, headSha);
    const exactRunSource = (value: unknown): boolean =>
      value === headSha ||
      (typeof currentPull.merge_commit_sha === "string" &&
        SHA.test(currentPull.merge_commit_sha) &&
        value === currentPull.merge_commit_sha);
    const runs =
      currentPull.merged === true
        ? []
        : (
            await pages(
              readGithub,
              `/repos/${repository}/actions/workflows/ci.yml/runs?event=pull_request&branch=${encodeURIComponent(RELEASE_DOCS_BRANCH)}`,
              "workflow_runs",
            )
          )
            .map(object)
            .filter(
              (run) =>
                run.event === "pull_request" &&
                run.head_branch === RELEASE_DOCS_BRANCH &&
                exactRunSource(run.head_sha) &&
                Array.isArray(run.pull_requests) &&
                run.pull_requests.some((value) => {
                  const associated = object(value);
                  const head = object(associated.head);
                  return (
                    associated.number === number &&
                    head.sha === headSha &&
                    head.ref === RELEASE_DOCS_BRANCH &&
                    object(associated.base).ref === "main"
                  );
                }),
            )
            .sort((left, right) => Number(right.id) - Number(left.id));
    // GitHub can clear pull_requests after merge or branch deletion. Only the
    // run already associated with this open PR can bridge that transition.
    let run = runs[0];
    if (currentPull.merged === true) {
      if (!previousRun) throw new Error("Merged documentation has no previously owned CI run.");
      run = object(await readGithub(`/repos/${repository}/actions/runs/${previousRun.id}`));
      if (
        run.id !== previousRun.id ||
        run.workflow_id !== previousRun.workflowId ||
        run.head_sha !== previousRun.sourceSha ||
        run.event !== "pull_request" ||
        run.head_branch !== RELEASE_DOCS_BRANCH
      )
        throw new Error("Previously owned CI run identity changed after merge.");
    }
    if (!run) {
      if (!options.automaticCi) return { action: "approval-required", passed: false };
      if (observedPull)
        throw new Error(
          "Normal PR CI disappeared for the owned head; finalization remains incomplete.",
        );
      if (attempt < 44 && now() < deadline) {
        await (
          options.pause ??
          ((milliseconds): Promise<void> =>
            new Promise((accept) => setTimeout(accept, milliseconds)))
        )(Math.min(2000, deadline - now()));
        continue;
      }
      throw new Error(
        "Normal pull request CI has not appeared for the owned head. Finalization remains pending; rerun or approve its CI in GitHub.",
      );
    }
    if (
      !Number.isSafeInteger(run.workflow_id) ||
      (run.workflow_id as number) <= 0 ||
      !Number.isSafeInteger(run.id) ||
      (run.id as number) <= 0 ||
      typeof run.head_sha !== "string" ||
      !SHA.test(run.head_sha)
    )
      throw new Error("Normal PR CI has incomplete run identity; refusing to use it.");
    if (
      typeof run.status !== "string" ||
      ![
        "queued",
        "in_progress",
        "completed",
        "waiting",
        "pending",
        "requested",
        "action_required",
      ].includes(String(run.status)) ||
      (run.status === "completed" &&
        (typeof run.conclusion !== "string" ||
          ![
            "success",
            "failure",
            "cancelled",
            "timed_out",
            "action_required",
            "neutral",
            "skipped",
            "stale",
            "startup_failure",
          ].includes(run.conclusion)))
    )
      throw new Error("Normal PR CI has an invalid status; refusing to treat it as qualification.");
    if (
      run.status !== "completed" &&
      run.conclusion !== null &&
      run.conclusion !== undefined &&
      !(run.status === "action_required" && run.conclusion === "action_required")
    )
      throw new Error("Normal PR CI has an inconsistent conclusion; refusing qualification.");
    const approval = run.conclusion === "action_required" || run.status === "action_required";
    if (approval) return { action: "approval-required", passed: false };
    if (run.status === "completed" && run.conclusion !== "success")
      throw new Error(
        `Normal PR CI ended with ${String(run.conclusion)}; inspect the owned PR before retrying finalization.`,
      );
    return {
      action: "existing",
      run: { id: run.id as number, workflowId: run.workflow_id as number, sourceSha: run.head_sha },
      passed: run.status === "completed",
    };
  }
  throw new Error("Normal pull request CI finalization did not complete.");
}

async function refreshOwnedMain(options: ReleaseDocumentationOptions, head: string): Promise<void> {
  const { root } = options;
  const assertOwned = async (): Promise<void> => {
    if (
      !(await cleanCheckout(root)) ||
      (await git(root, "branch", "--show-current")) !== RELEASE_DOCS_BRANCH ||
      (await git(root, "rev-parse", "HEAD")) !== head
    )
      throw new Error("Release finalization refresh requires the exact clean owned checkout.");
  };
  await assertOwned();
  if (options.refreshMain) await options.refreshMain();
  else await git(root, "fetch", "origin", "main", "--tags");
  await assertOwned();
  const next = await git(root, "rev-parse", "refs/remotes/origin/main");
  if (!SHA.test(next)) throw new Error("Release finalization refresh has invalid main identity.");
  await git(root, "merge-base", "--is-ancestor", "main", next);
  // This Node process already loaded these inputs. A fresh workflow invocation
  // must install and load a changed toolchain; resetting files cannot reload it.
  const paths = (await git(root, "diff", "--name-only", "-z", head, next)).split("\0");
  if (
    paths.some((path) =>
      /^(?:tools\/|src\/|config\/|\.github\/workflows\/|package(?:-lock)?\.json$|\.npmrc$|\.node-version$|\.nvmrc$|\.prettier|prettier\.config\.|website\/requirements\.txt$)/u.test(
        path,
      ),
    )
  )
    throw new Error(
      "Release finalization toolchain or dependency inputs changed; start a fresh invocation from main.",
    );
  await git(root, "switch", "main");
  await git(root, "reset", "--hard", next);
}

/** A successful handoff is pending work; only observed merge or synchronized main is complete. */
export async function finalizeReleaseDocumentation(
  options: ReleaseDocumentationOptions,
): Promise<ReleaseDocumentationFinalization> {
  const now = options.now ?? Date.now;
  const pause =
    options.pause ??
    ((milliseconds: number): Promise<void> =>
      new Promise((accept) => setTimeout(accept, milliseconds)));
  const deadline = now() + 25 * 60_000;
  let result = await reconcileReleaseDocumentation(options);
  for (let attempt = 0; attempt < 150; attempt++) {
    if (result.status === "unchanged") return { ...result, finalization: "unchanged" };
    const number = result.pullRequest!;
    const head = result.headSha!;
    const path = `/repos/${options.repository}/pulls/${number}`;
    const pull = ownedPull(await options.readGithub(path), options.repository, head, true);
    if (pull.number !== number) throw new Error("GitHub returned the wrong finalization PR.");
    await validateChangedPaths(options.readGithub, path, pull);
    const ci = await ensurePullRequestCi(options, number, head, pull, result.ciRun);
    result = {
      ...result,
      ciAction: ci.action,
      ...(ci.run ? { ciRun: ci.run } : {}),
    };
    if (pull.merged === true) {
      if (!ci.passed)
        throw new Error("Merged release documentation has no successful exact-head CI evidence.");
      return { ...result, finalization: "merged", mergeSha: String(pull.merge_commit_sha) };
    }
    const handoff = result.warnings.length
      ? "review-required"
      : ci.action === "approval-required"
        ? "approval-required"
        : !options.automaticCi
          ? "awaiting-maintainer"
          : null;
    if (handoff) {
      await autoMerge(options.writeGithub, options.repository, pull, false);
      return { ...result, autoMergeEnabled: false, finalization: handoff };
    }
    if (attempt === 149 || now() >= deadline)
      throw new Error(
        "Release documentation is still pending; bounded finalization ended without a confirmed merge.",
      );
    if (pull.mergeable_state === "behind") {
      await autoMerge(options.writeGithub, options.repository, pull, false);
      await refreshOwnedMain(options, head);
      // Re-read exact ownership after fetching, before publishing any replacement.
      await validateReleaseDocumentationPullRequest({
        repository: options.repository,
        sourceSha: head,
        pullRequest: number,
        readGithub: options.readGithub,
      });
      result = await reconcileReleaseDocumentation(options);
      continue;
    }
    if (!result.autoMergeEnabled)
      throw new Error("Release finalization has no protected merge request; inspect the owned PR.");
    await pause(Math.min(10_000, Math.max(0, deadline - now())));
  }
  throw new Error("Release documentation finalization exceeded its attempt limit.");
}

/** Persist precise status and the legacy flag that stops older workflow pollers. */
export async function writeReleaseDocumentationFinalization(
  result: ReleaseDocumentationFinalization,
  repository: string,
  paths: { output?: string | undefined; summary?: string | undefined },
): Promise<string> {
  if (paths.output)
    await appendFile(
      paths.output,
      `finalization_status=${result.finalization}\narchive_complete=${["unchanged", "merged"].includes(result.finalization)}\npull_request=${result.pullRequest ?? ""}\nhead_sha=${result.headSha ?? ""}\nreview_required=${!["unchanged", "merged"].includes(result.finalization)}\napproval_required=${result.finalization === "approval-required"}\n`,
      "utf8",
    );
  const descriptions: Record<ReleaseDocumentationFinalization["finalization"], string> = {
    unchanged: "Published release documentation is already synchronized.",
    merged: `Release documentation merged at ${result.mergeSha ?? ""} after required CI.`,
    "approval-required":
      "Archive incomplete: approve normal PR CI in GitHub, then review and merge the owned PR. Configure the release GitHub App for automatic finalization.",
    "review-required":
      "Archive incomplete: review the preserved commentary and merge the owned PR after required CI passes.",
    "awaiting-maintainer":
      "Archive incomplete: normal PR CI is active or successful. Wait for its required checks, then merge the owned PR.",
  };
  const summary = `${result.pullRequest ? `Release documentation PR: https://github.com/${repository}/pull/${result.pullRequest}\nCI head: ${result.headSha}\n` : ""}${descriptions[result.finalization]}\n${result.warnings.length ? `Advisory: ${result.warnings.join(" ")}\n` : ""}`;

  if (paths.summary) await appendFile(paths.summary, summary, "utf8");
  return summary;
}

export function githubWriter(token: string, apiUrl = "https://api.github.com"): GithubWrite {
  // Reuse endpoint validation before a credential can be transmitted.
  githubReader(token, apiUrl);
  const api = new URL(apiUrl).toString().replace(/\/$/u, "");
  return async (method, path, body) => {
    const response = await fetch(`${api}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify(body),
      redirect: "error",
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok)
      throw new Error(`GitHub ${method} returned HTTP ${response.status} for ${path}.`);
    return response.status === 204 ? null : response.json();
  };
}
async function main(): Promise<void> {
  const [mode, component, ...extra] = process.argv.slice(2);
  if (
    !["reconcile", "validate-pr", "check"].includes(mode ?? "") ||
    extra.length ||
    (mode !== "check" && component !== undefined) ||
    (mode === "check" && !["desktop", "eda", "nsp"].includes(component ?? ""))
  )
    throw new Error(
      "Usage: release-documentation.ts reconcile | validate-pr | check <desktop|eda|nsp>",
    );
  const { GITHUB_REPOSITORY: repository, GH_TOKEN: token } = process.env;
  if (!repository || !token)
    throw new Error("Set GITHUB_REPOSITORY and GH_TOKEN for release documentation.");
  const options = {
    root: process.cwd(),
    repository,
    readGithub: githubReader(token, process.env.GITHUB_API_URL),
    writeGithub: githubWriter(token, process.env.GITHUB_API_URL),
    automaticCi: process.env.RELEASE_DOCS_AUTOMATIC_CI === "true",
  };
  if (mode === "validate-pr") {
    await validateReleaseDocumentationPullRequest({
      repository,
      sourceSha: process.env.RELEASE_DOCS_SHA ?? "",
      pullRequest: Number(process.env.RELEASE_DOCS_PR),
      readGithub: options.readGithub,
    });
    process.stdout.write("Validated the owned release documentation PR and exact CI source.\n");
  } else if (mode === "check") {
    const plan = await checkReleaseReconciliation(options, component as ReleaseComponent);
    if (component === "desktop") await updateQualification(options.root, plan, true);
    process.stdout.write(`Verified previous release finalization for ${component}.\n`);
  } else {
    let expectedPublication: ReleaseDocumentationOptions["expectedPublication"];
    if (process.env.GITHUB_EVENT_PATH) {
      const event = object(JSON.parse(await readFile(process.env.GITHUB_EVENT_PATH, "utf8")));
      if (event.release !== undefined) {
        const release = object(event.release);
        if (
          typeof release.tag_name !== "string" ||
          !Number.isSafeInteger(release.id) ||
          (release.id as number) <= 0
        )
          throw new Error("Publication event has an invalid release identity.");
        if (/^(?:plugins\/(?:eda|nsp)\/)?v\d/u.test(release.tag_name))
          expectedPublication = { tag: release.tag_name, id: release.id as number };
      }
    }
    const result = await finalizeReleaseDocumentation({
      ...options,
      ...(expectedPublication ? { expectedPublication } : {}),
    });
    process.stdout.write(
      await writeReleaseDocumentationFinalization(result, repository, {
        output: process.env.GITHUB_OUTPUT,
        summary: process.env.GITHUB_STEP_SUMMARY,
      }),
    );
  }
}
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void main().catch((error: unknown) => {
    process.stderr.write(
      `Release documentation failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
