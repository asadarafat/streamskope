import { lstat, realpath } from "node:fs/promises";
import { join } from "node:path";

import { BUILD_DEPENDENCY_PATCHES } from "./build-dependency-patch-data";
import { FORGE_BACKPORT } from "./forge-patch";
import { RUNTIME_DEPENDENCY_PATCHES } from "./runtime-dependency-patch-data";

const UPSTREAM_STATUSES = [
  "proposed-fix",
  "reported-unfixed",
  "disputed-local-policy",
  "local-runtime-correction",
] as const;

interface DependencyMaintenanceEntry {
  readonly name: string;
  readonly reviewedVersion: string;
  readonly owner: string;
  readonly reviewedOn: string;
  readonly upstreamStatus: (typeof UPSTREAM_STATUSES)[number];
  readonly evidence: readonly string[];
  readonly retirementCriteria: string;
  readonly regressions: readonly string[];
}

// Reviewed versions are deliberately independent of the executable patch definitions.
// New definitions or locked versions require an explicit ownership/evidence review.
export const DEPENDENCY_MAINTENANCE = [
  {
    name: "node-forge",
    reviewedVersion: "1.4.0",
    owner: "@asadarafat",
    reviewedOn: "2026-10-08",
    upstreamStatus: "proposed-fix",
    evidence: [
      "https://github.com/digitalbazaar/forge/pull/1152",
      "https://github.com/advisories/GHSA-86w9-cpqp-85rv",
    ],
    retirementCriteria:
      "Qualify an unmodified upstream release against the nested DigestAlgorithm forgery and valid-signature regressions; then remove the exact-source backport and its audit allowance together.",
    regressions: [
      "test/integration/forge-security-patch.test.ts",
      "test/unit/dependency-audit.test.ts",
    ],
  },
  {
    name: "braces",
    reviewedVersion: "3.0.3",
    owner: "@asadarafat",
    reviewedOn: "2026-10-08",
    upstreamStatus: "reported-unfixed",
    evidence: [
      "https://github.com/micromatch/braces/issues/70",
      "https://github.com/advisories/GHSA-vfj7-8cjw-p6xm",
    ],
    retirementCriteria:
      "Qualify unmodified upstream parsing, compilation, expansion and stringification against excessive nesting while preserving ordinary brace behavior; remove the patch and audit allowance only after those regressions pass.",
    regressions: ["test/unit/build-dependency-patches.test.ts"],
  },
  {
    name: "http-cache-semantics",
    reviewedVersion: "4.2.0",
    owner: "@asadarafat",
    reviewedOn: "2026-10-08",
    upstreamStatus: "disputed-local-policy",
    evidence: [
      "https://github.com/kornelski/http-cache-semantics/issues/56",
      "https://github.com/kornelski/http-cache-semantics/issues/56#issuecomment-5975759591",
      "https://github.com/advisories/GHSA-ch52-4w7c-c8xp",
      "https://registry.npmjs.org/http-cache-semantics/4.3.0",
    ],
    retirementCriteria:
      "Qualify unmodified upstream cookie, private, no-cache and no-store rejection plus permitted public stale reuse against the project policy. Upstream disputes the advisory and version 4.3.0 fails that stricter policy; retirement requires passing regressions or an explicit review of the policy, patch and audit allowance together.",
    regressions: ["test/unit/build-dependency-patches.test.ts"],
  },
  {
    name: "@nats-io/transport-node",
    reviewedVersion: "3.4.0",
    owner: "@asadarafat",
    reviewedOn: "2026-10-08",
    upstreamStatus: "local-runtime-correction",
    evidence: [
      "https://github.com/nats-io/nats.js/issues/435",
      "https://github.com/nats-io/nats.js/blob/95e76e79d9feaa0a0bf3b0e8da526ec5a3460979/transport-node/src/node_transport.ts",
    ],
    retirementCriteria:
      "Qualify an unmodified upstream release against pending INFO/TLS socket cleanup and strict TLS hostname/authentication regressions. Issue 435 covers pending-dial ownership, not every local TLS correction; retire only when all owned behavior passes without the runtime patch.",
    regressions: [
      "test/integration/nats-sdk-runtime-patch.test.ts",
      "test/support/nats-sdk-socket-probe.mjs",
      "test/support/nats-sdk-tls-probe.mjs",
    ],
  },
] as const satisfies readonly DependencyMaintenanceEntry[];

const ENTRY_KEYS = [
  "name",
  "reviewedVersion",
  "owner",
  "reviewedOn",
  "upstreamStatus",
  "evidence",
  "retirementCriteria",
  "regressions",
];

function record(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error(`${label} must be an object.`);
  return value as Record<string, unknown>;
}

function text(value: unknown, label: string, maximum: number): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maximum ||
    value.trim() !== value ||
    [...value].some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127)
  )
    throw new Error(`${label} must be bounded nonempty text.`);
  return value;
}

function strings(value: unknown, label: string): readonly string[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > 8)
    throw new Error(`${label} must contain between one and eight entries.`);
  const values = value.map((entry: unknown) => text(entry, label, 2048));
  if (new Set(values).size !== values.length) throw new Error(`${label} contains duplicates.`);
  return values;
}

function evidenceUrl(value: string): void {
  const url = new URL(value);
  if (
    url.protocol !== "https:" ||
    !["github.com", "registry.npmjs.org"].includes(url.hostname) ||
    url.username !== "" ||
    url.password !== "" ||
    url.port !== "" ||
    url.search !== "" ||
    url.pathname === "/" ||
    /\s/u.test(value)
  )
    throw new Error("Mitigation evidence must use credential-free primary HTTPS URLs.");
}

async function regressionFile(root: string, path: string): Promise<void> {
  if (
    path.length > 256 ||
    !/^test\/(?:unit|integration|support)\/[a-zA-Z0-9/_.-]+$/u.test(path) ||
    path.split("/").some((part) => part === "" || part === "." || part === "..")
  )
    throw new Error("Mitigation regression must be a bounded repository-relative test path.");
  const file = join(root, path);
  if (!(await lstat(file)).isFile() || (await realpath(file)) !== file)
    throw new Error(`Mitigation regression must be a regular, unlinked test file: ${path}`);
}

function lockedPackageName(path: string): string | undefined {
  return /(?:^|\/)node_modules\/((?:@[^/]+\/)?[^/]+)$/u.exec(path)?.[1];
}

function safeLockPath(path: string): boolean {
  return (
    path.split("/").every((part) => part !== "." && part !== "..") &&
    /^(?:node_modules\/(?:@[a-zA-Z0-9._-]+\/)?[a-zA-Z0-9._-]+\/)*node_modules\/(?:@[a-zA-Z0-9._-]+\/)?[a-zA-Z0-9._-]+$/u.test(
      path,
    )
  );
}

export async function assertDependencyMaintenance(
  root: string,
  lockValue: unknown,
  entriesValue: unknown = DEPENDENCY_MAINTENANCE,
): Promise<{ readonly mitigationCount: number; readonly lockedInstanceCount: number }> {
  const definitions = [
    { name: "node-forge", version: FORGE_BACKPORT.version },
    ...BUILD_DEPENDENCY_PATCHES,
    ...RUNTIME_DEPENDENCY_PATCHES,
  ];
  const expected = new Map(definitions.map(({ name, version }) => [name, version]));
  if (expected.size !== definitions.length) throw new Error("Duplicate mitigation definitions.");
  if (!Array.isArray(entriesValue) || entriesValue.length !== expected.size)
    throw new Error("Mitigation maintenance must cover every active definition exactly once.");
  const directory = await realpath(root);
  const reviewed = new Set<string>();
  for (const value of entriesValue as unknown[]) {
    const entry = record(value, "Mitigation maintenance entry");
    const keys = Object.keys(entry);
    if (keys.length !== ENTRY_KEYS.length || keys.some((key) => !ENTRY_KEYS.includes(key)))
      throw new Error("Mitigation maintenance entry has unexpected or missing fields.");
    const name = text(entry.name, "Mitigation name", 128);
    if (!expected.has(name) || reviewed.has(name))
      throw new Error(`Unexpected or duplicate mitigation maintenance: ${name}`);
    if (entry.reviewedVersion !== expected.get(name))
      throw new Error(`${name} reviewed version differs from its active mitigation definition.`);
    const owner = text(entry.owner, `${name} owner`, 40);
    if (!/^@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,37}[a-zA-Z0-9])?$/u.test(owner) || owner.includes("--"))
      throw new Error(`${name} owner must be a GitHub handle.`);
    const reviewedOn = text(entry.reviewedOn, `${name} review date`, 10);
    const date = new Date(reviewedOn);
    if (
      !/^\d{4}-\d{2}-\d{2}$/u.test(reviewedOn) ||
      !Number.isFinite(date.getTime()) ||
      date.toISOString().slice(0, 10) !== reviewedOn
    )
      throw new Error(`${name} review date must be a real YYYY-MM-DD date.`);
    if (!UPSTREAM_STATUSES.some((status) => status === entry.upstreamStatus))
      throw new Error(`${name} has an unknown upstream status.`);
    strings(entry.evidence, `${name} evidence`).forEach(evidenceUrl);
    text(entry.retirementCriteria, `${name} retirement criteria`, 1500);
    for (const path of strings(entry.regressions, `${name} regressions`))
      await regressionFile(directory, path);
    reviewed.add(name);
  }

  const lock = record(lockValue, "Package lock");
  if (lock.lockfileVersion !== 3) throw new Error("Mitigation maintenance requires lockfile 3.");
  const found = new Set<string>();
  let lockedInstanceCount = 0;
  for (const [path, value] of Object.entries(record(lock.packages, "Locked packages"))) {
    const entry = record(value, `Locked ${path}`);
    const name = lockedPackageName(path);
    if (typeof entry.name === "string" && expected.has(entry.name) && entry.name !== name)
      throw new Error(`Mitigation maintenance refuses an aliased package: ${path}`);
    if (name === undefined || !expected.has(name)) continue;
    if (
      !safeLockPath(path) ||
      (entry.name !== undefined && entry.name !== name) ||
      entry.version !== expected.get(name)
    )
      throw new Error(`Unreviewed locked mitigation ${name}: ${path}`);
    found.add(name);
    lockedInstanceCount += 1;
  }
  for (const name of expected.keys())
    if (!found.has(name)) throw new Error(`Missing locked mitigation: ${name}`);
  return { mitigationCount: reviewed.size, lockedInstanceCount };
}
