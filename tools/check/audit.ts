import { spawnSync, type SpawnSyncOptionsWithStringEncoding } from "node:child_process";
import { resolve } from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

import { verifyForgePatch } from "./forge-patch";

const BACKPORTED_ADVISORY = "https://github.com/advisories/GHSA-86w9-cpqp-85rv";
const SEVERITIES = ["info", "low", "moderate", "high", "critical"] as const;
type Severity = (typeof SEVERITIES)[number];

interface Advisory {
  readonly dependency: string;
  readonly name: string;
  readonly range: string;
  readonly severity: Severity;
  readonly source: number;
  readonly url: string;
}

interface Finding {
  readonly name: string;
  readonly nodes: readonly string[];
  readonly severity: Severity;
  readonly via: readonly (string | Advisory)[];
}

export interface AuditExecution {
  readonly error?: unknown;
  readonly signal?: string | null;
  readonly status: number | null;
  readonly stdout: string;
}

export interface DependencyAuditResult {
  readonly upstreamAffectedPackages: number;
  readonly backportedPackages: readonly string[];
  readonly remainingHighOrCriticalPackages: readonly string[];
  readonly otherReportedPackages: readonly string[];
}

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("npm audit returned a malformed report.");
  }
  return value as Record<string, unknown>;
}

function nonemptyString(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("npm audit returned a malformed string field.");
  }
  return value;
}

function severity(value: unknown): Severity {
  if (!SEVERITIES.some((candidate) => candidate === value)) {
    throw new Error("npm audit returned an unknown severity.");
  }
  return value as Severity;
}

function advisory(value: unknown): Advisory {
  const item = record(value);
  if (typeof item.source !== "number" || !Number.isSafeInteger(item.source) || item.source <= 0) {
    throw new Error("npm audit returned an invalid advisory source.");
  }
  return {
    dependency: nonemptyString(item.dependency),
    name: nonemptyString(item.name),
    range: nonemptyString(item.range),
    severity: severity(item.severity),
    source: item.source,
    url: nonemptyString(item.url),
  };
}

function finding(name: string, value: unknown): Finding {
  const item = record(value);
  if (item.name !== name || typeof item.isDirect !== "boolean") {
    throw new Error("npm audit returned inconsistent package identity.");
  }
  nonemptyString(item.range);
  if (
    !Array.isArray(item.nodes) ||
    item.nodes.length === 0 ||
    !Array.isArray(item.via) ||
    item.via.length === 0
  ) {
    throw new Error("npm audit returned an incomplete vulnerability graph.");
  }
  const nodes = item.nodes.map(nonemptyString);
  if (
    nodes.some(
      (node) =>
        !node.startsWith("node_modules/") ||
        node.includes("\\") ||
        node.split("/").some((part) => part === ".." || part === "." || part.length === 0),
    ) ||
    new Set(nodes).size !== nodes.length
  ) {
    throw new Error("npm audit returned invalid package paths.");
  }
  return {
    name,
    nodes,
    severity: severity(item.severity),
    via: item.via.map((entry: unknown) =>
      typeof entry === "string" ? nonemptyString(entry) : advisory(entry),
    ),
  };
}

function validateGraph(findings: ReadonlyMap<string, Finding>): void {
  const complete = new Set<string>();
  const visiting = new Set<string>();
  function visit(name: string): void {
    if (complete.has(name)) return;
    const item = findings.get(name);
    if (item === undefined || visiting.has(name)) {
      throw new Error("npm audit returned a missing or cyclic dependency reference.");
    }
    visiting.add(name);
    for (const entry of item.via) {
      if (typeof entry === "string") visit(entry);
      const causeSeverity =
        typeof entry === "string" ? findings.get(entry)?.severity : entry.severity;
      if (
        causeSeverity === undefined ||
        SEVERITIES.indexOf(causeSeverity) > SEVERITIES.indexOf(item.severity)
      ) {
        throw new Error("npm audit understates the severity of an underlying advisory.");
      }
    }
    visiting.delete(name);
    complete.add(name);
  }
  for (const name of findings.keys()) visit(name);
}

function isBackportedAdvisory(item: Advisory): boolean {
  return (
    item.url === BACKPORTED_ADVISORY &&
    item.name === "node-forge" &&
    item.dependency === "node-forge" &&
    item.range === "<=1.4.0" &&
    item.severity === "high"
  );
}

/** Interpret npm's complete report; verified paths must come from verifyForgePatch. */
export function evaluateDependencyAudit(
  execution: AuditExecution,
  verifiedForgePaths: readonly string[],
): DependencyAuditResult {
  if (
    execution.error !== undefined ||
    execution.signal ||
    (execution.status !== 0 && execution.status !== 1)
  ) {
    throw new Error("npm audit could not complete successfully.");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(execution.stdout) as unknown;
  } catch {
    throw new Error("npm audit did not return a valid JSON report.");
  }
  const report = record(parsed);
  if (report.auditReportVersion !== 2 || "error" in report) {
    throw new Error("npm audit returned an error or unsupported report version.");
  }
  const findings = new Map(
    Object.entries(record(report.vulnerabilities)).map(([name, value]) => [
      name,
      finding(name, value),
    ]),
  );
  const counts = record(record(report.metadata).vulnerabilities);
  for (const level of SEVERITIES) {
    const expected = [...findings.values()].filter((item) => item.severity === level).length;
    if (counts[level] !== expected)
      throw new Error("npm audit vulnerability counts do not match its findings.");
  }
  if (counts.total !== findings.size)
    throw new Error("npm audit total does not match its findings.");
  const hasHighOrCritical = [...findings.values()].some(
    (item) => item.severity === "high" || item.severity === "critical",
  );
  if (execution.status !== (hasHighOrCritical ? 1 : 0)) {
    throw new Error("npm audit exit status does not match its high-severity findings.");
  }
  validateGraph(findings);
  const verified = new Set(verifiedForgePaths);
  const forge = findings.get("node-forge");
  const forgeBackported =
    forge !== undefined &&
    forge.severity === "high" &&
    forge.nodes.every((node) => verified.has(node)) &&
    forge.via.every((item) => typeof item !== "string" && isBackportedAdvisory(item));
  const backportedPackages: string[] = [];
  const remainingHighOrCriticalPackages: string[] = [];
  const otherReportedPackages: string[] = [];
  for (const item of findings.values()) {
    const backported =
      forgeBackported &&
      item.severity === "high" &&
      (item.name === "node-forge" ||
        (item.name === "jks-js" &&
          item.nodes.every((node) => node === "node_modules/jks-js") &&
          item.via.every((entry) => entry === "node-forge")));
    if (backported) backportedPackages.push(item.name);
    else if (item.severity === "high" || item.severity === "critical") {
      remainingHighOrCriticalPackages.push(item.name);
    } else otherReportedPackages.push(item.name);
  }
  return {
    upstreamAffectedPackages: findings.size,
    backportedPackages: backportedPackages.sort(),
    remainingHighOrCriticalPackages: remainingHighOrCriticalPackages.sort(),
    otherReportedPackages: otherReportedPackages.sort(),
  };
}

export function runNpmAudit(
  root: string,
  npmCli: string,
  execute: (
    command: string,
    args: string[],
    options: SpawnSyncOptionsWithStringEncoding,
  ) => AuditExecution = spawnSync,
): AuditExecution {
  return execute(
    process.execPath,
    [
      npmCli,
      "audit",
      "--package-lock-only",
      "--audit-level=high",
      "--json",
      "--registry=https://registry.npmjs.org/",
      "--offline=false",
      "--prefer-offline=false",
      "--audit=true",
      "--include=prod",
      "--include=dev",
      "--include=optional",
      "--include=peer",
    ],
    { cwd: root, encoding: "utf8", timeout: 120_000, maxBuffer: 8 * 1024 * 1024 },
  );
}

export async function auditDependencies(root: string): Promise<void> {
  const verifiedForgePaths = await verifyForgePatch(root);
  const npmCli = process.env.npm_execpath;
  if (!npmCli) throw new Error("Run dependency qualification through npm run check.");
  const execution = runNpmAudit(root, npmCli);
  const result = evaluateDependencyAudit(execution, verifiedForgePaths);
  process.stdout.write(
    `npm audit: ${result.upstreamAffectedPackages} upstream affected packages.\n`,
  );
  if (result.backportedPackages.length > 0) {
    process.stdout.write(
      `Verified security backport GHSA-86w9-cpqp-85rv in node-forge 1.4.0 accounts for: ${result.backportedPackages.join(", ")}.\n`,
    );
  }
  if (result.otherReportedPackages.length > 0) {
    process.stdout.write(
      `Additional findings below the high severity gate: ${result.otherReportedPackages.join(", ")}.\n`,
    );
  }
  if (result.remainingHighOrCriticalPackages.length > 0) {
    throw new Error(
      `Unresolved high/critical npm audit findings: ${result.remainingHighOrCriticalPackages.join(", ")}.`,
    );
  }
  process.stdout.write(
    "Dependency audit qualification passed; upstream advisory counts remain reported above.\n",
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  auditDependencies(process.cwd()).catch((error: unknown) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : "Dependency audit failed."}\n`,
    );
    process.exitCode = 1;
  });
}
