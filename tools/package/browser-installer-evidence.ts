import { ciExecution, type CiExecution } from "../check/ci-evidence";

export const BROWSER_INSTALLER_CHECKS = [
  "native installer deployment",
  "anonymous pinned registry image",
  "qualified topology unchanged",
  "setup code absent from captured output",
  "authenticated gateway",
  "encrypted profile persistence",
  "native workers",
  "graceful restart through installer",
  "running installer idempotency",
  "graceful disposable deployment cleanup",
] as const;

export interface BrowserInstallerEvidence {
  readonly schemaVersion: 2;
  readonly outcome: "passed";
  readonly version: string;
  readonly sourceRevision: string;
  readonly platform: string;
  readonly image: string;
  readonly execution: CiExecution;
  readonly startedAt: string;
  readonly completedAt: string;
  readonly releaseTransport: string;
  readonly checks: readonly string[];
}

export function browserInstallerEvidence(
  input: Pick<
    BrowserInstallerEvidence,
    "version" | "sourceRevision" | "platform" | "image" | "startedAt"
  >,
  env: NodeJS.ProcessEnv = process.env,
): BrowserInstallerEvidence {
  return {
    schemaVersion: 2,
    outcome: "passed",
    ...input,
    execution: ciExecution(env),
    completedAt: new Date().toISOString(),
    releaseTransport: "private staged exact-release assets; public GHCR image",
    checks: BROWSER_INSTALLER_CHECKS,
  };
}

export function validateBrowserInstallerEvidence(
  value: unknown,
  expected: {
    version: string;
    sourceRevision: string;
    platform: string;
    image: string;
    execution: CiExecution;
  },
): BrowserInstallerEvidence {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Missing native installer qualification.");
  const input = value as BrowserInstallerEvidence;
  if (
    input.schemaVersion !== 2 ||
    input.outcome !== "passed" ||
    input.version !== expected.version ||
    input.sourceRevision !== expected.sourceRevision ||
    input.platform !== expected.platform ||
    input.image !== expected.image ||
    !input.execution ||
    input.execution.runId !== expected.execution.runId ||
    !Number.isSafeInteger(input.execution.attempt) ||
    input.execution.attempt < 1 ||
    input.execution.attempt > expected.execution.attempt ||
    typeof input.startedAt !== "string" ||
    typeof input.completedAt !== "string" ||
    !Number.isFinite(Date.parse(input.startedAt)) ||
    !Number.isFinite(Date.parse(input.completedAt)) ||
    Date.parse(input.startedAt) > Date.parse(input.completedAt) ||
    Date.parse(input.completedAt) > Date.now() ||
    !Array.isArray(input.checks) ||
    JSON.stringify(input.checks) !== JSON.stringify(BROWSER_INSTALLER_CHECKS)
  )
    throw new Error("Native installer qualification does not match this release, source and run.");
  // Return only public fields, never arbitrary additions to an uploaded receipt.
  return {
    ...browserInstallerEvidence(
      {
        version: expected.version,
        sourceRevision: expected.sourceRevision,
        platform: expected.platform,
        image: expected.image,
        startedAt: input.startedAt,
      },
      {},
    ),
    execution: { runId: input.execution.runId, attempt: input.execution.attempt },
    completedAt: input.completedAt,
  };
}
