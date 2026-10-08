import { isDeepStrictEqual } from "node:util";

import {
  BROWSER_DATA_COMPATIBILITY,
  parseBrowserDataInspection,
  type BrowserDataInspection,
} from "../../src/platform/node/browser-data-compatibility";
import { ciExecution, type CiExecution } from "../check/ci-evidence";

import { parseBrowserRegistryMetadata } from "./browser-registry-metadata";

export const BROWSER_INSTALLER_CHECKS = [
  "native installer deployment",
  "anonymous pinned registry image",
  "qualified topology unchanged",
  "setup code absent from captured output",
  "authenticated gateway",
  "encrypted profile persistence",
  "native workers",
  "read-only bundled data preflight",
  "graceful restart through installer",
  "running installer idempotency",
  "graceful disposable deployment cleanup",
] as const;

export interface BrowserInstallerTarget {
  readonly version: string;
  readonly sourceRevision: string;
  readonly platform: string;
  readonly image: string;
  readonly imageId: string;
}

export interface BrowserDataPreflightEvidence {
  readonly imageId: string;
  readonly dataSnapshotSha256: string;
  readonly inspection: BrowserDataInspection;
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Browser qualification metadata must be an object.");
  return value as Record<string, unknown>;
}

/** Derive native authority from the qualified manifest, never from a submitted receipt. */
export function browserInstallerTargets(
  value: unknown,
  version: string,
  sourceRevision: string,
): readonly BrowserInstallerTarget[] {
  const manifest = object(value);
  if (
    manifest.schemaVersion !== 4 ||
    manifest.version !== version ||
    manifest.sourceRevision !== sourceRevision ||
    !isDeepStrictEqual(manifest.dataCompatibility, BROWSER_DATA_COMPATIBILITY)
  )
    throw new Error("Current native qualification requires matching schema 4 browser metadata.");
  const registry = parseBrowserRegistryMetadata(manifest.registry, version, sourceRevision);
  return registry.platforms.map(({ platform, imageId }) => ({
    version,
    sourceRevision,
    platform,
    image: registry.reference,
    imageId,
  }));
}

export function validateBrowserDataPreflight(
  value: unknown,
  expected: Pick<BrowserInstallerTarget, "version" | "imageId">,
): BrowserDataPreflightEvidence {
  const input = object(value);
  if (
    !isDeepStrictEqual(Object.keys(input).sort(), [
      "dataSnapshotSha256",
      "imageId",
      "inspection",
    ]) ||
    input.imageId !== expected.imageId ||
    !/^sha256:[a-f0-9]{64}$/u.test(expected.imageId) ||
    typeof input.dataSnapshotSha256 !== "string" ||
    !/^[a-f0-9]{64}$/u.test(input.dataSnapshotSha256)
  )
    throw new Error("Native data preflight does not match the qualified image or data snapshot.");
  const inspection = parseBrowserDataInspection(input.inspection);
  if (inspection.hostRelease !== `v${expected.version}` || inspection.outcome !== "eligible")
    throw new Error("Native data preflight did not qualify this host release's data envelopes.");
  const filesystem = inspection.documents.find((item) => item.kind === "filesystem")!;
  if (
    filesystem.state !== "verified" ||
    filesystem.count < 3 ||
    ["vault", "nats-profiles"].some((kind) => {
      const document = inspection.documents.find((item) => item.kind === kind)!;
      return (
        document.state !== "verified" ||
        document.count !== 1 ||
        !isDeepStrictEqual(document.formats, [1])
      );
    })
  )
    throw new Error(
      "Native data preflight must inspect the created vault and encrypted NATS profile fixture.",
    );
  return {
    imageId: expected.imageId,
    dataSnapshotSha256: input.dataSnapshotSha256,
    inspection,
  };
}

export interface BrowserInstallerEvidence {
  readonly schemaVersion: 3;
  readonly deliveryScope: "public-registry";
  readonly outcome: "passed";
  readonly version: string;
  readonly sourceRevision: string;
  readonly platform: string;
  readonly image: string;
  readonly execution: CiExecution;
  readonly startedAt: string;
  readonly completedAt: string;
  readonly releaseTransport: string;
  readonly preflight: BrowserDataPreflightEvidence;
  readonly checks: readonly string[];
}

export function browserInstallerEvidence(
  input: Pick<
    BrowserInstallerEvidence,
    "version" | "sourceRevision" | "platform" | "image" | "startedAt" | "preflight"
  >,
  env: NodeJS.ProcessEnv = process.env,
): BrowserInstallerEvidence {
  return {
    schemaVersion: 3,
    deliveryScope: "public-registry",
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
    imageId: string;
    execution: CiExecution;
  },
): BrowserInstallerEvidence {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Missing native installer qualification.");
  const input = value as BrowserInstallerEvidence;
  if (
    input.schemaVersion !== 3 ||
    input.deliveryScope !== "public-registry" ||
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
    throw new Error(
      "Current native installer qualification requires schema 3 public-registry evidence for this release, source and run.",
    );
  const preflight = validateBrowserDataPreflight(input.preflight, expected);
  // Return only public fields, never arbitrary additions to an uploaded receipt.
  return {
    ...browserInstallerEvidence(
      {
        version: expected.version,
        sourceRevision: expected.sourceRevision,
        platform: expected.platform,
        image: expected.image,
        startedAt: input.startedAt,
        preflight,
      },
      {},
    ),
    execution: { runId: input.execution.runId, attempt: input.execution.attempt },
    completedAt: input.completedAt,
  };
}
