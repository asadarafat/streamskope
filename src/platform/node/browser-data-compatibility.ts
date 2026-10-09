import { parseReleaseVersion } from "../../plugins/compatibility";

export const BROWSER_DATA_COMPATIBILITY = Object.freeze({
  contract: "streamskope-browser-data-v1",
  inspector: "dist/web/data-preflight.cjs",
  reportSchemaVersion: 1,
} as const);
export const BROWSER_DATA_DOCUMENT_KINDS = [
  "filesystem",
  "vault",
  "kafka-profiles",
  "nats-profiles",
  "rules",
  "preferences",
  "topic-history",
  "queries",
  "trust-recipes",
  "observations",
  "plugin-installations",
  "plugin-network",
  "plugin-catalog",
  "plugin-package-cache",
  "plugin-recovery",
  "profile-backups",
  "host-state",
] as const;
export const BROWSER_DATA_INSPECTION_LIMITATIONS = [
  "protected-content-authenticity",
  "protected-profile-schema",
  "remote-plugin-resource-cleanup",
  "host-quiescence",
] as const;
export const BROWSER_DATA_INSPECTION_REASONS = [
  "invalid-request",
  "unavailable",
  "unsafe-filesystem",
  "inspection-limit",
  "unrecognized-path",
  "unsupported-format",
  "invalid-protected-envelope",
  "vault-required",
  "plugin-change-pending",
  "plugin-package-invalid",
  "plugin-incompatible",
  "plugin-recovery-pending",
  "managed-source-unverified",
  "network-configuration-unsupported",
  "interrupted-state",
] as const;
export type BrowserDataDocumentKind = (typeof BROWSER_DATA_DOCUMENT_KINDS)[number];
export type BrowserDataInspectionReason = (typeof BROWSER_DATA_INSPECTION_REASONS)[number];
export interface BrowserDataDocumentInspection {
  readonly kind: BrowserDataDocumentKind;
  readonly state: "missing" | "verified" | "blocked" | "not-inspected";
  readonly count: number;
  readonly formats: readonly number[];
  readonly reason: BrowserDataInspectionReason | null;
}
export interface BrowserDataInspection {
  readonly schemaVersion: 1;
  readonly dataContract: typeof BROWSER_DATA_COMPATIBILITY.contract;
  readonly hostRelease: string;
  readonly outcome: "eligible" | "blocked";
  readonly documents: readonly BrowserDataDocumentInspection[];
  readonly unverified: typeof BROWSER_DATA_INSPECTION_LIMITATIONS;
}

function invalid(): never {
  throw new Error("Invalid browser data inspection report.");
}
function object(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return invalid();
  const record = value as Record<string, unknown>;
  if (Object.keys(record).length !== keys.length || keys.some((key) => !Object.hasOwn(record, key)))
    return invalid();
  return record;
}

/** Closed, source-free evidence; image/source authority belongs to the verified caller. */
export function parseBrowserDataInspection(value: unknown): BrowserDataInspection {
  const report = object(value, [
    "schemaVersion",
    "dataContract",
    "hostRelease",
    "outcome",
    "documents",
    "unverified",
  ]);
  if (
    report.schemaVersion !== 1 ||
    report.dataContract !== BROWSER_DATA_COMPATIBILITY.contract ||
    typeof report.hostRelease !== "string" ||
    !report.hostRelease.startsWith("v")
  )
    return invalid();
  try {
    parseReleaseVersion(report.hostRelease.slice(1));
  } catch {
    return invalid();
  }
  if (
    !Array.isArray(report.documents) ||
    report.documents.length !== BROWSER_DATA_DOCUMENT_KINDS.length ||
    !Array.isArray(report.unverified) ||
    report.unverified.length !== BROWSER_DATA_INSPECTION_LIMITATIONS.length ||
    report.unverified.some((item, index) => item !== BROWSER_DATA_INSPECTION_LIMITATIONS[index])
  )
    return invalid();
  const documents = report.documents.map((raw: unknown, index): BrowserDataDocumentInspection => {
    const row = object(raw, ["kind", "state", "count", "formats", "reason"]);
    const kind = BROWSER_DATA_DOCUMENT_KINDS[index]!;
    if (
      row.kind !== kind ||
      (row.state !== "missing" &&
        row.state !== "verified" &&
        row.state !== "blocked" &&
        row.state !== "not-inspected") ||
      typeof row.count !== "number" ||
      !Number.isSafeInteger(row.count) ||
      row.count < 0 ||
      row.count > 8192 ||
      !Array.isArray(row.formats) ||
      row.formats.length > 4
    )
      return invalid();
    let previousFormat = 0;
    const formats = row.formats.map((format: unknown): number => {
      if (
        typeof format !== "number" ||
        !Number.isInteger(format) ||
        format < 1 ||
        format >
          (kind === "kafka-profiles" || kind === "profile-backups"
            ? 4
            : kind === "preferences"
              ? 2
              : 1) ||
        format <= previousFormat
      )
        return invalid();
      previousFormat = format;
      return format;
    });
    const limited = row.state === "missing" || row.state === "not-inspected";
    const blocked = row.state === "blocked" || row.state === "not-inspected";
    if (
      (limited && (row.count !== 0 || formats.length !== 0)) ||
      (blocked
        ? !BROWSER_DATA_INSPECTION_REASONS.some((reason) => reason === row.reason)
        : row.reason !== null)
    )
      return invalid();
    return {
      kind,
      state: row.state,
      count: row.count,
      formats,
      reason: row.reason as BrowserDataInspectionReason | null,
    };
  });
  const outcome = documents.some((row) => row.state === "blocked" || row.state === "not-inspected")
    ? "blocked"
    : "eligible";
  if (report.outcome !== outcome) return invalid();
  return {
    schemaVersion: 1,
    dataContract: BROWSER_DATA_COMPATIBILITY.contract,
    hostRelease: report.hostRelease,
    outcome,
    documents,
    unverified: BROWSER_DATA_INSPECTION_LIMITATIONS,
  };
}

/** Exact reviewed predecessor identities, shared with the standalone maintenance policy. */
export const REVIEWED_BROWSER_PREDECESSORS = Object.freeze([
  Object.freeze({
    version: "0.10.3",
    sourceRevision: "089980705afaffc1a6135a9347eb1bff68b27206",
    registryReference:
      "ghcr.io/asadarafat/streamskope:0.10.3@sha256:74995d8654ad9c967abe631e177ee4739ce7d0267070c03e40b9f62c3dba2057",
    images: Object.freeze({
      amd64: "sha256:3dfbfb6eebfeed1e279a7286acc769ad50f491676babf0c8b71166807a5b8923",
      arm64: "sha256:deb3afffbb1196488e97993e420083fb47d9058e497a111861f372b3a1a63eaf",
    }),
    contract: BROWSER_DATA_COMPATIBILITY.contract,
  }),
]);

/** Reviewed predecessor identity only; maintenance must also limit its Kafka formats to 1–3. */
export function reviewedLegacyBrowserDataContract(identity: {
  readonly version: string;
  readonly sourceRevision: string;
  readonly registryReference: string;
  readonly architecture: "amd64" | "arm64";
  readonly imageId: string;
}): typeof BROWSER_DATA_COMPATIBILITY.contract | undefined {
  return REVIEWED_BROWSER_PREDECESSORS.find(
    (predecessor) =>
      identity.version === predecessor.version &&
      identity.sourceRevision === predecessor.sourceRevision &&
      identity.registryReference === predecessor.registryReference &&
      identity.imageId === predecessor.images[identity.architecture],
  )?.contract;
}
