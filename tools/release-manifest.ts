export type ReleasePlatform = "darwin" | "linux" | "win32";
export type ReleaseArchitecture = "arm64" | "x64";
export type ReleaseSignatureKind = "apple-developer-id" | "authenticode" | "openpgp";

export interface ReleaseArtifactEvidence {
  readonly architecture: ReleaseArchitecture;
  readonly file: string;
  readonly installation: "passed";
  readonly notarization: "not-applicable" | "verified";
  readonly platform: ReleasePlatform;
  readonly sensitiveScan: "passed";
  readonly sha256: string;
  readonly signature: {
    readonly kind: ReleaseSignatureKind;
    readonly state: "verified";
  };
  readonly startup: "passed";
}

export interface ReleaseCompatibilityEvidence {
  readonly fromVersion: string;
  readonly preferences: "passed";
  readonly profiles: "passed";
}

export interface ReleaseManifest {
  readonly artifacts: readonly ReleaseArtifactEvidence[];
  readonly channel: "candidate";
  readonly commit: string;
  readonly compatibility: ReleaseCompatibilityEvidence;
  readonly immutable: true;
  readonly provenance: {
    readonly attestationId: string;
    readonly issuer: "github-actions";
    readonly state: "verified";
  };
  readonly rollback: {
    readonly manifestSha256: string;
    readonly version: string;
  };
  readonly sbom: {
    readonly file: string;
    readonly format: "spdx-2.3";
    readonly sha256: string;
  };
  readonly schemaVersion: 1;
  readonly tag: string;
  readonly version: string;
}

const semanticVersionPattern = /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$/u;
const sha256Pattern = /^[a-f0-9]{64}$/u;
const artifactNamePattern = /^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/u;
const requiredTargets = new Set(["darwin-arm64", "linux-x64", "win32-x64"]);

function record(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${path} must be an object.`);
  }
  return value as Record<string, unknown>;
}

function exactKeys(value: Record<string, unknown>, keys: readonly string[], path: string): void {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  if (actual.length !== expected.length || actual.some((key, index) => key !== expected[index])) {
    throw new Error(`${path} contains undeclared or missing fields.`);
  }
}

function exactString<T extends string>(value: unknown, allowed: readonly T[], path: string): T {
  if (typeof value !== "string" || !allowed.includes(value as T)) {
    throw new Error(`${path} is not an allowed value.`);
  }
  return value as T;
}

function matchingString(value: unknown, pattern: RegExp, path: string): string {
  if (typeof value !== "string" || !pattern.test(value)) {
    throw new Error(`${path} is invalid.`);
  }
  return value;
}

function versionParts(version: string): readonly number[] {
  return version.split(".").map(Number);
}

function versionPrecedes(candidate: string, current: string): boolean {
  const left = versionParts(candidate);
  const right = versionParts(current);
  for (let index = 0; index < 3; index += 1) {
    const leftPart = left[index] ?? 0;
    const rightPart = right[index] ?? 0;
    if (leftPart !== rightPart) {
      return leftPart < rightPart;
    }
  }
  return false;
}

export function validateReleaseCompatibilityEvidence(value: unknown): ReleaseCompatibilityEvidence {
  const candidate = record(value, "release.compatibility");
  exactKeys(candidate, ["fromVersion", "preferences", "profiles"], "release.compatibility");
  return {
    fromVersion: matchingString(
      candidate.fromVersion,
      semanticVersionPattern,
      "release.compatibility.fromVersion",
    ),
    preferences: exactString(
      candidate.preferences,
      ["passed"],
      "release.compatibility.preferences",
    ),
    profiles: exactString(candidate.profiles, ["passed"], "release.compatibility.profiles"),
  };
}

export function validateReleaseArtifactEvidence(
  value: unknown,
  index = 0,
): ReleaseArtifactEvidence {
  const path = `release.artifacts[${String(index)}]`;
  const candidate = record(value, path);
  exactKeys(
    candidate,
    [
      "architecture",
      "file",
      "installation",
      "notarization",
      "platform",
      "sensitiveScan",
      "sha256",
      "signature",
      "startup",
    ],
    path,
  );
  const platform = exactString(
    candidate.platform,
    ["darwin", "linux", "win32"],
    `${path}.platform`,
  );
  const architecture = exactString(
    candidate.architecture,
    ["arm64", "x64"],
    `${path}.architecture`,
  );
  const signature = record(candidate.signature, `${path}.signature`);
  exactKeys(signature, ["kind", "state"], `${path}.signature`);
  const expectedSignature: Readonly<Record<ReleasePlatform, ReleaseSignatureKind>> = {
    darwin: "apple-developer-id",
    linux: "openpgp",
    win32: "authenticode",
  };
  const signatureKind = exactString(
    signature.kind,
    ["apple-developer-id", "authenticode", "openpgp"],
    `${path}.signature.kind`,
  );
  if (signatureKind !== expectedSignature[platform]) {
    throw new Error(`${path}.signature.kind does not match ${platform}.`);
  }
  const notarization = exactString(
    candidate.notarization,
    ["not-applicable", "verified"],
    `${path}.notarization`,
  );
  if (
    (platform === "darwin" && notarization !== "verified") ||
    (platform !== "darwin" && notarization !== "not-applicable")
  ) {
    throw new Error(`${path}.notarization does not match ${platform}.`);
  }
  return {
    architecture,
    file: matchingString(candidate.file, artifactNamePattern, `${path}.file`),
    installation: exactString(candidate.installation, ["passed"], `${path}.installation`),
    notarization,
    platform,
    sensitiveScan: exactString(candidate.sensitiveScan, ["passed"], `${path}.sensitiveScan`),
    sha256: matchingString(candidate.sha256, sha256Pattern, `${path}.sha256`),
    signature: {
      kind: signatureKind,
      state: exactString(signature.state, ["verified"], `${path}.signature.state`),
    },
    startup: exactString(candidate.startup, ["passed"], `${path}.startup`),
  };
}

export function validateReleaseManifest(value: unknown): ReleaseManifest {
  const candidate = record(value, "release");
  exactKeys(
    candidate,
    [
      "artifacts",
      "channel",
      "commit",
      "compatibility",
      "immutable",
      "provenance",
      "rollback",
      "sbom",
      "schemaVersion",
      "tag",
      "version",
    ],
    "release",
  );
  const version = matchingString(candidate.version, semanticVersionPattern, "release.version");
  if (candidate.tag !== `v${version}`) {
    throw new Error("release.tag must identify the exact release version.");
  }
  if (!Array.isArray(candidate.artifacts)) {
    throw new Error("release.artifacts must be an array.");
  }
  const artifacts = candidate.artifacts.map((artifact, index) =>
    validateReleaseArtifactEvidence(artifact, index),
  );
  const targets = artifacts.map((artifact) => `${artifact.platform}-${artifact.architecture}`);
  if (
    new Set(targets).size !== targets.length ||
    targets.length !== requiredTargets.size ||
    targets.some((target) => !requiredTargets.has(target))
  ) {
    throw new Error("release.artifacts must contain each supported native target exactly once.");
  }
  if (new Set(artifacts.map((artifact) => artifact.file)).size !== artifacts.length) {
    throw new Error("release.artifacts must use unique file names.");
  }

  const compatibility = validateReleaseCompatibilityEvidence(candidate.compatibility);
  const fromVersion = compatibility.fromVersion;
  if (!versionPrecedes(fromVersion, version)) {
    throw new Error("release.compatibility.fromVersion must precede the release.");
  }

  const provenance = record(candidate.provenance, "release.provenance");
  exactKeys(provenance, ["attestationId", "issuer", "state"], "release.provenance");
  const rollback = record(candidate.rollback, "release.rollback");
  exactKeys(rollback, ["manifestSha256", "version"], "release.rollback");
  const rollbackVersion = matchingString(
    rollback.version,
    semanticVersionPattern,
    "release.rollback.version",
  );
  if (rollbackVersion !== fromVersion) {
    throw new Error("release.rollback.version must match the compatibility baseline.");
  }
  const sbom = record(candidate.sbom, "release.sbom");
  exactKeys(sbom, ["file", "format", "sha256"], "release.sbom");

  if (candidate.immutable !== true) {
    throw new Error("release.immutable must be true.");
  }
  if (candidate.schemaVersion !== 1) {
    throw new Error("release.schemaVersion must be 1.");
  }
  return {
    artifacts,
    channel: exactString(candidate.channel, ["candidate"], "release.channel"),
    commit: matchingString(candidate.commit, /^[a-f0-9]{40}$/u, "release.commit"),
    compatibility,
    immutable: true,
    provenance: {
      attestationId: matchingString(
        provenance.attestationId,
        /^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/u,
        "release.provenance.attestationId",
      ),
      issuer: exactString(provenance.issuer, ["github-actions"], "release.provenance.issuer"),
      state: exactString(provenance.state, ["verified"], "release.provenance.state"),
    },
    rollback: {
      manifestSha256: matchingString(
        rollback.manifestSha256,
        sha256Pattern,
        "release.rollback.manifestSha256",
      ),
      version: rollbackVersion,
    },
    sbom: {
      file: matchingString(sbom.file, artifactNamePattern, "release.sbom.file"),
      format: exactString(sbom.format, ["spdx-2.3"], "release.sbom.format"),
      sha256: matchingString(sbom.sha256, sha256Pattern, "release.sbom.sha256"),
    },
    schemaVersion: 1,
    tag: `v${version}`,
    version,
  };
}
