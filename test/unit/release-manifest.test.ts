import { describe, expect, it } from "vitest";

import { validateReleaseManifest, type ReleaseManifest } from "../../tools/release-manifest";

const sha256 = "a".repeat(64);

function artifact(
  platform: "darwin" | "linux" | "win32",
  architecture: "arm64" | "x64",
): ReleaseManifest["artifacts"][number] {
  return {
    architecture,
    file: `StreamSkope-0.1.0-${platform}-${architecture}.zip`,
    installation: "passed",
    notarization: platform === "darwin" ? "verified" : "not-applicable",
    platform,
    sensitiveScan: "passed",
    sha256,
    signature: {
      kind:
        platform === "darwin"
          ? "apple-developer-id"
          : platform === "win32"
            ? "authenticode"
            : "openpgp",
      state: "verified",
    },
    startup: "passed",
  };
}

function validManifest(): ReleaseManifest {
  return {
    artifacts: [artifact("darwin", "arm64"), artifact("linux", "x64"), artifact("win32", "x64")],
    channel: "candidate",
    commit: "b".repeat(40),
    compatibility: {
      fromVersion: "0.0.0",
      preferences: "passed",
      profiles: "passed",
    },
    immutable: true,
    provenance: {
      attestationId: "github-attestation-123",
      issuer: "github-actions",
      state: "verified",
    },
    rollback: {
      manifestSha256: "c".repeat(64),
      version: "0.0.0",
    },
    sbom: {
      file: "streamskope-0.1.0.spdx.json",
      format: "spdx-2.3",
      sha256: "d".repeat(64),
    },
    schemaVersion: 1,
    tag: "v0.1.0",
    version: "0.1.0",
  };
}

describe("production release manifest", () => {
  it("rejects duplicate Linux candidates and missing desktop targets", () => {
    const manifest = validManifest();
    expect(() =>
      validateReleaseManifest({
        ...manifest,
        artifacts: [...manifest.artifacts, artifact("linux", "x64")],
      }),
    ).toThrow();
    for (const remaining of manifest.artifacts) {
      expect(() => validateReleaseManifest({ ...manifest, artifacts: [remaining] })).toThrow();
    }
  });
  it("accepts immutable native evidence tied to one version and commit", () => {
    expect(validateReleaseManifest(validManifest())).toEqual(validManifest());
  });

  it.each([
    [
      "tag mismatch",
      (manifest: ReleaseManifest): void => {
        Object.assign(manifest, { tag: "v0.1.1" });
      },
    ],
    [
      "unverified signature",
      (manifest: ReleaseManifest): void => {
        Object.assign(manifest.artifacts[0]?.signature ?? {}, { state: "pending" });
      },
    ],
    [
      "missing notarization",
      (manifest: ReleaseManifest): void => {
        Object.assign(manifest.artifacts[0] ?? {}, { notarization: "not-applicable" });
      },
    ],
    [
      "failed packaged startup",
      (manifest: ReleaseManifest): void => {
        Object.assign(manifest.artifacts[1] ?? {}, { startup: "failed" });
      },
    ],
    [
      "duplicate native target",
      (manifest: ReleaseManifest): void => {
        Object.assign(manifest, {
          artifacts: [...manifest.artifacts, { ...manifest.artifacts[0] }],
        });
      },
    ],
    [
      "missing provenance",
      (manifest: ReleaseManifest): void => {
        Object.assign(manifest.provenance, { state: "pending" });
      },
    ],
    [
      "failed N-1 preference compatibility",
      (manifest: ReleaseManifest): void => {
        Object.assign(manifest.compatibility, { preferences: "failed" });
      },
    ],
    [
      "mutable release",
      (manifest: ReleaseManifest): void => {
        Object.assign(manifest, { immutable: false });
      },
    ],
  ])("rejects %s", (_label, mutate) => {
    const manifest = structuredClone(validManifest());
    mutate(manifest);

    expect(() => validateReleaseManifest(manifest)).toThrow();
  });

  it("rejects path-bearing artifact names and undeclared fields", () => {
    const pathBearing = structuredClone(validManifest());
    Object.assign(pathBearing.artifacts[0] ?? {}, { file: "../unsigned/StreamSkope.zip" });
    expect(() => validateReleaseManifest(pathBearing)).toThrow();

    const expanded = { ...validManifest(), signingPassword: "must-never-enter-manifest" };
    expect(() => validateReleaseManifest(expanded)).toThrow();
  });
});
