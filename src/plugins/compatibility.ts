import type { PluginCompatibility, PluginManifest } from "./contracts";

const numericVersion = "(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)\\.(?:0|[1-9]\\d*)";
const targetVersion = new RegExp(`^${numericVersion}(?![\\s\\S])`, "u");
const desktopRelease = new RegExp(
  `^v(${numericVersion})(?:\\+build\\.([1-9]\\d*))?(?![\\s\\S])`,
  "u",
);
const systemIdentifier = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*(?![\s\S])/u;
const identity = new RegExp(
  `^(v${numericVersion}(?:\\+build\\.[1-9]\\d*)?)--([a-z][a-z0-9]*(?:-[a-z0-9]+)*)-(${numericVersion})-(${numericVersion})--r([1-9]\\d*)(?![\\s\\S])`,
  "u",
);
const semanticVersion =
  /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?![\s\S])/u;

export function parseSemanticPluginVersion(value: unknown): string {
  if (typeof value !== "string" || value.length > 128 || !semanticVersion.test(value)) {
    throw new Error("Plugin version must use semantic version syntax.");
  }
  return value;
}

/** New publications use precedence-bearing versions; metadata cannot identify a new release. */
export function parseReleaseVersion(value: unknown): string {
  const version = parseSemanticPluginVersion(value);
  if (version.includes("+")) {
    throw new Error(
      "Release versions must not use build metadata; publish a new semantic version.",
    );
  }
  return version;
}

function object(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).some((key) => !keys.includes(key))
  ) {
    throw new Error("Invalid plugin compatibility fields.");
  }
  return value as Record<string, unknown>;
}

function parseTargetVersion(value: unknown): string {
  if (typeof value !== "string" || value.length > 64 || !targetVersion.test(value)) {
    throw new Error("Plugin target versions must be exact numeric versions (major.minor.patch).");
  }
  return value;
}

function releaseParts(value: string): readonly bigint[] {
  const match = value.length <= 128 ? desktopRelease.exec(value) : null;
  if (match === null) {
    throw new Error("Invalid StreamSkope release; expected vMAJOR.MINOR.PATCH[+build.N].");
  }
  return [...match[1]!.split(".").map((part) => BigInt(part)), BigInt(match[2] ?? "0")];
}

function compareNumbers(left: readonly bigint[], right: readonly bigint[]): number {
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) return left[index]! < right[index]! ? -1 : 1;
  }
  return 0;
}

/** Desktop releases now follow SemVer, including prereleases and metadata precedence. */
export function compareDesktopReleases(left: string, right: string): number {
  return compareSemanticVersions(left.replace(/^v/u, ""), right.replace(/^v/u, ""));
}

/** Preserve the original minimum-build requirement of immutable API 3 packages. */
function compareLegacyDesktopReleases(left: string, right: string): number {
  const host = `v${left.replace(/^v/u, "")}`;
  if (desktopRelease.test(host) && desktopRelease.test(right)) {
    return compareNumbers(releaseParts(host), releaseParts(right));
  }
  const order = compareDesktopReleases(left, right);
  // Arbitrary SemVer metadata cannot establish a legacy minimum build number.
  return order === 0 && releaseParts(right)[3]! > 0n ? -1 : order;
}

function compareTargetVersions(left: string, right: string): number {
  return compareNumbers(
    parseTargetVersion(left)
      .split(".")
      .map((part) => BigInt(part)),
    parseTargetVersion(right)
      .split(".")
      .map((part) => BigInt(part)),
  );
}

export function parsePluginCompatibility(
  value: unknown,
  apiVersion: 3 | 4 = 3,
): PluginCompatibility {
  const input = object(value, ["streamskope", "target"]);
  const streamskope = object(
    input.streamskope,
    apiVersion === 3 ? ["minimum"] : ["minimum", "maximumExclusive"],
  );
  const target = object(input.target, ["system", "minimum", "maximum"]);
  if (typeof streamskope.minimum !== "string")
    throw new Error("Missing StreamSkope minimum release.");
  let maximumExclusive: string | undefined;
  if (apiVersion === 3) releaseParts(streamskope.minimum);
  else {
    parseReleaseVersion(streamskope.minimum);
    maximumExclusive = parseReleaseVersion(streamskope.maximumExclusive);
    if (compareSemanticVersions(streamskope.minimum, maximumExclusive) >= 0) {
      throw new Error(
        "Plugin host compatibility must have an inclusive minimum below its exclusive maximum.",
      );
    }
  }
  if (
    typeof target.system !== "string" ||
    target.system.length > 32 ||
    !systemIdentifier.test(target.system)
  ) {
    throw new Error("Invalid plugin target system identifier.");
  }
  const minimum = parseTargetVersion(target.minimum);
  const maximum = parseTargetVersion(target.maximum);
  if (compareTargetVersions(minimum, maximum) > 0) {
    throw new Error("Plugin target compatibility range is reversed.");
  }
  return {
    streamskope: {
      minimum: streamskope.minimum,
      ...(maximumExclusive === undefined ? {} : { maximumExclusive }),
    },
    target: { system: target.system, minimum, maximum },
  };
}

function parseRevision(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new Error("Plugin revision must be a positive safe integer.");
  }
  return value;
}

/** Legacy API 3 identity. New API 4 packages use independent Semantic Versions. */
export function formatPluginVersion(compatibility: PluginCompatibility, revision: number): string {
  const parsed = parsePluginCompatibility(compatibility);
  const { system, minimum, maximum } = parsed.target;
  const value = `${parsed.streamskope.minimum}--${system}-${minimum}-${maximum}--r${parseRevision(revision)}`;
  if (value.length > 256) throw new Error("Plugin version exceeds its length limit.");
  return value;
}

function parseIdentity(
  value: string,
): { compatibility: PluginCompatibility; revision: number } | null {
  const match = value.length <= 256 ? identity.exec(value) : null;
  if (match === null) return null;
  const result = {
    compatibility: parsePluginCompatibility({
      streamskope: { minimum: match[1] },
      target: { system: match[2], minimum: match[3], maximum: match[4] },
    }),
    revision: parseRevision(Number(match[5])),
  };
  if (formatPluginVersion(result.compatibility, result.revision) !== value) {
    throw new Error("Plugin version must use its canonical compatibility identity.");
  }
  return result;
}

/** Compare legacy API 2/3 versions; API 4 migration must use comparePluginManifests. */
export function comparePluginVersions(left: string, right: string): number {
  const aIdentity = parseIdentity(left);
  const bIdentity = parseIdentity(right);
  if (aIdentity !== null || bIdentity !== null) {
    if (aIdentity === null || bIdentity === null) {
      parseSemanticPluginVersion(aIdentity === null ? left : right);
      return aIdentity === null ? -1 : 1;
    }
    if (aIdentity.compatibility.target.system !== bIdentity.compatibility.target.system) {
      throw new Error("Cannot compare plugin versions for different target systems.");
    }
    if (aIdentity.revision === bIdentity.revision) {
      if (left !== right)
        throw new Error("Plugin revision has conflicting compatibility identities.");
      return 0;
    }
    return aIdentity.revision < bIdentity.revision ? -1 : 1;
  }
  return compareSemanticVersions(left, right);
}

/** SemVer 2.0 precedence, without imposing JavaScript's safe-integer limit on identifiers. */
export function compareSemanticVersions(left: string, right: string): number {
  function parts(value: string): { core: readonly bigint[]; prerelease: readonly string[] } {
    const validated = parseSemanticPluginVersion(value).split("+")[0]!;
    const separator = validated.indexOf("-");
    const core = separator === -1 ? validated : validated.slice(0, separator);
    return {
      core: core.split(".").map((part) => BigInt(part)),
      prerelease: separator === -1 ? [] : validated.slice(separator + 1).split("."),
    };
  }
  const a = parts(left);
  const b = parts(right);
  const coreOrder = compareNumbers(a.core, b.core);
  if (coreOrder !== 0) return coreOrder;
  if (a.prerelease.length === 0 || b.prerelease.length === 0) {
    return a.prerelease.length === b.prerelease.length ? 0 : a.prerelease.length === 0 ? 1 : -1;
  }
  for (let index = 0; index < Math.max(a.prerelease.length, b.prerelease.length); index += 1) {
    const x = a.prerelease[index];
    const y = b.prerelease[index];
    if (x === y) continue;
    if (x === undefined || y === undefined) return x === undefined ? -1 : 1;
    const xNumeric = /^\d+$/u.test(x);
    const yNumeric = /^\d+$/u.test(y);
    if (xNumeric && yNumeric) return BigInt(x) < BigInt(y) ? -1 : 1;
    if (xNumeric !== yNumeric) return xNumeric ? -1 : 1;
    return x < y ? -1 : 1;
  }
  return 0;
}

/** A one-way API 4 migration is independent of legacy target numbers or revision counters. */
export function comparePluginManifests(left: PluginManifest, right: PluginManifest): number {
  if (left.id !== right.id) throw new Error("Cannot compare versions of different plugins.");
  if (left.apiVersion === 4 || right.apiVersion === 4) {
    if (left.apiVersion !== right.apiVersion) return left.apiVersion === 4 ? 1 : -1;
    return compareSemanticVersions(left.version, right.version);
  }
  return comparePluginVersions(left.version, right.version);
}

export function isPrereleaseVersion(version: string): boolean {
  return parseSemanticPluginVersion(version).split("+")[0]!.includes("-");
}

export function isPluginCompatibleWithHost(manifest: PluginManifest, release: string): boolean {
  // Legacy API 2 has no host-build declaration; its original API compatibility remains valid.
  if (manifest.apiVersion === 2) return true;
  const host = manifest.compatibility?.streamskope;
  if (host === undefined) return false;
  if (manifest.apiVersion === 3) return compareLegacyDesktopReleases(release, host.minimum) >= 0;
  const version = release.replace(/^v/u, "");
  // Preview hosts need an explicitly qualified preview minimum for that same core release.
  // A numeric upper bound alone must not admit an untested future-minor release candidate.
  if (
    isPrereleaseVersion(version) &&
    (!isPrereleaseVersion(host.minimum) || version.split("-")[0] !== host.minimum.split("-")[0])
  )
    return false;
  return (
    host.maximumExclusive !== undefined &&
    compareDesktopReleases(release, host.minimum) >= 0 &&
    compareDesktopReleases(release, host.maximumExclusive) < 0
  );
}

export function isTargetVersionCompatible(manifest: PluginManifest, version: string): boolean {
  if (!targetVersion.test(version) || version.length > 64) return false;
  const target = manifest.compatibility?.target;
  if (target === undefined)
    return manifest.apiVersion === 2 && manifest.targetEdaVersion === version;
  return (
    compareTargetVersions(version, target.minimum) >= 0 &&
    compareTargetVersions(version, target.maximum) <= 0
  );
}
