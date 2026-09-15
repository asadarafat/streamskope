const ALLOWED_LICENSES = new Set([
  "0BSD",
  "Apache-2.0",
  "BSD-2-Clause",
  "BSD-3-Clause",
  "(BSD-3-Clause OR GPL-2.0)",
  "BlueOak-1.0.0",
  "CC0-1.0",
  "ISC",
  "MIT",
  "MIT-0",
  "MPL-2.0",
  "Unlicense",
]);

const EXACT_VERSION =
  /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/u;
const SHA512_INTEGRITY = /^sha512-[A-Za-z0-9+/]{86}==$/u;
const REVIEWED_REGISTRY = "https://registry.npmjs.org/";
const REVIEWED_LICENSE_OVERRIDES = new Map<string, string>([
  ["node_modules/buildcheck@0.0.7", "MIT"],
  ["node_modules/cpu-features@0.0.10", "MIT"],
  ["node_modules/ssh2@1.17.0", "MIT"],
] as const);

// Reviewed from the installed license files; these are unchanged build tools,
// excluded from the packaged production dependency graph. Keep original licenses
// and notices; an upgrade or promotion to runtime requires a fresh review.
// SPDX texts: https://spdx.org/licenses/Python-2.0.html and /WTFPL.html.
const REVIEWED_BUILD_TOOL_LICENSES = new Map<string, string>([
  ["node_modules/argparse@2.0.1", "Python-2.0"],
  ["node_modules/sanitize-filename@1.6.4", "WTFPL OR ISC"],
  ["node_modules/truncate-utf8-bytes@1.0.2", "WTFPL"],
  ["node_modules/type-fest@0.13.1", "(MIT OR CC0-1.0)"],
  ["node_modules/utf8-byte-length@1.0.5", "(WTFPL OR MIT)"],
]);

export interface DependencyPolicyResult {
  readonly directDependencyCount: number;
  readonly licenses: readonly string[];
  readonly packageCount: number;
}

function asRecord(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${name} must be an object.`);
  }
  return value as Record<string, unknown>;
}

function optionalRecord(value: unknown, name: string): Readonly<Record<string, unknown>> {
  return value === undefined ? {} : asRecord(value, name);
}

function directDependencies(
  manifest: Readonly<Record<string, unknown>>,
): ReadonlyMap<string, string> {
  const direct = new Map<string, string>();
  for (const groupName of ["dependencies", "devDependencies"] as const) {
    const group = optionalRecord(manifest[groupName], `package manifest ${groupName}`);
    for (const [name, value] of Object.entries(group)) {
      if (typeof value !== "string" || !EXACT_VERSION.test(value)) {
        throw new Error(`${name} must use an exact version.`);
      }
      if (direct.has(name)) {
        throw new Error(`${name} must have one dependency owner.`);
      }
      direct.set(name, value);
    }
  }
  return direct;
}

function sameEntries(
  left: Readonly<Record<string, unknown>>,
  right: Readonly<Record<string, unknown>>,
): boolean {
  const leftEntries = Object.entries(left);
  return (
    leftEntries.length === Object.keys(right).length &&
    leftEntries.every(([key, value]) => right[key] === value)
  );
}

export function assertDependencyPolicy(
  manifestValue: unknown,
  lockValue: unknown,
): DependencyPolicyResult {
  const manifest = asRecord(manifestValue, "package manifest");
  const lock = asRecord(lockValue, "package lock");
  if (lock.lockfileVersion !== 3) {
    throw new Error("package-lock.json must use lockfile version 3.");
  }
  const direct = directDependencies(manifest);
  const packages = asRecord(lock.packages, "package lock packages");
  const root = asRecord(packages[""], "package lock root");
  const licenses = new Set<string>();
  let packageCount = 0;

  for (const groupName of ["dependencies", "devDependencies"] as const) {
    const rootGroup = optionalRecord(root[groupName], `package lock root ${groupName}`);
    const manifestGroup = optionalRecord(manifest[groupName], `package manifest ${groupName}`);
    if (!sameEntries(rootGroup, manifestGroup)) {
      throw new Error(`package-lock.json ${groupName} does not match package.json.`);
    }
  }

  for (const [path, value] of Object.entries(packages)) {
    if (path.length === 0) {
      continue;
    }
    packageCount += 1;
    const lockedPackage = asRecord(value, path);
    const { integrity, license, resolved, version } = lockedPackage;

    if (typeof version !== "string" || version.length === 0) {
      throw new Error(`${path} has no version.`);
    }
    if (typeof integrity !== "string" || !SHA512_INTEGRITY.test(integrity)) {
      throw new Error(`${path} has no SHA-512 integrity.`);
    }
    if (typeof resolved !== "string" || !resolved.startsWith(REVIEWED_REGISTRY)) {
      throw new Error(`${path} does not resolve from the reviewed npm registry.`);
    }
    const reviewedLicense =
      typeof license === "string" ? license : REVIEWED_LICENSE_OVERRIDES.get(`${path}@${version}`);
    const reviewedBuildTool =
      lockedPackage.dev === true &&
      typeof license === "string" &&
      REVIEWED_BUILD_TOOL_LICENSES.get(`${path}@${version}`) === license;
    if (
      reviewedLicense === undefined ||
      (!ALLOWED_LICENSES.has(reviewedLicense) && !reviewedBuildTool)
    ) {
      throw new Error(`${path} uses unreviewed license ${String(license)}.`);
    }
    licenses.add(reviewedLicense);

    const packageName = path.slice("node_modules/".length);
    const expectedVersion = direct.get(packageName);
    if (expectedVersion !== undefined && path === `node_modules/${packageName}`) {
      if (version !== expectedVersion) {
        throw new Error(
          `${packageName} resolves to ${version}, not declared version ${expectedVersion}.`,
        );
      }
    }
  }

  for (const name of direct.keys()) {
    if (packages[`node_modules/${name}`] === undefined) {
      throw new Error(`package-lock.json is missing direct dependency ${name}.`);
    }
  }

  return {
    directDependencyCount: direct.size,
    licenses: [...licenses].sort(),
    packageCount,
  };
}
