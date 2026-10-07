import { parseReleaseVersion } from "../../src/plugins/compatibility";

export const BROWSER_IMAGE_REPOSITORY = "ghcr.io/asadarafat/streamskope";
export const BROWSER_IMAGE_ARCHITECTURES = ["amd64", "arm64"] as const;
export type BrowserImageArchitecture = (typeof BROWSER_IMAGE_ARCHITECTURES)[number];

export interface BrowserRegistryMetadata {
  readonly schemaVersion: 1;
  readonly version: string;
  readonly sourceRevision: string;
  readonly image: string;
  readonly reference: string;
  readonly digest: string;
  readonly platforms: readonly {
    readonly platform: `linux/${BrowserImageArchitecture}`;
    readonly manifestDigest: string;
    readonly imageId: string;
  }[];
}

export function browserRegistryIdentity(version: string, commit: string): void {
  parseReleaseVersion(version);
  if (
    version === "0.0.0" ||
    version.startsWith("0.0.0-") ||
    !/^[a-f0-9]{40}$/u.test(commit) ||
    version.length > 80
  )
    throw new Error(
      "Browser registry publication requires a release version and exact source commit.",
    );
}

export function browserRegistryObject(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Browser registry metadata must contain an object.");
  return value as Record<string, unknown>;
}

export function browserRegistryDigest(value: unknown): string {
  if (typeof value !== "string" || !/^sha256:[a-f0-9]{64}$/u.test(value))
    throw new Error("Browser registry metadata contains an invalid digest.");
  return value;
}

function exactKeys(record: Record<string, unknown>, keys: readonly string[]): void {
  if (
    Object.keys(record).length !== keys.length ||
    Object.keys(record).some((key) => !keys.includes(key))
  )
    throw new Error("Browser registry metadata contains unsupported fields.");
}

/** A public receipt binds the promoted immutable index to both qualified native images. */
export function parseBrowserRegistryMetadata(
  value: unknown,
  version: string,
  commit: string,
): BrowserRegistryMetadata {
  browserRegistryIdentity(version, commit);
  const record = browserRegistryObject(value);
  exactKeys(record, [
    "schemaVersion",
    "version",
    "sourceRevision",
    "image",
    "reference",
    "digest",
    "platforms",
  ]);
  const digest = browserRegistryDigest(record.digest);
  const image = `${BROWSER_IMAGE_REPOSITORY}:${version}`;
  if (
    record.schemaVersion !== 1 ||
    record.version !== version ||
    record.sourceRevision !== commit ||
    record.image !== image ||
    record.reference !== `${image}@${digest}` ||
    !Array.isArray(record.platforms) ||
    record.platforms.length !== BROWSER_IMAGE_ARCHITECTURES.length
  )
    throw new Error(
      "Browser registry receipt does not match the release, source, or platform set.",
    );
  const suppliedPlatforms = record.platforms;
  const platforms = BROWSER_IMAGE_ARCHITECTURES.map((architecture, index) => {
    const platform = browserRegistryObject(suppliedPlatforms[index]);
    exactKeys(platform, ["platform", "manifestDigest", "imageId"]);
    if (platform.platform !== `linux/${architecture}`)
      throw new Error(
        "Browser registry receipt must contain exactly Linux AMD64 and ARM64 in order.",
      );
    return {
      platform: `linux/${architecture}` as const,
      manifestDigest: browserRegistryDigest(platform.manifestDigest),
      imageId: browserRegistryDigest(platform.imageId),
    };
  });
  if (
    platforms[0]!.manifestDigest === platforms[1]!.manifestDigest ||
    platforms[0]!.imageId === platforms[1]!.imageId
  )
    throw new Error("Browser registry receipt contains duplicate native image identities.");
  return {
    schemaVersion: 1,
    version,
    sourceRevision: commit,
    image,
    reference: `${image}@${digest}`,
    digest,
    platforms,
  };
}
