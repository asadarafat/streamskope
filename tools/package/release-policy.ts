import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readFile, readdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { parseReleaseVersion } from "../../src/plugins/compatibility";

import { validateBrowserReleaseAssets } from "./browser-release";

export function parsePublicationVersion(version: string): string {
  const parsed = parseReleaseVersion(version);
  if (parsed === "0.0.0" || parsed.startsWith("0.0.0-"))
    throw new Error("The 0.0.0 namespace is reserved for development builds.");
  return parsed;
}

export function isDesktopReleaseTag(tag: string | undefined, version: string): boolean {
  try {
    return tag === `v${parsePublicationVersion(version)}`;
  } catch {
    return false;
  }
}

/** Development status instructions and empty highlights are never published notes. */
export function reviewedReleaseCommentary(source: string): string {
  const opening = "<!-- development-release-status -->";
  const closing = "<!-- /development-release-status -->";
  const openings = source.split(opening).length - 1;
  const closings = source.split(closing).length - 1;
  if (
    openings !== closings ||
    openings > 1 ||
    (openings && source.indexOf(opening) > source.indexOf(closing))
  )
    throw new Error("Development release status needs one complete bounded block.");
  return source
    .replace(
      /<!-- development-release-status -->[\s\S]*?<!-- \/development-release-status -->\s*/u,
      "",
    )
    .replace(
      /^## Release highlights\r?\n\s*No additional release highlights recorded\.\s*(?=^## |$(?![\s\S]))/mu,
      "",
    )
    .trim();
}

export async function prepareUnsignedRelease(
  directory: string,
  version: string,
  commit: string,
  tag = `v${version}`,
  containerDirectory?: string,
): Promise<string> {
  if (!isDesktopReleaseTag(tag, version) || !/^[a-f0-9]{40}$/u.test(commit)) {
    throw new Error("Invalid unsigned release tag, version, or source commit.");
  }
  const expected = [
    `StreamSkope-${version}-darwin-arm64.dmg`,
    `StreamSkope-${version}-linux-x64.AppImage`,
    `StreamSkope-${version}-win32-x64-Setup.exe`,
  ];
  const actual = (await readdir(directory)).sort();
  if (actual.length !== expected.length || actual.some((name, index) => name !== expected[index])) {
    throw new Error("Unsigned release must contain exactly the three version-matched installers.");
  }
  const browserNames =
    containerDirectory === undefined
      ? []
      : await validateBrowserReleaseAssets(containerDirectory, version, commit);
  const browser =
    containerDirectory === undefined
      ? undefined
      : (JSON.parse(
          await readFile(join(containerDirectory, `streamskope-${version}-container.json`), "utf8"),
        ) as { registry?: { reference: string }; installer?: { file: string } });
  const registry = browser?.registry;
  const lines: string[] = [];
  for (const [assetDirectory, names] of [
    [directory, expected],
    [containerDirectory, browserNames],
  ] as const) {
    if (assetDirectory === undefined) continue;
    for (const name of names) {
      const path = join(assetDirectory, name);
      const stat = await lstat(path);
      if (!stat.isFile() || stat.size === 0 || stat.size >= 2 * 1024 ** 3) {
        throw new Error(`Release asset must be a nonempty regular file under 2 GiB: ${name}`);
      }
      const hash = createHash("sha256");
      for await (const chunk of createReadStream(path)) hash.update(chunk as Buffer);
      lines.push(`${hash.digest("hex")}  ${name}`);
    }
  }
  await writeFile(join(directory, "SHA256SUMS"), `${lines.join("\n")}\n`, { flag: "wx" });
  return `# StreamSkope ${version}

A workbench for inspecting Kafka and NATS streams and their supported operations.

## Downloads

| Platform | File |
| --- | --- |
| macOS (Apple Silicon) | StreamSkope-${version}-darwin-arm64.dmg |
| Windows x64 | StreamSkope-${version}-win32-x64-Setup.exe |
| Linux x64 | StreamSkope-${version}-linux-x64.AppImage |

## Signing and installation

These downloads are unsigned. macOS has no Developer ID signature and is not notarized;
Windows is not Authenticode-signed; Linux is not publisher-signed.
Operating-system warnings are expected. Verify the trusted download against SHA256SUMS
before installing. Checksums verify integrity, not publisher identity or malware safety.
Do not disable operating-system security globally.

[Installation and checksum instructions](https://asadarafat.github.io/streamskope/start/installation/)

${
  containerDirectory === undefined
    ? ""
    : `## Browser with Containerlab

| Linux Docker host | Docker save archive |
| --- | --- |
| AMD64 | StreamSkope-${version}-container-linux-amd64.tar.gz |
| ARM64 | StreamSkope-${version}-container-linux-arm64.tar.gz |

${
  browser?.installer !== undefined
    ? `Install on your Linux host or Linux VM, then open the URL printed by the installer:

\`\`\`sh
curl -fsSL https://github.com/asadarafat/streamskope/releases/download/${tag}/${browser.installer.file} | sudo -E bash
\`\`\`

The installer verifies the matching release metadata, pulls the public version/digest-pinned
GHCR image, and preserves private vault data on repeat runs. No source build is required.
Manual and offline delivery remain available through the matching topology and image archives.
No Apple Developer ID certificate is needed.`
    : registry === undefined
      ? `Download the matching archive, streamskope-${version}.clab.yml and
streamskope-${version}-container.json. Verify all files against SHA256SUMS,
load the gzip-compressed Docker save archive with Docker, then deploy the topology
with Containerlab. No image registry or Apple Developer ID certificate is needed.`
      : `Download streamskope-${version}.clab.yml and verify it against SHA256SUMS, then
deploy with Containerlab. Its public GHCR image is pinned to ${registry.reference};
Docker selects AMD64 or ARM64. No registry login is required.
For offline use, download the matching gzip-compressed Docker save archive,
streamskope-${version}-offline.clab.yml and streamskope-${version}-container.json.
Verify those files against SHA256SUMS, load the archive and deploy the offline topology.
Both editions use the same qualified image and private data directory.
No Apple Developer ID certificate is needed.`
}
The browser stores credentials in an encrypted vault that you unlock with your
passphrase; protect that passphrase and the private data directory.

[Containerlab installation and backup instructions](https://asadarafat.github.io/streamskope/start/containerlab/)

`
}
Source: ${commit} (tag ${tag}). Native packaged-app checks ran during this build.
Local qualification is a maintainer prerequisite. Kafka is not bundled with the application.
`;
}

/** Returns the reviewed Markdown body from a versioned Zensical release page. */
export function releaseNotesBody(source: string, version: string, tag = `v${version}`): string {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n([\s\S]*)$/u.exec(source);
  if (!match?.[1] || match[2] === undefined) {
    throw new Error("Release notes must be a versioned Markdown page with YAML frontmatter.");
  }
  const versionLines = match[1].match(/^release_version:\s*['"]?([^'"\s]+)['"]?\s*$/gmu) ?? [];
  const pageVersion = /^release_version:\s*['"]?([^'"\s]+)['"]?\s*$/mu.exec(match[1])?.[1];
  if (versionLines.length !== 1 || pageVersion !== version) {
    throw new Error("Release notes frontmatter must identify the exact release version.");
  }
  const tagLines = match[1].match(/^release_tag:.*$/gmu) ?? [];
  const pageTag = /^release_tag:\s*['"]?([^'"\s]+)['"]?\s*$/mu.exec(match[1])?.[1];
  if (!isDesktopReleaseTag(tag, version) || tagLines.length !== 1 || pageTag !== tag) {
    throw new Error("Release notes must identify the exact release tag.");
  }
  const body = match[2];
  if (!body.trim() || !/^#\s+.+/mu.test(body)) {
    throw new Error("Release notes must contain a heading and reviewed release content.");
  }
  return body;
}
