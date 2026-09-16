import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

export async function prepareUnsignedRelease(
  directory: string,
  version: string,
  commit: string,
): Promise<string> {
  if (
    !/^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$/u.test(version) ||
    !/^[a-f0-9]{40}$/u.test(commit)
  ) {
    throw new Error("Invalid unsigned release version or source commit.");
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
  const lines: string[] = [];
  for (const name of expected) {
    const path = join(directory, name);
    const stat = await lstat(path);
    if (!stat.isFile() || stat.size === 0 || stat.size >= 2 * 1024 ** 3) {
      throw new Error(`Release asset must be a nonempty regular file under 2 GiB: ${name}`);
    }
    const hash = createHash("sha256");
    for await (const chunk of createReadStream(path)) hash.update(chunk as Buffer);
    lines.push(`${hash.digest("hex")}  ${name}`);
  }
  await writeFile(join(directory, "SHA256SUMS"), `${lines.join("\n")}\n`, { flag: "wx" });
  return `# StreamSkope ${version}

A desktop Kafka workbench for exploring messages, consumer groups, schemas and cluster operations.

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

Source: ${commit} (tag v${version}). Full source, real-Kafka and native packaged-app
checks ran before assembly. Packaged-app launch checks are not manual installer UX
or signing/notarization acceptance. Kafka is not bundled with the application.
`;
}
