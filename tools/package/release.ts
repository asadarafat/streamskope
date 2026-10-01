import { readFile, writeFile } from "node:fs/promises";

import { prepareUnsignedRelease, releaseNotesBody } from "./release-policy";

async function main(): Promise<void> {
  const [directory, version, commit, source, output, tag, ...extra] = process.argv.slice(2);
  if (!directory || !version || !commit || !source || !output || extra.length) {
    throw new Error(
      "Usage: npm run package -- release <assets-directory> <version> <commit> <source-page> <notes-output> [tag]",
    );
  }
  const body = releaseNotesBody(await readFile(source, "utf8"), version, tag);
  const downloads = await prepareUnsignedRelease(directory, version, commit, tag);
  await writeFile(output, `${body.trimEnd()}\n\n${downloads}`, { flag: "wx", mode: 0o600 });
  process.stdout.write("Prepared reviewed release notes, download notices and SHA256SUMS.\n");
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `Release preparation failed: ${error instanceof Error ? error.message : String(error)}\n`,
  );
  process.exitCode = 1;
});
