import { writeFile } from "node:fs/promises";

import { prepareUnsignedRelease } from "./unsigned-release-assets";

async function main(): Promise<void> {
  const [directory, version, commit, notes, ...extra] = process.argv.slice(2);
  if (!directory || !version || !commit || !notes || extra.length !== 0) {
    throw new Error(
      "Usage: prepare-unsigned-release <assets-directory> <version> <commit> <notes-file>",
    );
  }
  const content = await prepareUnsignedRelease(directory, version, commit);
  await writeFile(notes, content, { flag: "wx" });
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `Unsigned release assembly failed: ${error instanceof Error ? error.message : String(error)}\n`,
  );
  process.exitCode = 1;
});
