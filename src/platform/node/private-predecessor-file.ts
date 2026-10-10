import { constants, type Stats } from "node:fs";
import { link, lstat, open, unlink } from "node:fs/promises";
import { dirname } from "node:path";

import { createAtomicPrivateFileTempId } from "./atomic-private-text-file";
import { readBoundedFile } from "./bounded-file";

/** Publish an exact private predecessor without replacing an existing backup. */
export async function preservePrivatePredecessor(options: {
  readonly path: string;
  readonly bytes: Buffer;
  readonly maximumBytes: number;
  readonly syncDirectory: (directory: string) => Promise<void>;
}): Promise<void> {
  const temporary = `${options.path}.tmp-${createAtomicPrivateFileTempId()}`;
  const handle = await open(
    temporary,
    constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | (constants.O_NOFOLLOW ?? 0),
    0o600,
  );
  let created: Stats | undefined;
  try {
    created = await handle.stat();
    await handle.writeFile(options.bytes);
    await handle.sync();
    await handle.close();
    try {
      await link(temporary, options.path);
    } catch (error) {
      if (
        error === null ||
        typeof error !== "object" ||
        !("code" in error) ||
        error.code !== "EEXIST"
      )
        throw error;
    }
  } finally {
    await handle.close().catch(() => undefined);
    const current = await lstat(temporary).catch(() => undefined);
    if (created !== undefined && current?.dev === created.dev && current.ino === created.ino)
      await unlink(temporary);
  }
  const metadata = await lstat(options.path);
  if (
    !metadata.isFile() ||
    metadata.nlink !== 1 ||
    (process.platform !== "win32" && (metadata.mode & 0o777) !== 0o600)
  )
    throw new Error("The predecessor backup is unsafe.");
  const saved = await readBoundedFile(options.path, options.maximumBytes, { rejectSymlinks: true });
  if (!saved.equals(options.bytes))
    throw new Error(
      "An existing predecessor differs. Preserve complete backups before another migration.",
    );
  await options.syncDirectory(dirname(options.path));
}
