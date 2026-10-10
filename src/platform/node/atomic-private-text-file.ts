import { chmod, mkdir, open, rename, unlink } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

export function createAtomicPrivateFileTempId(): string {
  return globalThis.crypto.randomUUID();
}

/** Confirm a published directory entry on platforms with directory fsync support. */
export async function syncPrivateFileDirectory(directory: string): Promise<void> {
  if (process.platform === "win32") return;
  const handle = await open(directory, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

export interface AtomicPrivateTextFileInput {
  readonly beforeCommit?: () => Promise<void>;
  readonly contents: string;
  readonly createTempId: () => string;
  readonly path: string;
  readonly signal?: AbortSignal;
}

export async function writeAtomicPrivateTextFile(input: AtomicPrivateTextFileInput): Promise<void> {
  const directory = dirname(input.path);
  await mkdir(directory, { mode: 0o700, recursive: true });
  await chmod(directory, 0o700);
  const temporaryPath = join(directory, `.${basename(input.path)}.${input.createTempId()}.tmp`);
  let createdTemporary = false;
  let handle: Awaited<ReturnType<typeof open>> | undefined;
  try {
    handle = await open(temporaryPath, "wx", 0o600);
    createdTemporary = true;
    await handle.writeFile(input.contents, "utf8");
    await handle.sync();
    await handle.close();
    handle = undefined;
    await input.beforeCommit?.();
    input.signal?.throwIfAborted();
    await rename(temporaryPath, input.path);
    createdTemporary = false;
  } catch (error) {
    await handle?.close().catch(() => undefined);
    if (createdTemporary) {
      await unlink(temporaryPath).catch(() => undefined);
    }
    throw error;
  }
}
