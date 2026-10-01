import { constants } from "node:fs";
import { lstat, open, stat } from "node:fs/promises";

interface BoundedFileOptions {
  readonly rejectSymlinks?: boolean;
  readonly signal?: AbortSignal | undefined;
}

/** Read from one descriptor without allocating beyond the declared byte bound. */
export async function readBoundedFile(
  path: string,
  maximumBytes: number,
  options: BoundedFileOptions = {},
): Promise<Buffer> {
  options.signal?.throwIfAborted();
  const initial = await (options.rejectSymlinks ? lstat(path) : stat(path));
  if (!initial.isFile()) throw new Error("Expected a regular file");
  if (initial.size > maximumBytes) throw new Error("File exceeds its storage bound");
  // Nonblocking open also prevents a raced replacement with a FIFO from hanging the host.
  const flags =
    constants.O_RDONLY |
    (constants.O_NONBLOCK ?? 0) |
    (options.rejectSymlinks ? (constants.O_NOFOLLOW ?? 0) : 0);
  const file = await open(path, flags);
  try {
    const metadata = await file.stat();
    if (!metadata.isFile() || metadata.size > maximumBytes)
      throw new Error("File exceeds its storage bound or is not a regular file");
    const buffer = Buffer.alloc(metadata.size + 1);
    let offset = 0;
    while (offset < buffer.length) {
      options.signal?.throwIfAborted();
      const { bytesRead } = await file.read(buffer, offset, buffer.length - offset, null);
      if (bytesRead === 0) break;
      offset += bytesRead;
    }
    options.signal?.throwIfAborted();
    if (offset !== metadata.size || (await file.stat()).size !== metadata.size)
      throw new Error("File changed during read");
    return buffer.subarray(0, offset);
  } finally {
    await file.close();
  }
}
