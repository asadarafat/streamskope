import { lstat, readdir } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve, sep } from "node:path";

import type { BrowserDataInspectionReason } from "./browser-data-compatibility";

export class BrowserDataInspectionError extends Error {
  constructor(readonly reason: BrowserDataInspectionReason) {
    super("Browser data inspection could not establish the required condition.");
  }
}
export interface BrowserDataFile {
  readonly type: "file" | "directory";
  readonly bytes: number;
}
export function isMissingBrowserData(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

/** Reject unsafe objects before any owner parser can follow a path or inspect its contents. */
export async function inspectBrowserDataFiles(
  root: string,
): Promise<ReadonlyMap<string, BrowserDataFile>> {
  if (
    !isAbsolute(root) ||
    root.length > 4096 ||
    dirname(resolve(root)) === resolve(root) ||
    process.getuid === undefined
  )
    throw new BrowserDataInspectionError("invalid-request");
  const absolute = resolve(root);
  let component: string = sep;
  for (const part of absolute.split(sep).filter(Boolean)) {
    component = join(component, part);
    try {
      if (!(await lstat(component)).isDirectory())
        throw new BrowserDataInspectionError("unsafe-filesystem");
    } catch (error) {
      if (isMissingBrowserData(error)) return new Map();
      throw error;
    }
  }
  const files = new Map<string, BrowserDataFile>();
  let bytes = 0;
  const walk = async (relative: string, depth: number): Promise<void> => {
    if (depth > 6 || files.size >= 8192) throw new BrowserDataInspectionError("inspection-limit");
    const path = join(absolute, relative);
    const info = await lstat(path);
    if (
      (!info.isDirectory() && !info.isFile()) ||
      info.uid !== process.getuid!() ||
      (info.mode & 0o077) !== 0 ||
      (info.isFile() && info.nlink !== 1)
    )
      throw new BrowserDataInspectionError("unsafe-filesystem");
    if (info.isFile()) {
      bytes += info.size;
      if (info.size > 64 * 1024 * 1024 || bytes > 16 * 1024 ** 3)
        throw new BrowserDataInspectionError("inspection-limit");
      files.set(relative, { type: "file", bytes: info.size });
    } else {
      files.set(relative, { type: "directory", bytes: 0 });
      for (const child of (await readdir(path)).sort())
        await walk(relative === "" ? child : `${relative}/${child}`, depth + 1);
    }
  };
  await walk("", 0);
  return files;
}
