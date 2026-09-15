import { lstat, readdir, rm } from "node:fs/promises";
import { join } from "node:path";

import {
  supportedElectronLocaleFiles,
  type VerifiedElectronPlatform,
} from "./electron-runtime-efficiency-policy";

export interface ElectronLocalePruningEvidence {
  readonly localeFiles: readonly string[];
  readonly removedLocaleBytes: number;
}

function localeRoot(buildPath: string, platform: VerifiedElectronPlatform): string {
  return platform === "darwin"
    ? join(buildPath, "Electron.app", "Contents", "Resources")
    : join(buildPath, "locales");
}

async function entryBytes(path: string): Promise<number> {
  const metadata = await lstat(path);
  if (metadata.isSymbolicLink()) {
    throw new Error(`Electron locale resource ${path} must not be a symbolic link.`);
  }
  if (!metadata.isDirectory()) {
    return metadata.size;
  }
  let total = 0;
  for (const entry of await readdir(path)) {
    total += await entryBytes(join(path, entry));
  }
  return total;
}

export async function pruneElectronLocales(
  buildPath: string,
  platform: VerifiedElectronPlatform,
): Promise<ElectronLocalePruningEvidence> {
  const root = localeRoot(buildPath, platform);
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch (error: unknown) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`Electron ${platform} locale directory is unavailable: ${detail}`, {
      cause: error,
    });
  }

  const supported = new Set(supportedElectronLocaleFiles(platform));
  const localeEntries =
    platform === "darwin" ? entries.filter((entry) => entry.name.endsWith(".lproj")) : entries;
  const retained: string[] = [];
  let removedLocaleBytes = 0;

  for (const entry of localeEntries) {
    const path = join(root, entry.name);
    if (supported.has(entry.name)) {
      retained.push(entry.name);
      continue;
    }
    removedLocaleBytes += await entryBytes(path);
    await rm(path, { force: false, recursive: entry.isDirectory() });
  }

  return {
    localeFiles: retained.sort((left, right) => left.localeCompare(right)),
    removedLocaleBytes,
  };
}
