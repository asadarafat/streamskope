import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { inflateRawSync } from "node:zlib";

const END_OF_CENTRAL_DIRECTORY = 0x06054b50;
const CENTRAL_DIRECTORY_ENTRY = 0x02014b50;
const LOCAL_FILE_HEADER = 0x04034b50;
const MAXIMUM_ZIP_ENTRY_BYTES = 256 * 1_048_576;
const MAXIMUM_ZIP_TOTAL_BYTES = 512 * 1_048_576;

export const PRIVATE_RUNBOOK_ARTIFACT_SENTINEL =
  "https://private-runbook.invalid/streamskope/latency-sentinel";
export const RULE_EXPRESSION_ARTIFACT_SENTINEL = '$.source == "streamskope-fixture"';

async function artifactFiles(root: string): Promise<readonly string[]> {
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return [];
    }
    throw error;
  }

  const files: string[] = [];
  for (const entry of entries) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await artifactFiles(path)));
    } else if (entry.isFile()) {
      files.push(path);
    }
  }
  return files;
}

function boundedSlice(value: Buffer, start: number, length: number, description: string): Buffer {
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(length) ||
    start < 0 ||
    length < 0 ||
    start + length > value.byteLength
  ) {
    throw new Error(`ZIP ${description} exceeds the artifact bounds.`);
  }
  return value.subarray(start, start + length);
}

function endOfCentralDirectory(archive: Buffer): number {
  const minimum = Math.max(0, archive.byteLength - 65_557);
  for (let offset = archive.byteLength - 22; offset >= minimum; offset -= 1) {
    if (archive.readUInt32LE(offset) === END_OF_CENTRAL_DIRECTORY) {
      return offset;
    }
  }
  throw new Error("ZIP artifact has no supported central-directory footer.");
}

function zipContains(archive: Buffer, needles: readonly Buffer[]): boolean {
  const footer = endOfCentralDirectory(archive);
  const disk = archive.readUInt16LE(footer + 4);
  const centralDisk = archive.readUInt16LE(footer + 6);
  const entriesOnDisk = archive.readUInt16LE(footer + 8);
  const entryCount = archive.readUInt16LE(footer + 10);
  const centralOffset = archive.readUInt32LE(footer + 16);
  if (disk !== 0 || centralDisk !== 0 || entriesOnDisk !== entryCount) {
    throw new Error("Multi-disk ZIP artifacts are not supported by the sentinel scan.");
  }
  if (entryCount === 0xffff || centralOffset === 0xffffffff) {
    throw new Error("ZIP64 artifacts are not supported by the sentinel scan.");
  }

  let centralCursor = centralOffset;
  let expandedBytes = 0;
  for (let index = 0; index < entryCount; index += 1) {
    const central = boundedSlice(archive, centralCursor, 46, "central-directory entry");
    if (central.readUInt32LE(0) !== CENTRAL_DIRECTORY_ENTRY) {
      throw new Error("ZIP artifact contains an invalid central-directory entry.");
    }
    const flags = central.readUInt16LE(8);
    const compression = central.readUInt16LE(10);
    const compressedBytes = central.readUInt32LE(20);
    const expandedEntryBytes = central.readUInt32LE(24);
    const fileNameBytes = central.readUInt16LE(28);
    const extraBytes = central.readUInt16LE(30);
    const commentBytes = central.readUInt16LE(32);
    const localOffset = central.readUInt32LE(42);
    if ((flags & 0x1) !== 0) {
      throw new Error("Encrypted ZIP entries cannot be sentinel-scanned.");
    }
    if (
      compressedBytes === 0xffffffff ||
      expandedEntryBytes === 0xffffffff ||
      localOffset === 0xffffffff
    ) {
      throw new Error("ZIP64 entries are not supported by the sentinel scan.");
    }
    if (expandedEntryBytes > MAXIMUM_ZIP_ENTRY_BYTES) {
      throw new Error("ZIP entry exceeds the sentinel-scan expansion bound.");
    }
    expandedBytes += expandedEntryBytes;
    if (expandedBytes > MAXIMUM_ZIP_TOTAL_BYTES) {
      throw new Error("ZIP artifact exceeds the sentinel-scan expansion bound.");
    }

    const local = boundedSlice(archive, localOffset, 30, "local-file header");
    if (local.readUInt32LE(0) !== LOCAL_FILE_HEADER) {
      throw new Error("ZIP artifact contains an invalid local-file header.");
    }
    const localNameBytes = local.readUInt16LE(26);
    const localExtraBytes = local.readUInt16LE(28);
    const dataOffset = localOffset + 30 + localNameBytes + localExtraBytes;
    const compressed = boundedSlice(archive, dataOffset, compressedBytes, "compressed entry");
    let expanded: Buffer;
    if (compression === 0) {
      expanded = compressed;
    } else if (compression === 8) {
      expanded = inflateRawSync(compressed, {
        maxOutputLength: MAXIMUM_ZIP_ENTRY_BYTES,
      });
    } else {
      throw new Error(`ZIP compression method ${compression} cannot be sentinel-scanned.`);
    }
    if (expanded.byteLength !== expandedEntryBytes) {
      throw new Error("ZIP entry expansion size does not match its directory record.");
    }
    if (needles.some((needle) => expanded.includes(needle))) {
      return true;
    }
    centralCursor += 46 + fileNameBytes + extraBytes + commentBytes;
  }
  return false;
}

export async function findSensitiveArtifactPaths(
  roots: readonly string[],
  sensitiveValues: readonly string[],
): Promise<readonly string[]> {
  const needles = [...new Set(sensitiveValues)]
    .filter((value) => value.length > 0)
    .map((value) => Buffer.from(value, "utf8"));
  const files = (await Promise.all(roots.map((root) => artifactFiles(root)))).flat();
  const matches: string[] = [];

  for (const file of files) {
    const contents = await readFile(file);
    if (
      needles.some((needle) => contents.includes(needle)) ||
      (file.toLowerCase().endsWith(".zip") && zipContains(contents, needles))
    ) {
      matches.push(file);
    }
  }
  return matches.sort();
}

interface FixtureSensitiveValues {
  readonly oauthClientSecret: string;
  readonly seedPayload: string;
}

function parseFixtureSensitiveValues(value: unknown): FixtureSensitiveValues {
  if (
    value === null ||
    typeof value !== "object" ||
    !("oauthClientSecret" in value) ||
    typeof value.oauthClientSecret !== "string" ||
    value.oauthClientSecret.length === 0 ||
    !("seedPayload" in value) ||
    typeof value.seedPayload !== "string" ||
    value.seedPayload.length === 0
  ) {
    throw new Error("Fixture configuration does not contain the required sensitive sentinels.");
  }
  return {
    oauthClientSecret: value.oauthClientSecret,
    seedPayload: value.seedPayload,
  };
}

export interface SensitiveArtifactScanPolicy {
  readonly roots: readonly string[];
  readonly sensitiveValues: readonly string[];
}

export async function repositoryArtifactScanPolicy(
  repositoryRoot: string,
): Promise<SensitiveArtifactScanPolicy> {
  const config = parseFixtureSensitiveValues(
    JSON.parse(
      await readFile(join(repositoryRoot, "aio-kafka", "fixture.config.json"), "utf8"),
    ) as unknown,
  );
  return {
    roots: [
      join(repositoryRoot, "test-results"),
      join(repositoryRoot, "playwright-report"),
      join(repositoryRoot, "artifacts", "host-logs"),
      join(repositoryRoot, "dist", "renderer"),
      join(repositoryRoot, "dist", "electron"),
      join(repositoryRoot, "dist", "performance"),
      join(repositoryRoot, "dist", "package"),
      join(repositoryRoot, "dist", "release"),
    ],
    sensitiveValues: [
      config.oauthClientSecret,
      `${config.oauthClientSecret}-invalid`,
      config.seedPayload,
      RULE_EXPRESSION_ARTIFACT_SENTINEL,
      PRIVATE_RUNBOOK_ARTIFACT_SENTINEL,
    ],
  };
}

export async function scanRepositoryArtifacts(): Promise<void> {
  const repositoryRoot = process.cwd();
  const policy = await repositoryArtifactScanPolicy(repositoryRoot);
  const matches = await findSensitiveArtifactPaths(policy.roots, policy.sensitiveValues);
  if (matches.length > 0) {
    process.stderr.write(
      `Sensitive test values were retained in ${matches.length} artifact(s):\n${matches
        .map((path) => `- ${path}`)
        .join("\n")}\n`,
    );
    process.exitCode = 1;
    return;
  }
  process.stdout.write(
    "Test evidence and packaged application payloads contain no sensitive sentinel bytes.\n",
  );
}
