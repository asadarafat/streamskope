import { createHash } from "node:crypto";
import { constants } from "node:fs";
import {
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { Transform } from "node:stream";
import { isDeepStrictEqual } from "node:util";
import { createGunzip } from "node:zlib";

import { list, type ReadEntry } from "tar";

import { parseReleaseVersion } from "../../src/plugins/compatibility";
import { readBoundedFile } from "../../src/platform/node/bounded-file";

import {
  parseBrowserRegistryMetadata,
  type BrowserRegistryMetadata,
} from "./browser-registry-metadata";
import { BROWSER_INSTALLER_NAME, renderBrowserWorkbenchInstaller } from "./browser-installer";

const ARCHITECTURES = ["amd64", "arm64"] as const;
type Architecture = (typeof ARCHITECTURES)[number];
const MAX_ARCHIVE_BYTES = 2 * 1024 ** 3;
const MAX_EXPANDED_BYTES = 4 * 1024 ** 3;
const MAX_METADATA_BYTES = 128 * 1024;
const MAX_MANIFEST_BYTES = 1024 * 1024;

interface ImageRecord {
  readonly version: string;
  readonly sourceRevision: string;
  readonly image: string;
  readonly imageId: string;
  readonly platform: `linux/${Architecture}`;
  readonly archive: string;
}

interface ArchiveRecord extends ImageRecord {
  readonly bytes: number;
  readonly sha256: string;
}

export interface BrowserReleaseManifest {
  readonly schemaVersion: 1 | 2 | 3;
  readonly version: string;
  readonly sourceRevision: string;
  readonly image: string;
  readonly format: "docker-save-gzip";
  readonly topology: { readonly file: string; readonly sha256: string };
  readonly archives: readonly ArchiveRecord[];
  readonly registry?: BrowserRegistryMetadata;
  readonly offlineTopology?: { readonly file: string; readonly sha256: string };
  readonly installer?: { readonly file: typeof BROWSER_INSTALLER_NAME };
}

export function browserReleaseNames(
  version: string,
  registry = false,
  installer = false,
): readonly string[] {
  parseReleaseVersion(version);
  if (installer && !registry)
    throw new Error("Browser installers require public registry delivery.");
  return [
    ...ARCHITECTURES.map((arch) => `StreamSkope-${version}-container-linux-${arch}.tar.gz`),
    `streamskope-${version}-container.json`,
    `streamskope-${version}.clab.yml`,
    ...(registry ? [`streamskope-${version}-offline.clab.yml`] : []),
    ...(installer ? [BROWSER_INSTALLER_NAME] : []),
  ].sort();
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Browser release metadata must contain a JSON object.");
  return value as Record<string, unknown>;
}

function identity(version: string, commit: string): void {
  parseReleaseVersion(version);
  if (version === "0.0.0" || version.startsWith("0.0.0-") || !/^[a-f0-9]{40}$/u.test(commit))
    throw new Error("Browser delivery requires a publication version and exact source commit.");
}

function imageRecord(
  value: unknown,
  version: string,
  commit: string,
  arch: Architecture,
): ImageRecord {
  const record = object(value);
  const fields = ["version", "sourceRevision", "image", "imageId", "platform", "archive"];
  if (
    Object.keys(record).length !== fields.length ||
    Object.keys(record).some((key) => !fields.includes(key)) ||
    record.version !== version ||
    record.sourceRevision !== commit ||
    record.image !== `streamskope:${version}` ||
    record.platform !== `linux/${arch}` ||
    record.archive !== `StreamSkope-${version}-container-linux-${arch}.tar.gz` ||
    typeof record.imageId !== "string" ||
    !/^sha256:[a-f0-9]{64}$/u.test(record.imageId)
  )
    throw new Error("Browser image metadata does not match the release, source, or architecture.");
  return record as unknown as ImageRecord;
}

function memberPath(path: unknown): string {
  if (
    typeof path !== "string" ||
    path.length === 0 ||
    path.length > 1024 ||
    path.includes("\\") ||
    path.startsWith("/") ||
    path
      .replace(/\/$/u, "")
      .split("/")
      .some((part) => part === "" || part === "." || part === "..")
  )
    throw new Error("Docker save archive contains an invalid member path.");
  return path;
}

/** Inspect the compressed archive itself without extracting or buffering image layers. */
async function inspectArchive(path: string, record: ImageRecord): Promise<ArchiveRecord> {
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  const metadata = await file.stat();
  if (!metadata.isFile() || metadata.size === 0 || metadata.size >= MAX_ARCHIVE_BYTES) {
    await file.close();
    throw new Error("Browser archive must be a nonempty regular file under 2 GiB.");
  }
  const files = new Set<string>();
  const regularFiles = new Set<string>();
  const smallFiles = new Map<string, Buffer>();
  let expandedBytes = 0;
  let decompressedBytes = 0;
  let bufferedBytes = 0;
  let archiveBytes = 0;
  let eof = false;
  const hash = createHash("sha256");
  const source = file.createReadStream();
  const gunzip = createGunzip();
  const expansion = new Transform({
    transform(chunk: Buffer, _encoding, callback): void {
      decompressedBytes += chunk.length;
      if (decompressedBytes > MAX_EXPANDED_BYTES)
        callback(new Error("Docker save archive exceeds its expanded size limit."));
      else callback(null, chunk);
    },
  });
  const parser = list({
    strict: true,
    maxMetaEntrySize: 64 * 1024,
    onReadEntry(entry: ReadEntry) {
      try {
        const name = memberPath(entry.path);
        if (
          files.size >= 16_384 ||
          files.has(name) ||
          (entry.type !== "File" && entry.type !== "Directory")
        )
          throw new Error("Docker save archive contains duplicate, excessive, or unsafe members.");
        files.add(name);
        if (entry.type === "File") regularFiles.add(name);
        expandedBytes += entry.size;
        if (expandedBytes > MAX_EXPANDED_BYTES)
          throw new Error("Docker save archive exceeds its expanded size limit.");
        const limit = name === "manifest.json" ? MAX_MANIFEST_BYTES : MAX_METADATA_BYTES;
        if (entry.type !== "File" || entry.size > limit) return;
        bufferedBytes += entry.size;
        if (smallFiles.size >= 256 || bufferedBytes > 8 * 1024 * 1024)
          throw new Error("Docker save archive has excessive small metadata members.");
        const chunks: Buffer[] = [];
        entry.on("data", (chunk: Buffer) => chunks.push(chunk));
        entry.on("end", () => smallFiles.set(name, Buffer.concat(chunks)));
      } catch (error) {
        parser.abort(error instanceof Error ? error : new Error(String(error)));
      }
    },
  });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => parser.abort(new Error("Docker save inspection timed out.")),
      120_000,
    );
    const fail = (error: Error): void => {
      clearTimeout(timer);
      source.destroy();
      gunzip.destroy();
      expansion.destroy();
      reject(error);
    };
    source.on("error", fail);
    gunzip.on("error", fail);
    expansion.on("error", fail);
    parser.on("error", fail);
    parser.on("eof", () => {
      eof = true;
    });
    parser.on("end", () => {
      clearTimeout(timer);
      resolve();
    });
    source.on("data", (data: Buffer | string) => {
      const chunk = typeof data === "string" ? Buffer.from(data) : data;
      if (archiveBytes === 0 && (chunk[0] !== 0x1f || chunk[1] !== 0x8b))
        parser.abort(
          new Error("Browser image archives must use gzip-compressed Docker save format."),
        );
      archiveBytes += chunk.length;
      if (archiveBytes >= MAX_ARCHIVE_BYTES)
        parser.abort(new Error("Browser archive exceeds its compressed size limit."));
      hash.update(chunk);
    });
    source.pipe(gunzip).pipe(expansion).pipe(parser);
  });
  if (!eof || archiveBytes !== metadata.size)
    throw new Error("Docker save archive was truncated or changed while inspected.");
  const manifestBytes = smallFiles.get("manifest.json");
  if (manifestBytes === undefined) throw new Error("Docker save archive has no bounded manifest.");
  const manifest: unknown = JSON.parse(manifestBytes.toString("utf8"));
  if (!Array.isArray(manifest) || manifest.length !== 1)
    throw new Error("Browser archive must contain exactly one Docker image.");
  const image = object(manifest[0]);
  const tags = image.RepoTags;
  const layers = image.Layers;
  const configPath = memberPath(image.Config);
  if (
    !Array.isArray(tags) ||
    tags.length !== 1 ||
    tags[0] !== record.image ||
    !Array.isArray(layers) ||
    layers.length === 0 ||
    layers.length > 128 ||
    layers.some((layer) => !regularFiles.has(memberPath(layer)))
  )
    throw new Error("Docker save image tags or layer references do not match browser delivery.");
  const configBytes = smallFiles.get(configPath);
  if (
    configBytes === undefined ||
    `sha256:${createHash("sha256").update(configBytes).digest("hex")}` !== record.imageId
  )
    throw new Error("Docker save config does not match its image identity.");
  const config = object(JSON.parse(configBytes.toString("utf8")));
  const labels = object(object(config.config).Labels);
  if (
    config.os !== "linux" ||
    `linux/${String(config.architecture)}` !== record.platform ||
    labels["org.opencontainers.image.version"] !== record.version ||
    labels["org.opencontainers.image.revision"] !== record.sourceRevision
  )
    throw new Error("Docker save config does not match the release, source, or architecture.");
  return { ...record, bytes: archiveBytes, sha256: hash.digest("hex") };
}

/** Qualify one native archive without requiring the other architecture locally. */
export async function validateBrowserImageArchive(
  path: string,
  metadata: unknown,
  version: string,
  commit: string,
  architecture: Architecture,
): Promise<ArchiveRecord> {
  identity(version, commit);
  if (!ARCHITECTURES.includes(architecture)) throw new Error("Unsupported browser architecture.");
  return inspectArchive(path, imageRecord(metadata, version, commit, architecture));
}

/** The release topology differs from the reviewed source only in its image default. */
export function browserReleaseTopology(source: string, version: string): string {
  parseReleaseVersion(version);
  const sentinel = "${STREAMSKOPE_IMAGE:=streamskope:0.0.0-dev}";
  if (source.split(sentinel).length !== 2)
    throw new Error(
      "The reviewed Containerlab topology must contain one development image default.",
    );
  return source.replace(sentinel, `\${STREAMSKOPE_IMAGE:=streamskope:${version}}`);
}

export function browserRegistryTopology(source: string, registry: BrowserRegistryMetadata): string {
  const reviewed = browserReleaseTopology(source, registry.version);
  if (reviewed.split("image-pull-policy: Never").length !== 2)
    throw new Error("The reviewed Containerlab topology must declare one local image policy.");
  return reviewed
    .replace(
      `\${STREAMSKOPE_IMAGE:=streamskope:${registry.version}}`,
      `\${STREAMSKOPE_IMAGE:=${registry.reference}}`,
    )
    .replace("image-pull-policy: Never", "image-pull-policy: IfNotPresent");
}

async function directoryNames(directory: string, expected: readonly string[]): Promise<void> {
  const metadata = await lstat(directory);
  if (!metadata.isDirectory() || metadata.isSymbolicLink())
    throw new Error("Browser release assets must use regular directories.");
  const names = (await readdir(directory)).sort();
  if (
    names.length !== expected.length ||
    names.some((name, index) => name !== [...expected].sort()[index])
  )
    throw new Error("Browser release assets are missing, duplicated, or unexpected.");
}

/** Combine qualified native builds with matching online and offline delivery metadata. */
export async function prepareBrowserReleaseAssets(
  staging: string,
  output: string,
  version: string,
  commit: string,
  topologySource = "streamskope.clab.yml",
  registryPath?: string,
): Promise<void> {
  identity(version, commit);
  await directoryNames(
    staging,
    ARCHITECTURES.map((arch) => `browser-linux-${arch}`),
  );
  const source = await readFile(topologySource, "utf8");
  const offline = browserReleaseTopology(source, version);
  const registry =
    registryPath === undefined
      ? undefined
      : parseBrowserRegistryMetadata(
          JSON.parse(
            (
              await readBoundedFile(registryPath, MAX_METADATA_BYTES, { rejectSymlinks: true })
            ).toString("utf8"),
          ),
          version,
          commit,
        );
  const topology = registry === undefined ? offline : browserRegistryTopology(source, registry);
  const topologyFile = `streamskope-${version}.clab.yml`;
  const archives: ArchiveRecord[] = [];
  for (const arch of ARCHITECTURES) {
    const directory = join(staging, `browser-linux-${arch}`);
    const archive = `StreamSkope-${version}-container-linux-${arch}.tar.gz`;
    const metadataFile = `container-linux-${arch}.json`;
    await directoryNames(directory, [archive, metadataFile, topologyFile]);
    const record = imageRecord(
      JSON.parse(
        (
          await readBoundedFile(join(directory, metadataFile), MAX_METADATA_BYTES, {
            rejectSymlinks: true,
          })
        ).toString("utf8"),
      ),
      version,
      commit,
      arch,
    );
    const suppliedTopology = await readBoundedFile(
      join(directory, topologyFile),
      MAX_METADATA_BYTES,
      { rejectSymlinks: true },
    );
    if (!suppliedTopology.equals(Buffer.from(offline)))
      throw new Error(
        "Browser builds must include the identical version-matched reviewed topology.",
      );
    archives.push(await inspectArchive(join(directory, archive), record));
  }
  if (
    registry !== undefined &&
    registry.platforms.some((platform, index) => platform.imageId !== archives[index]?.imageId)
  )
    throw new Error("Registry native images differ from the qualified offline archives.");
  const offlineTopology =
    registry === undefined
      ? undefined
      : {
          file: `streamskope-${version}-offline.clab.yml`,
          sha256: createHash("sha256").update(offline).digest("hex"),
        };
  const manifest: BrowserReleaseManifest = {
    schemaVersion: registry === undefined ? 1 : 3,
    version,
    sourceRevision: commit,
    image: `streamskope:${version}`,
    format: "docker-save-gzip",
    topology: { file: topologyFile, sha256: createHash("sha256").update(topology).digest("hex") },
    archives,
    ...(registry === undefined || offlineTopology === undefined
      ? {}
      : { registry, offlineTopology, installer: { file: BROWSER_INSTALLER_NAME } }),
  };
  const manifestContent = `${JSON.stringify(manifest, null, 2)}\n`;
  try {
    await lstat(output);
    throw new Error("Browser release output already exists.");
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
  }
  await mkdir(dirname(output), { recursive: true });
  const temporary = await mkdtemp(join(dirname(output), ".browser-release-"));
  try {
    for (const record of archives)
      await copyFile(
        join(staging, `browser-${record.platform.replace("/", "-")}`, record.archive),
        join(temporary, record.archive),
        constants.COPYFILE_EXCL,
      );
    await writeFile(join(temporary, topologyFile), topology, { flag: "wx" });
    if (offlineTopology !== undefined)
      await writeFile(join(temporary, offlineTopology.file), offline, { flag: "wx" });
    await writeFile(join(temporary, `streamskope-${version}-container.json`), manifestContent, {
      flag: "wx",
    });
    if (registry !== undefined)
      await writeFile(
        join(temporary, BROWSER_INSTALLER_NAME),
        renderBrowserWorkbenchInstaller(
          {
            version,
            sourceRevision: commit,
            topologySha256: manifest.topology.sha256,
            manifestSha256: createHash("sha256").update(manifestContent).digest("hex"),
          },
          await readFile("tools/package/install-browser-workbench.sh", "utf8"),
        ),
        { flag: "wx", mode: 0o755 },
      );
    await rename(temporary, output);
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

/** Recheck the final files before checksums and publication, including copied archive bytes. */
export async function validateBrowserReleaseAssets(
  directory: string,
  version: string,
  commit: string,
  topologySource = "streamskope.clab.yml",
): Promise<readonly string[]> {
  identity(version, commit);
  const manifestBytes = await readBoundedFile(
    join(directory, `streamskope-${version}-container.json`),
    MAX_METADATA_BYTES,
    { rejectSymlinks: true },
  );
  const manifest: unknown = JSON.parse(manifestBytes.toString("utf8"));
  const input = object(manifest);
  const registry =
    input.schemaVersion === 2 || input.schemaVersion === 3
      ? parseBrowserRegistryMetadata(input.registry, version, commit)
      : undefined;
  const hasInstaller = input.schemaVersion === 3;
  const names = browserReleaseNames(version, registry !== undefined, hasInstaller);
  await directoryNames(directory, names);
  const source = await readFile(topologySource, "utf8");
  const offline = browserReleaseTopology(source, version);
  const topology = registry === undefined ? offline : browserRegistryTopology(source, registry);
  const topologyBytes = await readBoundedFile(
    join(directory, `streamskope-${version}.clab.yml`),
    MAX_METADATA_BYTES,
    { rejectSymlinks: true },
  );
  if (!topologyBytes.equals(Buffer.from(topology)))
    throw new Error(
      "Browser release topology does not match the reviewed source and registry digest.",
    );
  const offlineTopology =
    registry === undefined
      ? undefined
      : {
          file: `streamskope-${version}-offline.clab.yml`,
          sha256: createHash("sha256").update(offline).digest("hex"),
        };
  if (
    offlineTopology !== undefined &&
    !(
      await readBoundedFile(join(directory, offlineTopology.file), MAX_METADATA_BYTES, {
        rejectSymlinks: true,
      })
    ).equals(Buffer.from(offline))
  )
    throw new Error("Offline browser topology differs from the reviewed local-image topology.");
  if (!Array.isArray(input.archives) || input.archives.length !== ARCHITECTURES.length)
    throw new Error("Browser release must include both native Linux architectures.");
  const archives: ArchiveRecord[] = [];
  for (const [index, arch] of ARCHITECTURES.entries()) {
    const archived = object(input.archives[index]);
    const fields = Object.fromEntries(
      Object.entries(archived).filter(([key]) => !["bytes", "sha256"].includes(key)),
    );
    const record = imageRecord(fields, version, commit, arch);
    archives.push(await inspectArchive(join(directory, record.archive), record));
  }
  if (
    registry !== undefined &&
    registry.platforms.some((platform, index) => platform.imageId !== archives[index]?.imageId)
  )
    throw new Error("Registry native images differ from the qualified offline archives.");
  const expected: BrowserReleaseManifest = {
    schemaVersion: hasInstaller ? 3 : registry === undefined ? 1 : 2,
    version,
    sourceRevision: commit,
    image: `streamskope:${version}`,
    format: "docker-save-gzip",
    topology: {
      file: `streamskope-${version}.clab.yml`,
      sha256: createHash("sha256").update(topologyBytes).digest("hex"),
    },
    archives,
    ...(registry === undefined || offlineTopology === undefined
      ? {}
      : { registry, offlineTopology }),
    ...(hasInstaller ? { installer: { file: BROWSER_INSTALLER_NAME } } : {}),
  };
  if (!isDeepStrictEqual(input, expected))
    throw new Error("Browser release manifest does not match the actual archives and topology.");
  if (hasInstaller) {
    const installer = await readBoundedFile(
      join(directory, BROWSER_INSTALLER_NAME),
      MAX_METADATA_BYTES,
      {
        rejectSymlinks: true,
      },
    );
    const reviewed = renderBrowserWorkbenchInstaller(
      {
        version,
        sourceRevision: commit,
        topologySha256: expected.topology.sha256,
        manifestSha256: createHash("sha256").update(manifestBytes).digest("hex"),
      },
      await readFile("tools/package/install-browser-workbench.sh", "utf8"),
    );
    if (!installer.equals(Buffer.from(reviewed)))
      throw new Error("Browser installer does not match the reviewed template and release assets.");
  }
  return names;
}
