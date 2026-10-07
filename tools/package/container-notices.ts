import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, mkdir, open, readdir, realpath, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const MAX_PACKAGES = 2_000;
const MAX_DOCUMENT_BYTES = 512 * 1_024;
const MAX_ARTIFACT_BYTES = 16 * 1_024 * 1_024;
const DOCUMENT_NAME = /^(?:licen[cs]e|notice|copying|copyright)(?:[._-].*)?$/iu;
const README_NAME = /^readme(?:[._-].*)?$/iu;

interface NoticeDocument {
  readonly sha256: string;
  readonly bytes: number;
  readonly encoding: "utf8" | "base64";
  readonly content: string;
}

interface NoticeReference {
  readonly file: string;
  readonly selection?: "license-heading-through-end";
  readonly sha256: string;
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Public dependency metadata must contain a JSON object.");
  return value as Record<string, unknown>;
}

async function bytes(path: string, limit: number): Promise<Buffer> {
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const metadata = await file.stat();
    if (!metadata.isFile() || metadata.size > limit)
      throw new Error("A public dependency document is not a bounded regular file.");
    const result = await file.readFile();
    if (result.length > limit) throw new Error("A dependency document exceeds its byte limit.");
    return result;
  } finally {
    await file.close();
  }
}

async function packageDirectory(root: string, location: string): Promise<string | undefined> {
  if (
    !location.startsWith("node_modules/") ||
    location.includes("\\") ||
    location.split("/").some((part) => part === "" || part === "." || part === "..")
  )
    throw new Error("The public dependency lock contains an invalid package location.");
  let current = root;
  for (const part of location.split("/")) {
    current = join(current, part);
    let metadata;
    try {
      metadata = await lstat(current);
    } catch (error) {
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT")
        return undefined;
      throw error;
    }
    if (!metadata.isDirectory() || metadata.isSymbolicLink())
      throw new Error("Installed dependency packages must use regular directories.");
  }
  return current;
}

function documentContent(content: Buffer): Pick<NoticeDocument, "encoding" | "content"> {
  try {
    return {
      encoding: "utf8",
      content: new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(content),
    };
  } catch {
    // Preserve original copyright/license bytes even when a package is not UTF-8.
    return { encoding: "base64", content: content.toString("base64") };
  }
}

/** Preserve public installed-package notices before build dependencies are pruned. */
export async function collectContainerNotices(rootDirectory: string): Promise<string> {
  const root = await realpath(resolve(rootDirectory));
  const manifest = object(
    JSON.parse((await bytes(join(root, "package.json"), 1_024 * 1_024)).toString("utf8")),
  );
  const lock = object(
    JSON.parse((await bytes(join(root, "package-lock.json"), 8 * 1_024 * 1_024)).toString("utf8")),
  );
  if (lock.lockfileVersion !== 3)
    throw new Error("Container notices require the version 3 public npm package lock.");
  const lockedPackages = object(lock.packages);
  const packages: {
    name: string;
    version: string;
    location: string;
    declaredLicense: string | null;
    productionDependency: boolean;
    coverage: "metadata-only" | "packaged-notices";
    documents: NoticeReference[];
  }[] = [];
  const documents = new Map<string, NoticeDocument>();
  let documentBytes = 0;
  const addDocument = (content: Buffer): string => {
    const sha256 = createHash("sha256").update(content).digest("hex");
    if (!documents.has(sha256)) {
      documentBytes += content.length;
      if (documentBytes > MAX_ARTIFACT_BYTES)
        throw new Error("Dependency notice contents exceed their total byte limit.");
      documents.set(sha256, { sha256, bytes: content.length, ...documentContent(content) });
    }
    return sha256;
  };
  for (const location of Object.keys(lockedPackages).sort()) {
    if (location === "") continue;
    const locked = object(lockedPackages[location]);
    if (locked.link === true)
      throw new Error("Local linked packages cannot be included in container notices.");
    const directory = await packageDirectory(root, location);
    if (directory === undefined) continue; // Optional dependencies for other operating systems.
    if (packages.length >= MAX_PACKAGES)
      throw new Error("Installed dependency count exceeds its bound.");
    const pkg = object(
      JSON.parse((await bytes(join(directory, "package.json"), 1_024 * 1_024)).toString("utf8")),
    );
    if (
      typeof pkg.name !== "string" ||
      typeof pkg.version !== "string" ||
      pkg.name.length > 256 ||
      pkg.version.length > 256 ||
      (typeof pkg.license === "string" && pkg.license.length > 10_240) ||
      (typeof locked.license === "string" && locked.license.length > 10_240) ||
      pkg.version !== locked.version
    )
      throw new Error("Installed public package metadata does not match the lock.");
    const names = (await readdir(directory)).sort();
    const references: NoticeReference[] = [];
    for (const name of names.filter((value) => DOCUMENT_NAME.test(value))) {
      const path = join(directory, name);
      const metadata = await lstat(path);
      if (metadata.isSymbolicLink())
        throw new Error("Dependency notice files must not be symbolic links.");
      if (metadata.isDirectory()) {
        // Packages sometimes distribute additional notices in LICENSES/.
        for (const child of (await readdir(path)).sort()) {
          const childPath = join(path, child);
          const childMetadata = await lstat(childPath);
          if (!childMetadata.isFile() || childMetadata.isSymbolicLink())
            throw new Error("Dependency notice directories must contain regular files.");
          references.push({
            file: `${name}/${child}`,
            sha256: addDocument(await bytes(childPath, MAX_DOCUMENT_BYTES)),
          });
        }
      } else {
        references.push({ file: name, sha256: addDocument(await bytes(path, MAX_DOCUMENT_BYTES)) });
      }
    }
    if (references.length === 0) {
      // Include the exact public README license tail where that is the package's only notice.
      for (const name of names.filter((value) => README_NAME.test(value))) {
        const content = await bytes(join(directory, name), MAX_DOCUMENT_BYTES);
        const text = new TextDecoder("utf-8", { fatal: true }).decode(content);
        const start = /^#{1,6}\s+(?:licen[cs]e|copyright)\b[^\r\n]*$/imu.exec(text);
        if (start !== null) {
          references.push({
            file: name,
            selection: "license-heading-through-end",
            sha256: addDocument(Buffer.from(text.slice(start.index), "utf8")),
          });
        }
      }
    }
    packages.push({
      name: pkg.name,
      version: pkg.version,
      location,
      declaredLicense:
        typeof pkg.license === "string"
          ? pkg.license
          : typeof locked.license === "string"
            ? locked.license
            : null,
      productionDependency: locked.dev !== true,
      coverage: references.length === 0 ? "metadata-only" : "packaged-notices",
      documents: references,
    });
  }
  const artifact = {
    formatVersion: 1,
    product: { name: manifest.name, version: manifest.version },
    scope:
      "All public npm packages installed during the build, before development dependencies are pruned. This is a conservative superset, not a claim that every package ships in the image.",
    limitation:
      "Metadata-only entries contain no packaged license text. Preserve relevant parent-package notices and review these entries for any shipped dependencies. Base-image operating-system notices remain with the base image.",
    packages,
    documents: [...documents.values()].sort((a, b) => (a.sha256 < b.sha256 ? -1 : 1)),
  };
  const output = `${JSON.stringify(artifact, null, 2)}\n`;
  if (Buffer.byteLength(output) > MAX_ARTIFACT_BYTES)
    throw new Error("Container notices exceed their artifact limit.");
  return output;
}

async function main(): Promise<void> {
  const outputPath = resolve(process.argv[2] ?? "dist/web/THIRD_PARTY_LICENSES.json");
  const root = await realpath(process.cwd());
  if (!outputPath.startsWith(`${root}${sep}`))
    throw new Error("Write container notices inside the build checkout.");
  const output = await collectContainerNotices(root);
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, output, { flag: "wx", mode: 0o644 });
  process.stdout.write(
    `Container dependency notices written (${Buffer.byteLength(output)} bytes).\n`,
  );
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void main().catch((error: unknown) => {
    process.stderr.write(
      `Container notices failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
