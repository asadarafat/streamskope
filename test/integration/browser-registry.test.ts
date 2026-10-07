import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

import { Header } from "tar";
import { afterEach, expect, it } from "vitest";

import {
  BROWSER_IMAGE_REPOSITORY,
  parseBrowserRegistryMetadata,
} from "../../tools/package/browser-registry-metadata";
import {
  publishBrowserRegistryCandidate,
  promoteBrowserRegistryCandidate,
  type BrowserDockerResult,
  type BrowserDockerRunner,
  type BrowserRegistryOptions,
} from "../../tools/package/browser-registry";

const version = "0.10.0-rc.1";
const commit = "a".repeat(40);
const roots: string[] = [];
const repository = BROWSER_IMAGE_REPOSITORY;
const nativeType = "application/vnd.oci.image.manifest.v1+json";
const indexType = "application/vnd.oci.image.index.v1+json";
type Architecture = "amd64" | "arm64";

afterEach(async () => {
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

function digest(value: string | Buffer): string {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function entry(path: string, data: Buffer): Buffer {
  const header = new Header({ path, size: data.length, mode: 0o644, type: "File" });
  header.encode();
  return Buffer.concat([header.block!, data, Buffer.alloc((512 - (data.length % 512)) % 512)]);
}

async function fixture(): Promise<{ options: BrowserRegistryOptions; docker: DockerFixture }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-browser-registry-"));
  roots.push(root);
  const options = {
    stagingRoot: join(root, "staging"),
    version,
    sourceCommit: commit,
    runId: "12345",
    outputFile: join(root, "registry.json"),
  };
  const images = new Map<Architecture, string>();
  for (const architecture of ["amd64", "arm64"] as const) {
    const directory = join(options.stagingRoot, `browser-linux-${architecture}`);
    await mkdir(directory, { recursive: true });
    const config = Buffer.from(
      JSON.stringify({ os: "linux", architecture, config: { Labels: labels() } }),
    );
    const imageId = digest(config);
    images.set(architecture, imageId);
    const configName = `${imageId.slice(7)}.json`;
    const archive = `StreamSkope-${version}-container-linux-${architecture}.tar.gz`;
    await writeFile(
      join(directory, archive),
      gzipSync(
        Buffer.concat([
          entry(configName, config),
          entry("layer.tar", Buffer.from("qualified test layer")),
          entry(
            "manifest.json",
            Buffer.from(
              JSON.stringify([
                { Config: configName, RepoTags: [`streamskope:${version}`], Layers: ["layer.tar"] },
              ]),
            ),
          ),
          Buffer.alloc(1024),
        ]),
      ),
    );
    await writeFile(
      join(directory, `container-linux-${architecture}.json`),
      JSON.stringify({
        version,
        sourceRevision: commit,
        image: `streamskope:${version}`,
        imageId,
        platform: `linux/${architecture}`,
        archive,
      }),
    );
  }
  return { options, docker: new DockerFixture(images) };
}

function labels(): Record<string, string> {
  return {
    "org.opencontainers.image.version": version,
    "org.opencontainers.image.revision": commit,
  };
}

interface RegistryEntry {
  readonly digest: string;
  readonly architecture?: Architecture;
  readonly imageId?: string;
  readonly manifests?: readonly {
    mediaType: string;
    digest: string;
    platform: { os: string; architecture: string };
  }[];
}

/** Docker's remote and local state are separate, including real on-disk anonymous config. */
class DockerFixture {
  readonly calls: string[][] = [];
  readonly remote = new Map<string, RegistryEntry>();
  readonly localTags = new Map<string, Architecture>();
  readonly anonymousConfigs: string[] = [];
  readonly images: Map<Architecture, string>;
  publicPull = true;
  probeAuthorizationFailure = false;
  configMutation: "id" | "source" | "platform" | undefined;
  localMutation = false;
  private lastLoaded: Architecture | undefined;

  constructor(images: Map<Architecture, string>) {
    this.images = images;
  }

  private response(value: unknown): BrowserDockerResult {
    return {
      exitCode: 0,
      stdout: typeof value === "string" ? value : JSON.stringify(value),
      stderr: "",
    };
  }

  private lookup(reference: string): RegistryEntry | undefined {
    return (
      this.remote.get(reference) ??
      [...this.remote.values()].find((value) => `${repository}@${value.digest}` === reference)
    );
  }

  readonly runner: BrowserDockerRunner = async (input) => {
    this.calls.push([...input]);
    let args = [...input];
    const anonymous = args[0] === "--config";
    if (anonymous) {
      const directory = args[1]!;
      expect((await stat(directory)).mode & 0o777).toBe(0o700);
      expect((await stat(join(directory, "config.json"))).mode & 0o777).toBe(0o600);
      expect(await readFile(join(directory, "config.json"), "utf8")).toBe("{}\n");
      this.anonymousConfigs.push(directory);
      args = args.slice(2);
    }
    if (args[0] === "buildx" && args[2] === "inspect") {
      if (this.probeAuthorizationFailure)
        return { exitCode: 1, stdout: "", stderr: "401 Unauthorized: manifest unknown" };
      const reference = args[3]!;
      const image = this.lookup(reference);
      if (image === undefined)
        return { exitCode: 1, stdout: "", stderr: `ERROR: ${reference}: not found` };
      if (args[4] === "--raw")
        return this.response({
          schemaVersion: 2,
          mediaType: nativeType,
          config: { digest: this.configMutation === "id" ? digest("wrong") : image.imageId },
        });
      if (args[5] === "{{json .Image}}")
        return this.response({
          os: "linux",
          architecture: this.configMutation === "platform" ? "s390x" : image.architecture,
          config: {
            Labels: {
              ...labels(),
              ...(this.configMutation === "source"
                ? { "org.opencontainers.image.revision": "b".repeat(40) }
                : {}),
            },
          },
        });
      return this.response(
        image.manifests === undefined
          ? { mediaType: nativeType, digest: image.digest, size: 100 }
          : {
              schemaVersion: 2,
              mediaType: indexType,
              digest: image.digest,
              size: 200,
              manifests: image.manifests,
            },
      );
    }
    if (args[0] === "buildx" && args[2] === "create") {
      const reference = args[4]!;
      const sources = args.slice(5).map((source) => this.lookup(source)!);
      const manifests =
        sources.length === 1 && sources[0]!.manifests !== undefined
          ? sources[0]!.manifests
          : sources.map((source) => ({
              mediaType: nativeType,
              digest: source.digest,
              platform: { os: "linux", architecture: source.architecture! },
            }));
      const image = {
        digest: sources.length === 1 ? sources[0]!.digest : digest(JSON.stringify(manifests)),
        manifests,
      };
      this.remote.set(reference, image);
      return this.response("");
    }
    if (args[0] === "image" && args[1] === "load") {
      this.lastLoaded = args[3]!.includes("amd64") ? "amd64" : "arm64";
      this.localTags.set(`streamskope:${version}`, this.lastLoaded);
      return this.response("Loaded image.");
    }
    if (args[0] === "image" && args[1] === "tag") {
      const architecture = [...this.images.entries()].find(
        ([, imageId]) => imageId === args[2],
      )![0];
      this.localTags.set(args[3]!, architecture);
      return this.response("");
    }
    if (args[0] === "image" && args[1] === "push") {
      const reference = args[2]!;
      const architecture = this.localTags.get(reference)!;
      this.remote.set(reference, {
        architecture,
        imageId: this.images.get(architecture)!,
        digest: digest(`manifest-${architecture}`),
      });
      return this.response("");
    }
    if (args[0] === "image" && args[1] === "pull") {
      expect(anonymous).toBe(true);
      if (!this.publicPull) return { exitCode: 1, stdout: "", stderr: "denied" };
      this.lastLoaded = args[3] === "linux/amd64" ? "amd64" : "arm64";
      this.localTags.set(args[4]!, this.lastLoaded);
      return this.response("");
    }
    if (args[0] === "image" && args[1] === "inspect") {
      const architecture = this.localTags.get(args[2]!)!;
      return this.response({
        Id: this.localMutation ? digest("wrong-local") : this.images.get(architecture),
        Os: "linux",
        Architecture: architecture,
        Config: { Labels: labels() },
      });
    }
    throw new Error(`Unexpected test Docker operation ${args.slice(0, 3).join(" ")}`);
  };
}

it("publishes only the two validated native images and records an anonymously verified immutable index", async () => {
  const { options, docker } = await fixture();
  const candidate = await publishBrowserRegistryCandidate(options, docker.runner);
  expect(docker.remote.has(`${repository}:${version}`)).toBe(false);
  const receipt = await promoteBrowserRegistryCandidate(options, candidate, docker.runner);
  expect(
    parseBrowserRegistryMetadata(
      JSON.parse(await readFile(options.outputFile, "utf8")),
      version,
      commit,
    ),
  ).toEqual(receipt);
  expect(receipt.platforms.map((platform) => platform.imageId)).toEqual([
    ...docker.images.values(),
  ]);
  expect(docker.calls.filter((args) => args.includes("pull"))).toHaveLength(2);
  expect(docker.calls.filter((args) => args.includes("pull")).map((args) => args.at(-1))).toEqual([
    `${repository}@${receipt.digest}`,
    `${repository}@${receipt.platforms[1]!.manifestDigest}`,
  ]);
  expect(docker.calls.filter((args) => args[1] === "tag").map((args) => args[2])).toEqual([
    ...docker.images.values(),
  ]);
  for (const path of docker.anonymousConfigs)
    await expect(stat(path)).rejects.toMatchObject({ code: "ENOENT" });
});

it("rejects real archive corruption before any Docker operation", async () => {
  const { options, docker } = await fixture();
  await writeFile(
    join(
      options.stagingRoot,
      "browser-linux-arm64",
      `StreamSkope-${version}-container-linux-arm64.tar.gz`,
    ),
    "corrupt",
  );
  await expect(publishBrowserRegistryCandidate(options, docker.runner)).rejects.toThrow();
  expect(docker.calls).toHaveLength(0);
});

it("rejects unsafe publication inputs before any Docker operation or archive replacement", async () => {
  const { options, docker } = await fixture();
  await expect(
    publishBrowserRegistryCandidate({ ...options, runId: "other/tag" }, docker.runner),
  ).rejects.toThrow(/workflow run ID/u);
  await expect(
    publishBrowserRegistryCandidate(
      { ...options, outputFile: join(options.stagingRoot, "registry.json") },
      docker.runner,
    ),
  ).rejects.toThrow(/outside/u);
  expect(docker.calls).toHaveLength(0);
});

it("fails closed on authorization failures even when the message also says manifest unknown", async () => {
  const { options, docker } = await fixture();
  docker.probeAuthorizationFailure = true;
  await expect(publishBrowserRegistryCandidate(options, docker.runner)).rejects.toThrow(
    /cannot safely be assumed absent/u,
  );
  expect(docker.calls).toHaveLength(1);
});

it("refuses a loaded image ID that differs from the actual archive before retagging or pushing", async () => {
  const { options, docker } = await fixture();
  docker.localMutation = true;
  await expect(publishBrowserRegistryCandidate(options, docker.runner)).rejects.toThrow(
    /qualified native archive/u,
  );
  expect(docker.calls.some((args) => args[1] === "tag" || args[1] === "push")).toBe(false);
});

it.each(["id", "source", "platform"] as const)(
  "refuses remote native %s mismatch without replacing an existing native tag",
  async (mutation) => {
    const { options, docker } = await fixture();
    const candidate = await publishBrowserRegistryCandidate(options, docker.runner);
    const count = docker.calls.length;
    docker.configMutation = mutation;
    await expect(publishBrowserRegistryCandidate(options, docker.runner)).rejects.toThrow();
    await expect(
      promoteBrowserRegistryCandidate(options, candidate, docker.runner),
    ).rejects.toThrow();
    expect(
      docker.calls.slice(count).some((args) => args.includes("push") || args.includes("create")),
    ).toBe(false);
  },
);

it("keeps a private-package failure retryable, with no stable tag or receipt until anonymous pulls pass", async () => {
  const { options, docker } = await fixture();
  const candidate = await publishBrowserRegistryCandidate(options, docker.runner);
  docker.publicPull = false;
  await expect(promoteBrowserRegistryCandidate(options, candidate, docker.runner)).rejects.toThrow(
    /Make the GHCR package public/u,
  );
  expect(docker.remote.has(`${repository}:${version}`)).toBe(false);
  await expect(stat(options.outputFile)).rejects.toMatchObject({ code: "ENOENT" });
  const count = docker.calls.length;
  docker.publicPull = true;
  const retry = await publishBrowserRegistryCandidate(options, docker.runner);
  expect(retry).toEqual(candidate);
  const receipt = await promoteBrowserRegistryCandidate(options, retry, docker.runner);
  expect(receipt.digest).toBe(candidate.metadata.digest);
  expect(docker.calls.slice(count).some((args) => args[1] === "load" || args[1] === "push")).toBe(
    false,
  );
});

it("rejects a duplicate-platform candidate index and never promotes it", async () => {
  const { options, docker } = await fixture();
  const candidate = await publishBrowserRegistryCandidate(options, docker.runner);
  const saved = docker.remote.get(candidate.reference)!;
  docker.remote.set(candidate.reference, {
    ...saved,
    manifests: [saved.manifests![0]!, saved.manifests![0]!],
  });
  await expect(publishBrowserRegistryCandidate(options, docker.runner)).rejects.toThrow(
    /platforms/u,
  );
  expect(docker.remote.has(`${repository}:${version}`)).toBe(false);
});

it("reuses an identical promoted version and refuses to overwrite a different immutable index", async () => {
  const { options, docker } = await fixture();
  const candidate = await publishBrowserRegistryCandidate(options, docker.runner);
  await promoteBrowserRegistryCandidate(options, candidate, docker.runner);
  const count = docker.calls.length;
  await promoteBrowserRegistryCandidate(options, candidate, docker.runner);
  expect(docker.calls.slice(count).some((args) => args.includes("create"))).toBe(false);
  const stable = `${repository}:${version}`;
  const original = docker.remote.get(stable)!;
  docker.remote.set(stable, { ...original, digest: digest("different-index") });
  const before = docker.calls.length;
  await expect(promoteBrowserRegistryCandidate(options, candidate, docker.runner)).rejects.toThrow(
    /will not be overwritten/u,
  );
  expect(docker.calls.slice(before).some((args) => args.includes("create"))).toBe(false);
  expect(docker.remote.get(stable)!.digest).toBe(digest("different-index"));
});

it("strictly binds registry receipts to version, source, repository, digest and platform order", async () => {
  const { options, docker } = await fixture();
  const { metadata } = await publishBrowserRegistryCandidate(options, docker.runner);
  for (const mutation of [
    { ...metadata, sourceRevision: "b".repeat(40) },
    { ...metadata, image: "ghcr.io/untrusted/streamskope:0.10.0" },
    { ...metadata, reference: `${repository}:${version}` },
    { ...metadata, platforms: [...metadata.platforms].reverse() },
    { ...metadata, digest: "sha256:bad" },
    { ...metadata, extra: "unsupported" },
  ])
    expect(() => parseBrowserRegistryMetadata(mutation, version, commit)).toThrow();
  expect(() => parseBrowserRegistryMetadata(metadata, "0.0.0-dev", commit)).toThrow(
    /release version/u,
  );
});
