/* global process, fetch, AbortSignal, Buffer */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import { EDA_TARGET_VERSION } from "../../plugins/eda/contracts/eda-capture-types.ts";

const [registry, version, output, ...extra] = process.argv.slice(2);
assert(
  registry && /^http:\/\/127\.0\.0\.1:\d+$/u.test(registry),
  "Use the disposable loopback registry.",
);
assert.equal(version, EDA_TARGET_VERSION, "The EDA app must use the exact target EDA version.");
assert(output && extra.length === 0, "Expected registry, version and OCI output directory.");
const repository = "streamskope-eda-app";
const manifestTypes = new Set([
  "application/vnd.oci.image.index.v1+json",
  "application/vnd.oci.image.manifest.v1+json",
  "application/vnd.docker.distribution.manifest.list.v2+json",
  "application/vnd.docker.distribution.manifest.v2+json",
]);
const headers = { Accept: [...manifestTypes].join(", ") };
const directory = resolve(output);
await mkdir(resolve(directory, "blobs/sha256"), { recursive: true });
const visited = new Set();

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

async function get(path) {
  const response = await fetch(`${registry}/v2/${repository}/${path}`, {
    headers,
    signal: AbortSignal.timeout(30_000),
  });
  assert(response.ok, `Missing EDA OCI object (${response.status}).`);
  return { bytes: Buffer.from(await response.arrayBuffer()), response };
}

async function retain(descriptor, bytes, depth = 0) {
  assert(/^sha256:[a-f0-9]{64}$/u.test(descriptor?.digest), "Invalid EDA blob digest.");
  assert(
    Number.isSafeInteger(descriptor.size) &&
      descriptor.size >= 0 &&
      descriptor.size <= 128 * 1024 * 1024,
    "Unbounded EDA blob size.",
  );
  assert(depth <= 16 && visited.size <= 256, "Unbounded EDA OCI graph.");
  assert.equal(bytes.length, descriptor.size, "The EDA blob size changed.");
  assert.equal(digest(bytes), descriptor.digest, "The EDA blob digest changed.");
  if (visited.has(descriptor.digest)) return;
  visited.add(descriptor.digest);
  await writeFile(resolve(directory, "blobs/sha256", descriptor.digest.slice(7)), bytes);
  if (manifestTypes.has(descriptor.mediaType)) {
    const manifest = JSON.parse(bytes.toString("utf8"));
    assert.equal(manifest.schemaVersion, 2);
    const children = manifest.manifests ?? [manifest.config, ...(manifest.layers ?? [])];
    assert(
      Array.isArray(children) && children.length > 0 && children.every(Boolean),
      "The EDA app has no OCI components.",
    );
    for (const child of children) {
      const path = manifestTypes.has(child.mediaType)
        ? `manifests/${child.digest}`
        : `blobs/${child.digest}`;
      const object = await get(path);
      await retain(child, object.bytes, depth + 1);
    }
  }
}

const root = await get(`manifests/${version}`);
const rootDigest = digest(root.bytes);
assert.equal(
  root.response.headers.get("Docker-Content-Digest"),
  rootDigest,
  "The registry manifest digest changed.",
);
const mediaType = root.response.headers.get("content-type")?.split(";")[0];
assert(manifestTypes.has(mediaType), "The registry returned an unsupported EDA manifest type.");
const descriptor = {
  mediaType,
  digest: rootDigest,
  size: root.bytes.length,
  annotations: { "org.opencontainers.image.ref.name": version },
};
await retain(descriptor, root.bytes);
await writeFile(resolve(directory, "oci-layout"), '{"imageLayoutVersion":"1.0.0"}\n');
await writeFile(
  resolve(directory, "index.json"),
  JSON.stringify(
    {
      schemaVersion: 2,
      mediaType: "application/vnd.oci.image.index.v1+json",
      manifests: [descriptor],
    },
    null,
    2,
  ) + "\n",
);
process.stdout.write(
  `EDA application OCI layout verified: ${version} ${rootDigest} (${visited.size} objects)\n`,
);
