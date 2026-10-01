import { createHash } from "node:crypto";

import type { PluginManifest } from "../../../plugins/contracts";
import { parsePluginManifest } from "../../../plugins/validation";

export const MAX_PLUGIN_PACKAGE_BYTES = 32 * 1024 * 1024;
const MAX_PLUGIN_CONTENT_BYTES = 24 * 1024 * 1024;
const FILE_NAMES = new Set(["backend.cjs", "renderer.js", "renderer.css"]);
export const MAX_PLUGIN_RESOURCE_BYTES = 1024 * 1024;

export interface VerifiedPluginPackage {
  readonly manifest: PluginManifest;
  readonly sha256: string;
  readonly files: ReadonlyMap<string, Uint8Array>;
}

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Invalid plugin package object.");
  }
  return value as Record<string, unknown>;
}

function exactKeys(value: Record<string, unknown>, keys: readonly string[]): void {
  if (Object.keys(value).sort().join("\n") !== [...keys].sort().join("\n")) {
    throw new Error("Unexpected plugin package fields.");
  }
}

export function pluginPackageSha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** Fixed entrypoints plus declared, hashed flat resources; never arbitrary archive paths. */
export function parsePluginPackage(
  bytes: Uint8Array,
  expectedSha256?: string,
): VerifiedPluginPackage {
  if (bytes.byteLength === 0 || bytes.byteLength > MAX_PLUGIN_PACKAGE_BYTES) {
    throw new Error("Plugin package exceeds its size limit or is empty.");
  }
  const sha256 = pluginPackageSha256(bytes);
  if (
    expectedSha256 !== undefined &&
    (!/^[a-f0-9]{64}$/u.test(expectedSha256) || sha256 !== expectedSha256)
  ) {
    throw new Error("Plugin package SHA256 does not match the official release.");
  }
  const envelope = record(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)));
  exactKeys(envelope, ["formatVersion", "manifest", "files"]);
  if (envelope.formatVersion !== 1 || !Array.isArray(envelope.files)) {
    throw new Error("Unsupported plugin package format.");
  }
  const manifest = parsePluginManifest(envelope.manifest);
  const resources = new Map(
    (manifest.resources ?? []).map((resource) => [resource.path, resource]),
  );
  const expectedFiles = [
    manifest.backend,
    manifest.renderer,
    ...(manifest.styles ? [manifest.styles] : []),
    ...resources.keys(),
  ];
  if (envelope.files.length !== expectedFiles.length) {
    throw new Error("Plugin package files do not match its manifest.");
  }
  const files = new Map<string, Uint8Array>();
  let totalBytes = 0;
  for (const item of envelope.files as unknown[]) {
    const file = record(item);
    exactKeys(file, ["path", "content"]);
    if (
      typeof file.path !== "string" ||
      (!FILE_NAMES.has(file.path) && !resources.has(file.path)) ||
      !expectedFiles.includes(file.path) ||
      files.has(file.path) ||
      typeof file.content !== "string"
    ) {
      throw new Error("Invalid, duplicated or unexpected plugin file.");
    }
    const content = Buffer.from(file.content, "base64");
    if (content.length === 0 || content.toString("base64") !== file.content) {
      throw new Error("Plugin files must contain canonical, nonempty base64 content.");
    }
    const resource = resources.get(file.path);
    if (resource !== undefined) {
      if (content.byteLength > MAX_PLUGIN_RESOURCE_BYTES) {
        throw new Error("Plugin resource exceeds its size limit.");
      }
      if (pluginPackageSha256(content) !== resource.sha256) {
        throw new Error("Plugin resource SHA256 does not match its manifest.");
      }
    }
    totalBytes += content.byteLength;
    if (totalBytes > MAX_PLUGIN_CONTENT_BYTES) {
      throw new Error("Plugin content exceeds its size limit.");
    }
    files.set(file.path, content);
  }
  return { manifest, sha256, files };
}

export function encodePluginPackage(
  manifest: PluginManifest,
  files: ReadonlyMap<string, Uint8Array>,
): Uint8Array {
  const bytes = Buffer.from(
    JSON.stringify({
      formatVersion: 1,
      manifest,
      files: [...files]
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([path, content]) => ({
          path,
          content: Buffer.from(content).toString("base64"),
        })),
    }),
    "utf8",
  );
  parsePluginPackage(bytes);
  return bytes;
}
