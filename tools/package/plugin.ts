import { createPrivateKey, createPublicKey, type KeyObject } from "node:crypto";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { EDA_TARGET_VERSION } from "../../plugins/eda/contracts/eda-capture-types";
import { PLUGIN_API_VERSION } from "../../src/plugins/contracts";
import { parsePluginManifest } from "../../src/plugins/validation";
import { DEVELOPMENT_VERSION, isDevelopmentPluginVersion } from "../../src/plugins/compatibility";
import {
  OFFICIAL_PLUGINS,
  officialPluginAssets,
  type OfficialPlugin,
} from "../../src/platform/node/plugins/official";
import {
  encodePluginPackage,
  parsePluginPackage,
  parsePortablePluginPackage,
  pluginPackagePayloadBytes,
  pluginPackageSha256,
  signPortablePluginPackage,
} from "../../src/platform/node/plugins/package";
import {
  TRUSTED_PLUGIN_PUBLISHERS,
  type TrustedPluginPublisher,
} from "../../src/platform/node/plugins/publishers";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { buildPlugin } from "../build/plugin.js";

let lastDevelopmentBuild = 0;

function signingKey(
  encodedKey: string | undefined,
  pluginId: string,
  publishers: readonly TrustedPluginPublisher[],
): { key: KeyObject; publisher: TrustedPluginPublisher } {
  if (encodedKey === undefined || encodedKey.length === 0)
    throw new Error("Production plugin packaging requires STREAMSKOPE_PLUGIN_SIGNING_KEY_B64.");
  if (encodedKey.length > 16_384)
    throw new Error("The plugin signing key must be a bounded canonical base64 PKCS8 PEM.");
  const bytes = Buffer.from(encodedKey, "base64");
  if (bytes.toString("base64") !== encodedKey)
    throw new Error("The plugin signing key must be a bounded canonical base64 PKCS8 PEM.");
  let key: KeyObject;
  try {
    key = createPrivateKey({ key: bytes, format: "pem", type: "pkcs8" });
  } catch {
    throw new Error("The plugin signing key must be a valid unencrypted Ed25519 PKCS8 PEM.");
  }
  if (key.asymmetricKeyType !== "ed25519")
    throw new Error("The plugin signing key must be a valid unencrypted Ed25519 PKCS8 PEM.");
  const publicKey = createPublicKey(key).export({ format: "der", type: "spki" });
  const publisher = publishers.find(
    (candidate) =>
      candidate.pluginIds.includes(pluginId) &&
      createPublicKey(candidate.publicKey)
        .export({ format: "der", type: "spki" })
        .equals(publicKey),
  );
  if (publisher === undefined)
    throw new Error("The plugin signing key does not match a trusted publisher for this plugin.");
  return { key, publisher };
}

/** Test trust is explicit caller injection; production callers use only the shipped registry. */
export function createPortablePluginRelease(
  primary: Uint8Array,
  encodedKey: string | undefined,
  publishers: readonly TrustedPluginPublisher[] = TRUSTED_PLUGIN_PUBLISHERS,
): Uint8Array {
  const manifest = parsePluginPackage(primary).manifest;
  const { key, publisher } = signingKey(encodedKey, manifest.id, publishers);
  const portable = signPortablePluginPackage(primary, publisher.keyId, key);
  const verified = parsePortablePluginPackage(portable, undefined, publishers);
  if (
    verified.manifest.id !== manifest.id ||
    !Buffer.from(pluginPackagePayloadBytes(portable, publishers)).equals(Buffer.from(primary))
  )
    throw new Error("The signed portable plugin does not match its primary release package.");
  return portable;
}

async function packagePlugin(plugin: OfficialPlugin, output: string): Promise<void> {
  const manifest = parsePluginManifest(
    JSON.parse(await readFile(join("plugins", plugin.directory, "manifest.json"), "utf8")),
  );
  if (manifest.id !== plugin.id)
    throw new Error("The desktop plugin manifest does not match its official identity.");
  if (
    manifest.apiVersion !== PLUGIN_API_VERSION ||
    manifest.compatibility?.target.system !== plugin.directory
  ) {
    throw new Error("Official plugins must declare current API host and target compatibility.");
  }
  if (
    plugin.directory === "eda" &&
    (manifest.compatibility?.target.minimum !== EDA_TARGET_VERSION.replace(/^v/u, "") ||
      manifest.compatibility.target.maximum !== EDA_TARGET_VERSION.replace(/^v/u, ""))
  ) {
    throw new Error("The desktop plugin manifest must target the exact supported EDA version.");
  }
  if (!isDevelopmentPluginVersion(manifest.version))
    signingKey(
      process.env.STREAMSKOPE_PLUGIN_SIGNING_KEY_B64,
      manifest.id,
      TRUSTED_PLUGIN_PUBLISHERS,
    );
  await buildPlugin(plugin);
  const directory = resolve("dist/plugins", plugin.directory);
  const files = new Map<string, Uint8Array>();
  for (const name of (await readdir(directory)).sort())
    files.set(name, await readFile(join(directory, name)));
  let version = manifest.version;
  if (version === DEVELOPMENT_VERSION) {
    // Keep local rebuilds ordered without weakening immutable package checks.
    lastDevelopmentBuild = Math.max(Date.now(), lastDevelopmentBuild + 1);
    version = `${DEVELOPMENT_VERSION}.${lastDevelopmentBuild}`;
  }
  const publishedManifest = parsePluginManifest({
    ...manifest,
    version,
    ...(files.has("renderer.css") ? { styles: "renderer.css" } : {}),
  });
  const bytes = encodePluginPackage(publishedManifest, files);
  const assets = officialPluginAssets(plugin, publishedManifest.version);
  const portable = isDevelopmentPluginVersion(publishedManifest.version)
    ? undefined
    : createPortablePluginRelease(bytes, process.env.STREAMSKOPE_PLUGIN_SIGNING_KEY_B64);
  const temporary = await mkdtemp(join(tmpdir(), "streamskope-plugin-package-"));
  try {
    const store = new PluginStore(temporary);
    await store.install(bytes, pluginPackageSha256(bytes));
    const [active] = await store.activatePending();
    if (!active) throw new Error("The generated plugin package could not be activated.");
    const backend: unknown = await import(pathToFileURL(active.backendPath).href);
    if (
      backend === null ||
      typeof backend !== "object" ||
      !("activate" in backend) ||
      typeof backend.activate !== "function"
    ) {
      throw new Error("The standalone plugin backend does not export activate().");
    }
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
  await writeFile(join(output, assets.packageAsset), bytes);
  if (portable !== undefined) await writeFile(join(output, assets.portablePackageAsset), portable);
  await writeFile(
    join(output, assets.manifestAsset),
    `${JSON.stringify(publishedManifest, null, 2)}\n`,
  );
  for (const resource of publishedManifest.resources ?? []) {
    await writeFile(join(output, `${assets.prefix}-${resource.path}`), files.get(resource.path)!);
  }
  process.stdout.write(
    `Built ${manifest.name} desktop plugin ${publishedManifest.version}: ${join(output, assets.packageAsset)}\n`,
  );
}

export async function packagePlugins(directory?: string): Promise<void> {
  const plugins = OFFICIAL_PLUGINS.filter(
    (plugin) => directory === undefined || plugin.directory === directory,
  );
  if (plugins.length === 0) throw new Error("Unknown official plugin. Choose eda or nsp.");
  const output = resolve("dist/plugin-package");
  await rm(output, { recursive: true, force: true });
  await mkdir(output, { recursive: true });
  for (const plugin of plugins) await packagePlugin(plugin, output);
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  void (async (): Promise<void> => {
    if (process.argv.length > 3) throw new Error("Usage: plugin.ts [eda|nsp]");
    await packagePlugins(process.argv[2]);
  })().catch((error: unknown) => {
    process.stderr.write(
      `Plugin packaging failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
}
