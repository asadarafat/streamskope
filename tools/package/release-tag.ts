import { appendFile, readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { parsePluginManifest } from "../../src/plugins/validation";
import { parseReleaseVersion } from "../../src/plugins/compatibility";
import { PLUGIN_API_VERSION } from "../../src/plugins/contracts";
import { OFFICIAL_PLUGINS } from "../../src/platform/node/plugins/official";

import { isDesktopReleaseTag } from "./release-policy";

interface ProjectManifest {
  readonly version?: unknown;
}

async function main(): Promise<void> {
  const tag = process.env.GITHUB_REF_NAME;
  const commit = process.env.GITHUB_SHA;
  const output = process.env.GITHUB_OUTPUT;
  const plugin = OFFICIAL_PLUGINS.find((entry) => tag?.startsWith(`plugins/${entry.directory}/v`));
  const manifest = plugin
    ? parsePluginManifest(
        JSON.parse(await readFile(resolve("plugins", plugin.directory, "manifest.json"), "utf8")),
      )
    : (JSON.parse(await readFile(resolve("package.json"), "utf8")) as ProjectManifest);
  if (
    typeof manifest.version !== "string" ||
    (plugin
      ? !("apiVersion" in manifest) ||
        !("id" in manifest) ||
        manifest.id !== plugin.id ||
        manifest.apiVersion !== PLUGIN_API_VERSION ||
        tag !== `plugins/${plugin.directory}/v${parseReleaseVersion(manifest.version)}`
      : !isDesktopReleaseTag(tag, manifest.version)) ||
    typeof commit !== "string" ||
    !/^[a-f0-9]{40}$/u.test(commit) ||
    output === undefined
  ) {
    throw new Error(
      "The protected tag, package version, plugin API, commit, or workflow output is invalid.",
    );
  }
  await appendFile(
    output,
    `version=${manifest.version}\nprerelease=${manifest.version.includes("-")}\n`,
    "utf8",
  );
  process.stdout.write(`Validated release tag ${tag} at ${commit}.\n`);
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? error.message : String(error);
  process.stderr.write(`Release tag validation failed: ${detail}\n`);
  process.exitCode = 1;
});
