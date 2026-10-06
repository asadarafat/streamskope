import { execFile } from "node:child_process";
import { readFile, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { expect, test, type Page } from "@playwright/test";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../../src/features/kafka/contracts";
import {
  isDevelopmentPluginVersion,
  isPluginCompatibleWithHost,
} from "../../src/plugins/compatibility";
import type { PluginManifest } from "../../src/plugins/contracts";
import { parsePluginManifest } from "../../src/plugins/validation";
import { OFFICIAL_PLUGINS, officialPluginAssets } from "../../src/platform/node/plugins/official";
import {
  parsePortablePluginPackage,
  pluginPackageSha256,
} from "../../src/platform/node/plugins/package";
import {
  electronPluginStorageAvailable,
  startElectronPluginFixture,
} from "../support/electron-plugin";

const directories = {
  eda: process.env.STREAMSKOPE_PLUGIN_RELEASE_EDA_DIRECTORY,
  nsp: process.env.STREAMSKOPE_PLUGIN_RELEASE_NSP_DIRECTORY,
};
const configured = Object.values(directories).some((directory) => directory !== undefined);
const execute = promisify(execFile);

async function validateReleaseArtifacts(
  directory: string,
  component: string,
  version: string,
): Promise<PluginManifest> {
  // The release tool is ESM; run its existing validator through tsx instead of
  // transforming its CLI entrypoint into Playwright's CommonJS test context.
  const { stdout } = await execute(
    process.execPath,
    [
      "--import",
      "tsx",
      "--input-type=module",
      "--eval",
      "import { validatePluginReleaseAssets } from './tools/package/release.ts'; process.stdout.write(JSON.stringify(await validatePluginReleaseAssets(...process.argv.slice(1))));",
      directory,
      component,
      version,
    ],
    { timeout: 15_000, maxBuffer: 64 * 1024 },
  );
  return parsePluginManifest(JSON.parse(stdout));
}

async function installation(
  page: Page,
  pluginId: string,
): Promise<
  | {
      activationId: string | undefined;
      version: string | undefined;
      rendererUrl: string | undefined;
    }
  | undefined
> {
  return page.evaluate(
    async ({ version, pluginId }) => {
      const host = (window as unknown as { streamSkopeHost: StreamSkopeHost }).streamSkopeHost;
      const response = await host.execute({
        command: "plugins.list",
        id: crypto.randomUUID(),
        payload: {},
        version,
      });
      if (!response.ok) throw new Error(response.error.summary);
      const found = response.result.pluginSnapshot.plugins.find((entry) => entry.id === pluginId);
      return found === undefined
        ? undefined
        : {
            activationId: found.activationId,
            version: found.active?.version,
            rendererUrl: found.rendererUrl,
          };
    },
    { version: HOST_PROTOCOL_VERSION, pluginId },
  );
}

// Opt-in release acceptance consumes downloaded CI artifacts unchanged. Normal PR
// qualification does not claim this lane ran without actual release artifacts.
test.describe("actual signed plugin release artifacts in the native source shell", () => {
  test.skip(
    !configured,
    "Set both plugin release artifact directories to run this release qualification.",
  );
  test.beforeAll(() => {
    if (!directories.eda || !directories.nsp)
      throw new Error(
        "Release artifact qualification requires both EDA and NSP asset directories.",
      );
    if (!electronPluginStorageAvailable)
      throw new Error(
        "Release artifact qualification requires a real protected credential service.",
      );
    if (process.env.STREAMSKOPE_PLUGIN_TEST_PUBLISHERS !== undefined)
      throw new Error("Release artifact qualification must use the shipped publisher registry.");
  });

  for (const hostRelease of ["0.9.0", "0.9.1"]) {
    for (const component of ["eda", "nsp"] as const) {
      test(`imports, preserves and reinstalls ${component.toUpperCase()} release bytes offline on host ${hostRelease}`, async ({
        browserName: _browserName,
      }, info) => {
        test.setTimeout(180_000);
        const directory = resolve(directories[component]!);
        const manifests = (await readdir(directory)).filter((name) =>
          name.endsWith("-plugin.json"),
        );
        expect(manifests).toHaveLength(1);
        const metadata = parsePluginManifest(
          JSON.parse(await readFile(join(directory, manifests[0]!), "utf8")),
        );
        expect(isDevelopmentPluginVersion(metadata.version)).toBe(false);
        // This validator checks the exact primary/portable/shared-manifest/resource
        // identities and signature using the desktop's unmodified publisher registry.
        const manifest = await validateReleaseArtifacts(directory, component, metadata.version);
        expect(isPluginCompatibleWithHost(manifest, hostRelease)).toBe(true);
        expect(isPluginCompatibleWithHost(manifest, "0.10.0")).toBe(false);
        const plugin = OFFICIAL_PLUGINS.find((entry) => entry.directory === component)!;
        const assets = officialPluginAssets(plugin, manifest.version);
        const primary = await readFile(join(directory, assets.packageAsset));
        const portablePath = join(directory, assets.portablePackageAsset);
        const portable = parsePortablePluginPackage(await readFile(portablePath));
        expect(portable.publisher).toBeDefined();
        const running = await startElectronPluginFixture(primary, info, [], {
          STREAMSKOPE_PLUGIN_TEST_HOST_RELEASE: hostRelease,
          STREAMSKOPE_PLUGIN_TEST_FILE: portablePath,
          STREAMSKOPE_PLUGIN_TEST_OFFLINE: "1",
        });
        try {
          const { page } = running;
          await page.getByRole("button", { name: "Preferences", exact: true }).click();
          await page.getByRole("tab", { name: "Plugins", exact: true }).click();
          await page.getByRole("button", { name: "Install from file", exact: true }).click();
          const review = page.getByRole("dialog", { name: "Review plugin", exact: true });
          await expect(review).toBeVisible();
          await expect(review).toContainText(
            `${portable.publisher!.name} (${portable.publisher!.keyId})`,
          );
          await expect(review).toContainText(portable.sha256);
          await expect(review).toContainText("Signed local file");
          await review.getByRole("button", { name: "Install plugin", exact: true }).click();
          await expect(review).toBeHidden();
          const card = page.getByRole("region", { name: manifest.name, exact: true });
          await expect(card).toContainText(`Active version ${manifest.version}`);
          const original = await installation(page, manifest.id);
          expect(original?.activationId).toBeTruthy();
          expect(original?.rendererUrl).toBeTruthy();

          await page.getByRole("button", { name: "Install from file", exact: true }).click();
          await expect(review).toContainText("already installed");
          await expect(
            review.getByRole("button", { name: "Install plugin", exact: true }),
          ).toHaveCount(0);
          await review.getByRole("button", { name: "Close", exact: true }).click();
          expect(await installation(page, manifest.id)).toEqual(original);
          await card.getByRole("button", { name: "Remove", exact: true }).click();
          await page
            .getByRole("dialog", { name: /^Remove/u })
            .getByRole("button", { name: "Remove plugin", exact: true })
            .click();
          await expect(
            page.getByText(`Active version ${manifest.version}`, { exact: true }),
          ).toHaveCount(0);

          const cached = page.getByRole("region", {
            name: `Cached ${manifest.name} ${manifest.version}`,
            exact: true,
          });
          await cached.getByRole("button", { name: "Use cached package", exact: true }).click();
          await expect(review).toContainText("Verified local cache");
          await expect(review).toContainText(portable.sha256);
          await review.getByRole("button", { name: "Install plugin", exact: true }).click();
          await expect(card).toContainText(`Active version ${manifest.version}`);
          const reinstalled = await installation(page, manifest.id);
          expect(reinstalled?.activationId).not.toBe(original?.activationId);
          expect(await page.evaluate(() => performance.timeOrigin)).toBe(running.origin);
          expect(running.application.process().pid).toBe(running.processId);
          expect(running.errors).toEqual([]);
          expect(running.assetFailures).toEqual([]);
          await page.screenshot({ path: info.outputPath("release-portable-installed.png") });
          await info.attach("release-artifact-native-evidence", {
            body: JSON.stringify(
              {
                scope:
                  "Native source Electron shell with stable host compatibility override; unchanged signed release packages; scripted file selection; not installer or upgrade acceptance.",
                platform: process.platform,
                architecture: process.arch,
                hostRelease,
                pluginId: manifest.id,
                pluginVersion: manifest.version,
                primarySha256: pluginPackageSha256(primary),
                portableSha256: portable.sha256,
                publisher: portable.publisher,
                resources: manifest.resources ?? [],
                security: running.security,
                original,
                reinstalled,
                githubUnavailable: true,
                sameProcess: true,
                sameWindow: true,
                rendererReloaded: false,
              },
              null,
              2,
            ),
            contentType: "application/json",
          });
        } finally {
          await running.close();
        }
      });
    }
  }
});
