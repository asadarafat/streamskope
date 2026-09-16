import { createRequire } from "node:module";
import { join, resolve } from "node:path";

import {
  _electron as electron,
  expect,
  type ElectronApplication,
  type Page,
} from "@playwright/test";
import { build } from "vite";

import { PACKAGED_RENDERER_URL } from "../../src/platform/electron/main/packaged-renderer-origin";

import type { FixtureConfig, FixtureConnection } from "./kafka-fixture";

const repositoryRoot = process.cwd();
const require = createRequire(join(repositoryRoot, "package.json"));
const electronExecutable = require("electron") as string;

export async function buildElectronSmoke(outputDirectory: string): Promise<void> {
  const entries = [
    {
      emptyOutDir: true,
      input: resolve(repositoryRoot, "test/electron/smoke-main.ts"),
      name: "main",
    },
    {
      emptyOutDir: false,
      input: resolve(repositoryRoot, "test/electron/profile-main.ts"),
      name: "profile-main",
    },
    {
      emptyOutDir: false,
      input: resolve(repositoryRoot, "src/platform/electron/preload/index.ts"),
      name: "preload",
    },
    {
      emptyOutDir: false,
      input: resolve(repositoryRoot, "src/features/kafka/engine/trust-material-worker.ts"),
      name: "trust-material-worker",
    },
  ] as const;
  for (const entry of entries) {
    await build({
      build: {
        emptyOutDir: entry.emptyOutDir,
        minify: false,
        outDir: outputDirectory,
        rollupOptions: {
          external: ["electron"],
          input: entry.input,
          output: {
            codeSplitting: false,
            entryFileNames: `${entry.name}.cjs`,
            format: "cjs",
          },
        },
        sourcemap: false,
        ssr: true,
      },
      configFile: false,
      logLevel: "silent",
      root: repositoryRoot,
    });
  }
}

export async function buildRenderer(outputDirectory: string): Promise<string> {
  const rendererDirectory = join(outputDirectory, "renderer");
  await build({
    build: {
      emptyOutDir: true,
      outDir: rendererDirectory,
    },
    configFile: resolve(repositoryRoot, "config/vite.config.ts"),
    logLevel: "silent",
    root: repositoryRoot,
  });
  return PACKAGED_RENDERER_URL;
}

export async function connectElectronToFixture(
  page: Page,
  config: FixtureConfig,
  fixture: FixtureConnection,
): Promise<void> {
  await page.getByRole("button", { name: "Add profile" }).click();
  const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
  await editor.getByRole("textbox", { name: "Profile name" }).fill("Electron local aio");
  await editor.getByRole("textbox", { name: "Bootstrap brokers" }).fill(fixture.kafkaEndpoint);
  await editor.getByRole("combobox", { name: "Trust material format" }).click();
  await page.getByRole("option", { name: "PEM certificate" }).click();
  await editor.getByLabel("Trust material file").setInputFiles(fixture.caPath);
  await editor.getByRole("switch", { name: "Use OAuth OAUTHBEARER" }).click();
  await editor.getByRole("textbox", { name: "OAuth token endpoint" }).fill(fixture.oauthEndpoint);
  await editor.getByRole("textbox", { name: "OAuth client ID" }).fill(config.oauthClientId);
  await editor
    .getByRole("textbox", { name: "OAuth client secret", exact: true })
    .fill(config.oauthClientSecret);
  await editor.getByRole("textbox", { name: "OAuth scope" }).fill(config.oauthScope);
  await editor.getByRole("button", { name: "Save profile" }).click();
  await page.getByRole("button", { name: "Connect profile Electron local aio" }).click();
  await expect(page.getByLabel("Connection status")).toContainText("Connected");
}

function electronEnvironment(
  rendererUrl: string,
  rendererRoot: string,
  userDataPath: string,
  storage: "available" | "unavailable",
  externalUrlLogPath: string | undefined,
): Record<string, string> {
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined,
    ),
  );
  environment.STREAMSKOPE_PROFILE_RENDERER_URL = rendererUrl;
  environment.STREAMSKOPE_PROFILE_RENDERER_ROOT = rendererRoot;
  environment.STREAMSKOPE_PROFILE_STORAGE = storage;
  environment.STREAMSKOPE_PROFILE_USER_DATA = userDataPath;
  if (externalUrlLogPath !== undefined) {
    environment.STREAMSKOPE_PROFILE_EXTERNAL_URL_LOG = externalUrlLogPath;
  }
  return environment;
}

export async function launchProfileApplication(
  outputDirectory: string,
  rendererUrl: string,
  userDataPath: string,
  storage: "available" | "unavailable" = "available",
  externalUrlLogPath?: string,
  videoDirectory?: string,
): Promise<ElectronApplication> {
  return electron.launch({
    ...(videoDirectory === undefined
      ? {}
      : { recordVideo: { dir: videoDirectory, size: { width: 1440, height: 1000 } } }),
    args: [
      ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
      join(outputDirectory, "profile-main.cjs"),
    ],
    env: electronEnvironment(
      rendererUrl,
      join(outputDirectory, "renderer"),
      userDataPath,
      storage,
      externalUrlLogPath,
    ),
    executablePath: electronExecutable,
  });
}

export async function chooseNextElectronSavePath(
  application: ElectronApplication,
  filePath: string,
): Promise<void> {
  await application.evaluate(({ dialog }, selectedPath) => {
    Object.defineProperty(dialog, "showSaveDialog", {
      configurable: true,
      value: () => Promise.resolve({ canceled: false, filePath: selectedPath }),
    });
  }, filePath);
}
