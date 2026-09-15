import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import {
  access,
  cp,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  unlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, relative, resolve } from "node:path";

import { packager } from "@electron/packager";

import {
  assertVerificationApplicationContents,
  assertVerificationBundleContents,
} from "./package-content-policy";
import {
  pruneElectronLocales,
  type ElectronLocalePruningEvidence,
} from "./electron-package-locales";
import {
  ELECTRON_RUNTIME_EFFICIENCY_POLICY,
  assertPackageEfficiency,
} from "./electron-runtime-efficiency-policy";
import { nativePackageLayout, productionSigningOptions } from "./package-platform";

interface ProjectManifest {
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly description?: string;
  readonly devDependencies?: Readonly<Record<string, string>>;
  readonly license?: string;
  readonly name: string;
  readonly version: string;
}

interface VerificationReport {
  readonly applicationFileCount: number;
  readonly applicationManifestPath: string;
  readonly archivePath: string;
  readonly arch: string;
  readonly artifactBytes: number;
  readonly bundlePath: string;
  readonly check: "verification-package";
  readonly command: string;
  readonly electronVersion: string;
  readonly executablePath: string;
  readonly localeFiles: readonly string[];
  readonly nodeVersion: string;
  readonly outcome: "passed";
  readonly packageBudgetBytes: number;
  readonly platform: string;
  readonly productionSigningRequired: boolean;
  readonly removedLocaleBytes: number;
  readonly sampleMethod: string;
  readonly schemaVersion: 1;
}

interface ApplicationContentEntry {
  readonly bytes: number;
  readonly path: string;
  readonly sha256: string;
}

const repositoryRoot = process.cwd();
const packageOutput = resolve(repositoryRoot, "dist/package");
const stagingPrefix = join(tmpdir(), "streamskope-package-");

function requireExactVersion(
  dependencies: Readonly<Record<string, string>> | undefined,
  name: string,
): string {
  const version = dependencies?.[name];
  if (version === undefined || !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/u.test(version)) {
    throw new Error(`${name} must have an exact package-manifest version.`);
  }
  return version;
}

async function run(command: string, arguments_: readonly string[], cwd: string): Promise<void> {
  await new Promise<void>((resolveRun, reject) => {
    const environment = { ...process.env };
    for (const name of ["STREAMSKOPE_APPLE_APP_PASSWORD", "WINDOWS_CERTIFICATE_PASSWORD"]) {
      delete environment[name];
    }
    const child = spawn(command, arguments_, {
      cwd,
      env: environment,
      stdio: "inherit",
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (signal !== null) {
        reject(new Error(`${command} stopped by signal ${signal}.`));
      } else if (code !== 0) {
        reject(new Error(`${command} exited with status ${String(code)}.`));
      } else {
        resolveRun();
      }
    });
  });
}

async function filesBelow(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const paths: string[] = [];
  for (const entry of entries) {
    const absolutePath = join(root, entry.name);
    if (entry.isDirectory()) {
      paths.push(...(await filesBelow(absolutePath)));
    } else {
      paths.push(absolutePath);
    }
  }
  return paths;
}

async function removeSourceMaps(root: string): Promise<void> {
  for (const path of await filesBelow(root)) {
    if (path.endsWith(".map")) {
      await unlink(path);
    }
  }
}

async function artifactBytes(paths: readonly string[]): Promise<number> {
  let total = 0;
  for (const path of paths) {
    const metadata = await lstat(path);
    if (metadata.isFile()) {
      total += metadata.size;
    }
  }
  return total;
}

async function applicationContent(
  root: string,
  paths: readonly string[],
): Promise<ApplicationContentEntry[]> {
  const entries: ApplicationContentEntry[] = [];
  for (const path of paths) {
    const content = await readFile(path);
    entries.push({
      bytes: content.byteLength,
      path: relative(root, path).replaceAll("\\", "/"),
      sha256: createHash("sha256").update(content).digest("hex"),
    });
  }
  return entries.sort((left, right) => left.path.localeCompare(right.path));
}

function packageArchitecture(): "arm64" | "x64" {
  if (process.arch !== "arm64" && process.arch !== "x64") {
    throw new Error(`Architecture ${process.arch} is not a verified package target.`);
  }
  return process.arch;
}

function packagePlatform(): "darwin" | "linux" | "win32" {
  if (
    process.platform !== "darwin" &&
    process.platform !== "linux" &&
    process.platform !== "win32"
  ) {
    throw new Error(`${process.platform} is not a verified package target.`);
  }
  return process.platform;
}

async function stageApplication(
  stagingDirectory: string,
  manifest: ProjectManifest,
  npmCli: string,
): Promise<void> {
  await cp(resolve(repositoryRoot, "LICENSE"), join(stagingDirectory, "LICENSE"));
  await cp(resolve(repositoryRoot, "package.json"), join(stagingDirectory, "package.json"));
  await cp(
    resolve(repositoryRoot, "package-lock.json"),
    join(stagingDirectory, "package-lock.json"),
  );
  await run(
    process.execPath,
    [npmCli, "ci", "--omit=dev", "--ignore-scripts", "--no-audit", "--no-fund"],
    stagingDirectory,
  );
  await rm(join(stagingDirectory, "node_modules", "jks-js", "examples"), {
    force: true,
    recursive: true,
  });
  await rm(join(stagingDirectory, "node_modules", "ssh2", "test"), {
    force: true,
    recursive: true,
  });

  await mkdir(join(stagingDirectory, "dist/electron"), { recursive: true });
  await cp(
    resolve(repositoryRoot, "dist/electron/main.cjs"),
    join(stagingDirectory, "dist/electron/main.cjs"),
  );
  await cp(
    resolve(repositoryRoot, "dist/electron/preload.cjs"),
    join(stagingDirectory, "dist/electron/preload.cjs"),
  );
  await cp(
    resolve(repositoryRoot, "dist/electron/trust-material-worker.cjs"),
    join(stagingDirectory, "dist/electron/trust-material-worker.cjs"),
  );
  await cp(resolve(repositoryRoot, "dist/renderer"), join(stagingDirectory, "dist/renderer"), {
    recursive: true,
  });
  await removeSourceMaps(join(stagingDirectory, "node_modules"));

  const runtimeManifest = {
    dependencies: {
      "@platformatic/kafka": requireExactVersion(manifest.dependencies, "@platformatic/kafka"),
      "jks-js": requireExactVersion(manifest.dependencies, "jks-js"),
      "node-forge": requireExactVersion(manifest.dependencies, "node-forge"),
      ssh2: requireExactVersion(manifest.dependencies, "ssh2"),
    },
    description: manifest.description,
    license: manifest.license,
    main: "dist/electron/main.cjs",
    name: manifest.name,
    private: true,
    version: manifest.version,
  };
  await writeFile(
    join(stagingDirectory, "package.json"),
    `${JSON.stringify(runtimeManifest, null, 2)}\n`,
    "utf8",
  );
  await rm(join(stagingDirectory, "package-lock.json"));
}

async function main(): Promise<void> {
  const npmCli = process.env.npm_execpath;
  if (npmCli === undefined || npmCli.length === 0) {
    throw new Error(
      "Run package verification through npm run package:verify or package:release:verify.",
    );
  }
  let productionRelease = false;
  if (process.argv.length === 3 && process.argv[2] === "--release") {
    productionRelease = true;
  } else if (process.argv.length !== 2) {
    throw new Error("Package verification accepts only the optional --release flag.");
  }
  if (packageOutput !== resolve(repositoryRoot, "dist", "package")) {
    throw new Error("Refusing to write outside the owned package output directory.");
  }
  const manifest = JSON.parse(
    await readFile(resolve(repositoryRoot, "package.json"), "utf8"),
  ) as ProjectManifest;
  const electronVersion = requireExactVersion(manifest.devDependencies, "electron");
  const arch = packageArchitecture();
  const platform = packagePlatform();
  const stagingDirectory = await mkdtemp(stagingPrefix);
  let localePruningEvidence: ElectronLocalePruningEvidence | undefined;

  try {
    await stageApplication(stagingDirectory, manifest, npmCli);
    await rm(packageOutput, { force: true, recursive: true });
    await mkdir(packageOutput, { recursive: true });
    const bundlePaths = await packager({
      arch,
      asar: true,
      afterExtract: [
        async ({ arch: extractedArch, buildPath, platform: extractedPlatform }): Promise<void> => {
          if (extractedArch !== arch || extractedPlatform !== platform) {
            throw new Error(
              `Refusing locale pruning for unexpected target ${extractedPlatform}-${extractedArch}.`,
            );
          }
          localePruningEvidence = await pruneElectronLocales(buildPath, platform);
        },
      ],
      dir: stagingDirectory,
      electronVersion,
      executableName: "StreamSkope",
      icon: resolve(
        repositoryRoot,
        "assets/icons",
        `streamskope.${platform === "darwin" ? "icns" : platform === "win32" ? "ico" : "png"}`,
      ),
      name: "StreamSkope",
      out: packageOutput,
      overwrite: true,
      platform,
      prune: false,
      quiet: true,
      ...productionSigningOptions(platform, productionRelease, process.env),
    });
    const [generatedBundle] = bundlePaths;
    if (bundlePaths.length !== 1 || generatedBundle === undefined) {
      throw new Error(`Expected one verification bundle; received ${String(bundlePaths.length)}.`);
    }
    if (localePruningEvidence === undefined) {
      throw new Error("Electron locale pruning did not produce package evidence.");
    }

    const bundlePath = resolve(generatedBundle);
    const layout = nativePackageLayout(bundlePath, platform);
    const packagedExecutable = layout.executablePath;
    await access(packagedExecutable);
    const bundleFiles = await filesBelow(bundlePath);
    const packagedArtifactBytes = await artifactBytes(bundleFiles);
    assertPackageEfficiency({
      artifactBytes: packagedArtifactBytes,
      localeFiles: localePruningEvidence.localeFiles,
      platform,
      removedLocaleBytes: localePruningEvidence.removedLocaleBytes,
    });
    const bundleRelativePaths = bundleFiles.map((path) => relative(bundlePath, path));
    assertVerificationBundleContents(bundleRelativePaths);
    const archivePath = layout.archivePath;
    const inspectionDirectory = join(stagingDirectory, "archive-inspection");
    await mkdir(inspectionDirectory, { recursive: true });
    await run(
      process.execPath,
      [npmCli, "exec", "--no", "--", "asar", "extract", archivePath, inspectionDirectory],
      repositoryRoot,
    );
    const applicationFiles = await filesBelow(inspectionDirectory);
    const applicationRelativePaths = applicationFiles.map((path) =>
      relative(inspectionDirectory, path),
    );
    assertVerificationApplicationContents(applicationRelativePaths);
    const applicationManifestPath = join(packageOutput, "application-content.json");
    await writeFile(
      applicationManifestPath,
      `${JSON.stringify(
        {
          files: await applicationContent(inspectionDirectory, applicationFiles),
          name: manifest.name,
          schemaVersion: 1,
          version: manifest.version,
        },
        null,
        2,
      )}\n`,
      "utf8",
    );
    const report: VerificationReport = {
      applicationFileCount: applicationFiles.length,
      applicationManifestPath,
      archivePath,
      arch,
      artifactBytes: packagedArtifactBytes,
      bundlePath,
      check: "verification-package",
      command: productionRelease ? "npm run package:release:verify" : "npm run package:verify",
      electronVersion,
      executablePath: packagedExecutable,
      localeFiles: localePruningEvidence.localeFiles,
      nodeVersion: process.version,
      outcome: "passed",
      packageBudgetBytes: ELECTRON_RUNTIME_EFFICIENCY_POLICY.packageBytes[platform],
      platform,
      productionSigningRequired: productionRelease,
      removedLocaleBytes: localePruningEvidence.removedLocaleBytes,
      sampleMethod:
        "Electron packager post-extraction locale pruning, recursive artifact byte count, ASAR inspection and native launch.",
      schemaVersion: 1,
    };
    await writeFile(
      join(packageOutput, "verification.json"),
      `${JSON.stringify(report, null, 2)}\n`,
      "utf8",
    );
    process.stdout.write(
      `Verified ${basename(bundlePath)}: ${String(report.applicationFileCount)} application files, ${String(report.artifactBytes)} bytes.\n`,
    );
  } finally {
    await rm(stagingDirectory, { force: true, recursive: true });
  }
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`Package verification failed: ${detail}\n`);
  process.exitCode = 1;
});
