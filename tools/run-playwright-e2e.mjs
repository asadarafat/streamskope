import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath, URL } from "node:url";

const project = process.argv[2];
if (
  project !== "electron" &&
  project !== "package" &&
  project !== "web" &&
  project !== "boundary"
) {
  process.stderr.write(
    "Playwright verification project must be web, electron, boundary or package.\n",
  );
  process.exit(2);
}

const require = createRequire(import.meta.url);
const playwrightCli = require.resolve("@playwright/test/cli");
const tsxCli = require.resolve("tsx/cli");
const artifactScanner = fileURLToPath(new URL("./scan-sensitive-artifacts.ts", import.meta.url));
const playwrightArguments = [
  playwrightCli,
  "test",
  "--config",
  "config/playwright.config.ts",
  `--project=${project === "boundary" ? "electron" : project}`,
  ...(project === "boundary"
    ? ["test/e2e/electron-host.spec.ts", "--grep", "narrow working preload"]
    : []),
  ...process.argv.slice(3),
];
const needsVirtualDisplay =
  (project === "electron" || project === "package" || project === "boundary") &&
  process.platform === "linux" &&
  process.env.DISPLAY === undefined;
const command = needsVirtualDisplay ? "xvfb-run" : process.execPath;
const commandArguments = needsVirtualDisplay
  ? ["-a", process.execPath, ...playwrightArguments]
  : playwrightArguments;
const childEnvironment = { ...process.env, STREAMSKOPE_TEST_PROJECT: project };
delete childEnvironment.NO_COLOR;
const child = spawn(command, commandArguments, {
  env: childEnvironment,
  stdio: "inherit",
});

child.once("error", (error) => {
  process.stderr.write(`Unable to start ${project} verification: ${error.message}\n`);
  process.exitCode = 1;
});
child.once("exit", (code, signal) => {
  if (signal !== null) {
    process.stderr.write(`${project} verification stopped by signal ${signal}.\n`);
  }
  const scanner = spawn(process.execPath, [tsxCli, artifactScanner], {
    env: childEnvironment,
    stdio: "inherit",
  });
  scanner.once("error", (error) => {
    process.stderr.write(`Unable to scan verification artifacts: ${error.message}\n`);
    process.exitCode = 1;
  });
  scanner.once("exit", (scannerCode, scannerSignal) => {
    if (scannerSignal !== null) {
      process.stderr.write(`Artifact scan stopped by signal ${scannerSignal}.\n`);
    }
    process.exitCode = code === 0 && scannerCode === 0 ? 0 : 1;
  });
});
