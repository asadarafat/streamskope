import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  formatOperationalDiagnostic,
  operationalDiagnostic,
  OperationalDiagnosticError,
  type OperationalDiagnostic,
} from "../diagnostics";

import { openBrowserRuntime } from "./browser-runtime";
import { inspectPassphraseVault } from "./vault/passphrase-vault";
import { startWebGateway } from "./web-gateway";

function writeDiagnostic(diagnostic: OperationalDiagnostic): void {
  try {
    process.stderr.write(`${formatOperationalDiagnostic(diagnostic)}\n`);
  } catch {
    /* Console availability cannot change startup or cleanup outcomes. */
  }
}

async function main(): Promise<void> {
  const dataRoot = process.env.STREAMSKOPE_DATA_DIR;
  if (dataRoot === undefined || dataRoot.length === 0) {
    throw new OperationalDiagnosticError("BROWSER_CONFIGURATION_INVALID");
  }
  const portValue = process.env.STREAMSKOPE_PORT ?? "8080";
  if (!/^[1-9]\d{0,4}$/u.test(portValue) || Number(portValue) > 65_535) {
    throw new OperationalDiagnosticError("BROWSER_CONFIGURATION_INVALID");
  }
  const port = Number(portValue);
  const entryDirectory =
    typeof __dirname === "string" ? __dirname : fileURLToPath(new URL(".", import.meta.url));
  const rendererRoot =
    process.env.STREAMSKOPE_RENDERER_DIR ?? resolve(entryDirectory, "../renderer");
  const gateway = await startWebGateway({
    port,
    hostname: process.env.STREAMSKOPE_LISTEN_HOST ?? "127.0.0.1",
    publicOrigin: process.env.STREAMSKOPE_PUBLIC_ORIGIN ?? `http://127.0.0.1:${port}`,
    rendererRoot,
    dataRoot,
    inspectVault: () => inspectPassphraseVault(dataRoot),
    openRuntime: (passphrase, mode) => openBrowserRuntime(dataRoot, passphrase, mode),
    onDiagnostic: writeDiagnostic,
  });
  process.stdout.write(`StreamSkope browser host: ${gateway.origin}\n`);
  if (gateway.setupCodePath !== undefined) {
    process.stdout.write(`First-time setup code file: ${gateway.setupCodePath}\n`);
  }
  let shuttingDown = false;
  const stop = (): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    void gateway.close().then(
      () => process.exit(0),
      (error: unknown) => {
        writeDiagnostic(operationalDiagnostic(error, "CLEANUP_UNCONFIRMED"));
        process.exit(1);
      },
    );
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
}

void main().catch((error: unknown) => {
  // Never print nested authentication, profile, plugin or credential errors.
  writeDiagnostic(operationalDiagnostic(error, "BROWSER_START_FAILED"));
  process.exitCode = 1;
});
