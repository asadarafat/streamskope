import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import {
  inspectPassphraseVault,
  openPassphraseVault,
} from "../../src/platform/node/vault/passphrase-vault";

async function main(): Promise<void> {
  const dataRoot = process.argv[2];
  if (dataRoot === undefined) throw new Error("Expected the independent fixture directory.");
  const passphrase = "independent startup fixture passphrase";
  // Source fallback workers resolve from cwd. This fresh empty directory reproduces
  // a missing deployment worker without changing files in the shared repository.
  process.chdir(dataRoot);
  let startupRejected = false;
  try {
    const runtime = await openBrowserRuntime(dataRoot, passphrase, "create");
    await runtime.lock();
  } catch (error) {
    if (
      !(error instanceof Error) ||
      error.message !== "The StreamSkope trust-material worker is unavailable."
    )
      throw new Error("The independent fixture did not reach the missing-worker failure.", {
        cause: error,
      });
    startupRejected = true;
  }
  if (!startupRejected) throw new Error("The incomplete deployment unexpectedly started.");
  const metadata = await readFile(join(dataRoot, "vault.json"));
  const vault = await openPassphraseVault({ dataRoot, passphrase, mode: "unlock" });
  await vault.lock();
  const vaultPreserved = metadata.equals(await readFile(join(dataRoot, "vault.json")));
  const formatPreserved = (await inspectPassphraseVault(dataRoot)) === "present";
  process.stdout.write(
    `${JSON.stringify({ startupRejected, leaseReleased: true, vaultPreserved, formatPreserved })}\n`,
  );
}

void main().catch(() => {
  process.stderr.write("Independent browser startup recovery did not complete.\n");
  process.exitCode = 1;
});
