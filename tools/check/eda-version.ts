import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { EDA_TARGET_VERSION } from "../../plugins/eda/contracts/eda-capture-types";
import { EdaApiClient } from "../../plugins/eda/backend/eda-api-client";

export async function localEdaClient(signal?: AbortSignal): Promise<EdaApiClient | undefined> {
  const baseUrl = process.env.STREAMSKOPE_EDA_API_URL;
  const username = process.env.STREAMSKOPE_EDA_API_USERNAME;
  const password = process.env.STREAMSKOPE_EDA_API_PASSWORD;
  if (!baseUrl && !username && !password) return undefined;
  assert(baseUrl && username && password, "Configure EDA API URL, username and password together.");
  return new EdaApiClient(
    { baseUrl, username, password },
    {
      ...(process.env.STREAMSKOPE_EDA_API_CA
        ? { caPem: await readFile(process.env.STREAMSKOPE_EDA_API_CA, "utf8") }
        : {}),
      ...(process.env.STREAMSKOPE_EDA_API_CLIENT_SECRET
        ? { clientSecret: process.env.STREAMSKOPE_EDA_API_CLIENT_SECRET }
        : {}),
      rejectUnauthorized: true,
    },
    signal,
  );
}

export function requireTargetEdaVersion(releaseVersion: string): void {
  assert.equal(releaseVersion, EDA_TARGET_VERSION, "Running EDA must match the declared target.");
}

async function main(): Promise<void> {
  let observed: Awaited<ReturnType<EdaApiClient["clusterVersion"]>> | undefined;
  let outcome = "failed";
  let reason: string | undefined;
  try {
    if (process.env.GITHUB_ACTIONS === "true") {
      outcome = "not-checked";
      reason = "GitHub builds use the declared target; cluster verification runs locally.";
      process.stdout.write(`EDA target: ${EDA_TARGET_VERSION} (cluster not checked on GitHub).\n`);
    } else {
      const client = await localEdaClient(AbortSignal.timeout(60_000));
      assert(client, "Local EDA packaging requires API connection settings.");
      observed = await client.clusterVersion();
      requireTargetEdaVersion(observed.releaseVersion);
      outcome = "passed";
      process.stdout.write(
        `Running EDA verified: ${observed.buildVersion} → ${EDA_TARGET_VERSION}.\n`,
      );
    }
  } catch {
    reason = observed
      ? "Running EDA differs from the declared target."
      : "EDA API version could not be verified.";
    process.stderr.write(
      `EDA version verification failed. ${reason} Configure STREAMSKOPE_EDA_API_URL, STREAMSKOPE_EDA_API_USERNAME, STREAMSKOPE_EDA_API_PASSWORD and trusted CA settings.\n`,
    );
    process.exitCode = 1;
  }
  await mkdir("dist/ci", { recursive: true });
  await writeFile(
    "dist/ci/eda-version.json",
    JSON.stringify(
      {
        outcome,
        checkedAt: new Date().toISOString(),
        targetVersion: EDA_TARGET_VERSION,
        ...observed,
        ...(reason ? { reason } : {}),
      },
      null,
      2,
    ) + "\n",
  );
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  void main().catch(() => {
    process.stderr.write("Could not record EDA version verification evidence.\n");
    process.exitCode = 1;
  });
}
