import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

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

export function selectLiveEdaTopic(spec: unknown, topics: readonly string[]): string | undefined {
  const exports =
    spec !== null && typeof spec === "object" && "exports" in spec ? spec.exports : [];
  if (Array.isArray(exports)) {
    for (const entry of exports as readonly unknown[]) {
      if (
        entry !== null &&
        typeof entry === "object" &&
        "mode" in entry &&
        entry.mode === "periodic" &&
        "topic" in entry &&
        typeof entry.topic === "string" &&
        topics.includes(entry.topic)
      )
        return entry.topic;
    }
  }
  return topics[0];
}
