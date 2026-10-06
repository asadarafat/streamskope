import { createRequire } from "node:module";

import { PlatformaticAdminFactory } from "../../../features/kafka/engine/platformatic-admin";
import type { PluginBackend, PluginBackendModule } from "../../../plugins/api";

export function loadPluginBackendModule(path: string): Promise<PluginBackendModule> {
  const require = createRequire(path);
  delete require.cache[require.resolve(path)];
  const value: unknown = require(path);
  if (
    value === null ||
    typeof value !== "object" ||
    !("activate" in value) ||
    typeof value.activate !== "function"
  )
    throw new Error("The plugin does not export its backend activation function.");
  return Promise.resolve(value as PluginBackendModule);
}

export function assertPluginBackend(value: PluginBackend): void {
  for (const name of [
    "execute",
    "validateProfile",
    "beforeExit",
    "resolveExit",
    "close",
    "beforeChange",
    "prepareUnload",
  ] as const) {
    if (typeof value?.[name] !== "function") throw new Error(`Plugin backend is missing ${name}.`);
  }
}

export async function probePluginTopics(
  brokers: readonly string[],
  signal?: AbortSignal,
): Promise<readonly string[]> {
  signal?.throwIfAborted();
  const admin = new PlatformaticAdminFactory().create({
    brokers,
    tlsEnabled: false,
    operationTimeoutMs: 10_000,
  });
  const abort = (): void => {
    void admin.close().catch(() => undefined);
  };
  signal?.addEventListener("abort", abort, { once: true });
  try {
    const topics = await admin.listTopics();
    signal?.throwIfAborted();
    return topics;
  } finally {
    signal?.removeEventListener("abort", abort);
    await admin.close();
  }
}
