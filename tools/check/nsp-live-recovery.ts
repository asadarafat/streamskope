import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { NSP_PLUGIN_ID, parseNspConnectInput } from "../../plugins/nsp/contracts";
import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { parsePluginJson } from "../../src/plugins/validation";

/** Kill an actual package-loaded host after NSP accepts an execution and its journal reaches disk. */
export async function interruptNspRetrieval(root: string): Promise<void> {
  const child = fork(fileURLToPath(import.meta.url), ["--interrupt-after-journal", root], {
    execArgv: ["--import", "tsx"],
    stdio: ["ignore", "ignore", "ignore", "ipc"],
  });
  const exited = once(child, "exit");
  try {
    const accepted = await Promise.race([
      once(child, "message", { signal: AbortSignal.timeout(120_000) }),
      exited.then(() => {
        throw new Error("The interrupted NSP fixture exited before persisting an execution.");
      }),
    ]);
    assert.deepEqual(accepted[0], { executionJournaled: true });
  } finally {
    child.kill("SIGKILL");
    await exited;
  }
}

async function interruptedHost(root: string): Promise<void> {
  assert(!process.env.GITHUB_ACTIONS, "NSP live interruption is local only.");
  const config = process.env.STREAMSKOPE_NSP_CONFIG;
  assert(config, "STREAMSKOPE_NSP_CONFIG is required.");
  const input = parseNspConnectInput(JSON.parse(await readFile(config, "utf8")));
  const store = new PluginStore(root);
  const persist = store.writeRecoveryState.bind(store);
  store.writeRecoveryState = async (id, value): Promise<void> => {
    await persist(id, value);
    if (
      id === NSP_PLUGIN_ID &&
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      typeof value.executionId === "string"
    ) {
      process.send?.({ executionJournaled: true });
      // Deterministic crash boundary: real journal, real execution, no fabricated recovery state.
      await new Promise<void>(() => undefined);
    }
  };
  const runtime = new PluginRuntime({ store });
  const facade = createKafkaBackend(
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    runtime,
  );
  try {
    const active = (await runtime.list()).plugins.find((plugin) => plugin.id === NSP_PLUGIN_ID);
    assert(active?.activationId, "The recovery fixture requires an installed package.");
    await facade.execute({
      command: "plugin.execute",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {
        pluginId: NSP_PLUGIN_ID,
        activationId: active.activationId,
        method: "nspCapture.connect",
        input: parsePluginJson(input),
      },
    });
    throw new Error("NSP retrieval did not reach the journal interruption boundary.");
  } finally {
    await facade.shutdown();
  }
}

if (
  process.argv[1] !== undefined &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url) &&
  process.argv[2] === "--interrupt-after-journal"
) {
  void interruptedHost(process.argv[3]!).catch(() => {
    // Live credentials and remote payloads must not appear in child logs.
    process.exitCode = 1;
    process.disconnect?.();
  });
}
