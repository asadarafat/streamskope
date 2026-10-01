import { HOST_PROTOCOL_VERSION, type StreamSkopeBackend } from "../../../features/kafka/contracts";
import type { PluginExitPrompt } from "../../../plugins/contracts";

/** Resolve each plugin's cleanup choice before the desktop destroys its backend. */
export async function confirmPluginExit(
  backend: Pick<StreamSkopeBackend, "execute">,
  choose: (prompt: PluginExitPrompt) => Promise<string>,
  reportFailure: (message: string) => Promise<void>,
): Promise<boolean> {
  try {
    for (let remaining = 32; remaining >= 0; remaining -= 1) {
      const prepared = await backend.execute({
        command: "plugins.exit.prepare",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (!prepared.ok) throw new Error(prepared.error.summary);
      const prompt = prepared.result.pluginExit;
      if (prompt === null) return true;
      if (remaining === 0) break;
      const action = await choose(prompt);
      const resolved = await backend.execute({
        command: "plugins.exit.resolve",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { pluginId: prompt.pluginId, action },
      });
      if (!resolved.ok) throw new Error(resolved.error.summary);
      if (!resolved.result.allowed) return false;
    }
    throw new Error("A plugin did not finish its exit workflow.");
  } catch {
    await reportFailure(
      "Plugin cleanup could not be confirmed. StreamSkope will stay open. Resolve the plugin's pending work and retry. Saved recovery information is retained.",
    );
    return false;
  }
}
