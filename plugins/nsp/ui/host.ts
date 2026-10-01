import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../../../src/features/kafka/contracts";
import { parsePluginJson } from "../../../src/plugins/validation";
import {
  NSP_PLUGIN_ID,
  parseNspProgress,
  parseNspResult,
  type NspInputs,
  type NspMethod,
  type NspProgress,
  type NspResult,
} from "../contracts";

export interface NspUiHost {
  execute<Method extends NspMethod>(
    method: Method,
    input: NspInputs[Method],
    requestId?: string,
  ): Promise<NspResult>;
  subscribe(listener: (progress: NspProgress) => void): () => void;
}

export function createNspUiHost(host: StreamSkopeHost): NspUiHost {
  return {
    execute: async (
      method,
      input,
      requestId = globalThis.crypto.randomUUID(),
    ): Promise<NspResult> => {
      const response = await host.execute({
        command: "plugin.execute",
        id: requestId,
        payload: { pluginId: NSP_PLUGIN_ID, method, input: parsePluginJson(input) },
        version: HOST_PROTOCOL_VERSION,
      });
      return response.ok
        ? parseNspResult(response.result.output)
        : { ok: false, error: response.error };
    },
    subscribe: (listener) =>
      host.subscribe((event) => {
        if (
          event.event !== "plugin.event" ||
          event.payload.pluginId !== NSP_PLUGIN_ID ||
          event.payload.name !== "nspCapture.progress"
        )
          return;
        listener(parseNspProgress(event.payload.data));
      }),
  };
}
