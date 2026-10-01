import type { JsonObject, PluginProfileSource } from "../../../src/plugins/contracts";
import { parsePluginJson } from "../../../src/plugins/validation";

import { EDA_PLUGIN_ID } from "./types";
import type { ProfileEdaCaptureSource } from "./profile-types";
import { parseProfileSource } from "./profile-validation";

export function toPluginProfileSource(source: ProfileEdaCaptureSource): PluginProfileSource {
  return {
    kind: "plugin",
    pluginId: EDA_PLUGIN_ID,
    version: 1,
    data: parsePluginJson(parseProfileSource(source, "profile.source")) as JsonObject,
  };
}
export function fromPluginProfileSource(
  source: PluginProfileSource | undefined,
): ProfileEdaCaptureSource | undefined {
  if (source === undefined || source.pluginId !== EDA_PLUGIN_ID || source.version !== 1)
    return undefined;
  return parseProfileSource(source.data, "profile.source.data");
}
