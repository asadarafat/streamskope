import type { HostCommand } from "../../../features/kafka/contracts";
import type { PluginHostBindings } from "../../../plugins/api";
import {
  compatiblePluginProfileCommand,
  PLUGIN_PROFILE_REFRESH_UPGRADE,
} from "../../../plugins/profile-refresh-compatibility";

import { pluginProblem } from "./problem";

/** Read authoritative safe summaries before a plugin can replace protected settings. */
export async function assertPluginProfileRefresh(
  command: HostCommand,
  bindings: Pick<PluginHostBindings, "profiles">,
): Promise<void> {
  if (
    (command.command === "profiles.update" ||
      command.command === "profiles.test" ||
      command.command === "profiles.create") &&
    !compatiblePluginProfileCommand(command, await bindings.profiles())
  )
    throw pluginProblem(PLUGIN_PROFILE_REFRESH_UPGRADE);
}
