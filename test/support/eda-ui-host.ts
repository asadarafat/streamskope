import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { parsePluginJson, parsePluginManifest } from "../../src/plugins/validation";
import edaManifestJson from "../../plugins/eda/manifest.json";
import {
  EDA_PLUGIN_ID,
  EDA_PROTOCOL_VERSION,
  parseEdaCaptureCommand,
  parseEdaCaptureResponse,
} from "../../plugins/eda/contracts";
import {
  isEdaUiCommand,
  type EdaUiCommand,
  type EdaUiHost,
  type EdaUiResponse,
} from "../../plugins/eda/ui/host";

import { testHostResponse as coreResponse } from "./host-response";

export const edaPluginManifest = parsePluginManifest(edaManifestJson);

export function testHostResponse<Command extends EdaUiCommand>(
  command: Command,
  value: unknown,
): EdaUiResponse<Command["command"]> {
  const envelope = value as Record<string, unknown>;
  return (
    isEdaUiCommand(command)
      ? parseEdaCaptureResponse({ ...envelope, version: EDA_PROTOCOL_VERSION })
      : coreResponse(
          { ...command, version: HOST_PROTOCOL_VERSION },
          { ...envelope, version: HOST_PROTOCOL_VERSION },
        )
  ) as EdaUiResponse<Command["command"]>;
}

export function testHostExecute(
  dispatch: (command: EdaUiCommand) => Promise<unknown>,
): EdaUiHost["execute"] {
  return async <Command extends EdaUiCommand>(
    command: Command,
  ): Promise<EdaUiResponse<Command["command"]>> =>
    testHostResponse(command, await dispatch(command));
}

export function testHostAccepted<Command extends EdaUiCommand>(
  command: Command,
  correlationId: string,
): EdaUiResponse<Command["command"]> {
  return testHostResponse(command, {
    command: command.command,
    id: command.id,
    ok: true,
    version: EDA_PROTOCOL_VERSION,
    result: { correlationId },
  });
}

/** Real generic transport around a private EDA fixture, including profile/event validation. */
export function edaDesktopHost(eda: EdaUiHost, installed = true): StreamSkopeHost {
  async function execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  async function execute(command: HostCommand): Promise<HostCommandResponse> {
    const base = {
      command: command.command,
      id: command.id,
      ok: true,
      version: HOST_PROTOCOL_VERSION,
    };
    if (command.command === "plugins.list") {
      return coreResponse(command, {
        ...base,
        result: {
          correlationId: command.id,
          pluginSnapshot: {
            revision: 0,
            plugins: installed
              ? [
                  {
                    id: EDA_PLUGIN_ID,
                    active: edaPluginManifest,
                    activationId: "eda-fixture",
                    installed: edaPluginManifest,
                    pending: null,
                    restartRequired: false,
                    rendererUrl: `/plugins/${EDA_PLUGIN_ID}/fixture/renderer.js`,
                  },
                ]
              : [],
          },
        },
      });
    }
    if (command.command === "plugin.execute") {
      const output = await eda.execute(
        parseEdaCaptureCommand({
          command: command.payload.method,
          id: command.id,
          payload: command.payload.input,
          version: EDA_PROTOCOL_VERSION,
        }),
      );
      return coreResponse(command, {
        ...base,
        result: { correlationId: command.id, output: parsePluginJson(output) },
      });
    }
    return eda.execute(command);
  }
  return {
    execute,
    openExternalUrl: (url) => eda.openExternalUrl(url),
    subscribe: (listener) =>
      eda.subscribe((event) => {
        listener(
          parseHostEvent(
            event.event === "edaCapture.progress"
              ? {
                  event: "plugin.event",
                  sequence: event.sequence,
                  version: HOST_PROTOCOL_VERSION,
                  payload: { pluginId: EDA_PLUGIN_ID, name: event.event, data: event.payload },
                }
              : event,
          ),
        );
      }),
  };
}
