import {
  HOST_PROTOCOL_VERSION as CORE_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandName,
  type HostCommandResponse,
  type HostEvent,
  type StreamSkopeHost,
} from "../../../src/features/kafka/contracts";
import { parsePluginJson } from "../../../src/plugins/validation";
import {
  EDA_CAPTURE_COMMANDS,
  EDA_PLUGIN_ID,
  EDA_PROTOCOL_VERSION,
  parseEdaCaptureCommand,
  parseEdaCaptureEvent,
  parseEdaCaptureResponse,
  type EdaCaptureCommand,
  type EdaCaptureCommandName,
  type EdaCaptureCommandResponse,
  type EdaCaptureHostEvent,
} from "../contracts";

type PluginCoreCommand<Command> = Command extends HostCommand
  ? Omit<Command, "version"> & { readonly version: number }
  : never;

export type EdaUiCommand = PluginCoreCommand<HostCommand> | EdaCaptureCommand;
export type EdaUiCommandName = EdaUiCommand["command"];
export type EdaUiResponse<Name extends EdaUiCommandName = EdaUiCommandName> =
  Name extends EdaCaptureCommandName
    ? EdaCaptureCommandResponse<Name>
    : Name extends HostCommandName
      ? HostCommandResponse<Name>
      : never;
export type EdaUiEvent = HostEvent | EdaCaptureHostEvent;
export type EdaUiEventListener = (event: EdaUiEvent) => void;

export interface EdaUiHost {
  execute<Command extends EdaUiCommand>(
    command: Command,
  ): Promise<EdaUiResponse<Command["command"]>>;
  subscribe(listener: EdaUiEventListener): () => void;
  openExternalUrl: StreamSkopeHost["openExternalUrl"];
}

export function isEdaUiCommand(command: EdaUiCommand): command is EdaCaptureCommand {
  return EDA_CAPTURE_COMMANDS.some((name) => name === command.command);
}

/** Keep EDA's private protocol behind the generic, versioned desktop plugin transport. */
export function createEdaUiHost(host: StreamSkopeHost): EdaUiHost {
  async function execute<Command extends EdaUiCommand>(
    command: Command,
  ): Promise<EdaUiResponse<Command["command"]>>;
  async function execute(
    command: EdaUiCommand,
  ): Promise<HostCommandResponse | EdaCaptureCommandResponse> {
    if (!isEdaUiCommand(command)) {
      return host.execute({ ...command, version: CORE_PROTOCOL_VERSION });
    }
    const checked = parseEdaCaptureCommand(command);
    const response = await host.execute({
      command: "plugin.execute",
      id: checked.id,
      payload: {
        pluginId: EDA_PLUGIN_ID,
        method: checked.command,
        input: parsePluginJson(checked.payload),
      },
      version: CORE_PROTOCOL_VERSION,
    });
    if (!response.ok) {
      return {
        command: checked.command,
        id: checked.id,
        ok: false,
        error: response.error,
        version: EDA_PROTOCOL_VERSION,
      };
    }
    const output = parseEdaCaptureResponse(response.result.output);
    if (output.command !== checked.command || output.id !== checked.id) {
      throw new Error("The EDA plugin response does not match its request.");
    }
    return output;
  }

  return {
    execute,
    openExternalUrl: (url) => host.openExternalUrl(url),
    subscribe: (listener) =>
      host.subscribe((event) => {
        if (event.event !== "plugin.event") {
          listener(event);
        } else if (event.payload.pluginId === EDA_PLUGIN_ID) {
          listener(
            parseEdaCaptureEvent({
              event: event.payload.name,
              payload: event.payload.data,
              sequence: event.sequence,
              version: EDA_PROTOCOL_VERSION,
            }),
          );
        }
      }),
  };
}
