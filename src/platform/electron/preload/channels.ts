export const DESKTOP_ACTION_CHANNEL = "streamskope:desktop-action";
export const DESKTOP_DOCUMENT_SAVE_CHANNEL = "streamskope:desktop-document-save";
export const DESKTOP_ARTIFACT_SAVE_CHANNEL = "streamskope:desktop-artifact-save";
export const EXTERNAL_URL_OPEN_CHANNEL = "streamskope:external-url-open";
export const HOST_COMMAND_CHANNEL = "streamskope:host-command";
export const HOST_EVENT_CHANNEL = "streamskope:host-event";
export const HOST_EVENT_ACK_CHANNEL = "streamskope:host-event-ack";
export const HOST_SUBSCRIBE_CHANNEL = "streamskope:host-subscribe";

export interface ProviderIpcChannels {
  readonly command: string;
  readonly event: string;
  readonly acknowledge: string;
  readonly subscribe: string;
}

/** Called only for IDs selected by trusted host composition. */
export function providerIpcChannels(id: string): ProviderIpcChannels {
  if (!/^[a-z][a-z0-9-]{0,31}$/u.test(id)) throw new Error("Invalid messaging provider ID.");
  if (id === "kafka")
    return {
      command: HOST_COMMAND_CHANNEL,
      event: HOST_EVENT_CHANNEL,
      acknowledge: HOST_EVENT_ACK_CHANNEL,
      subscribe: HOST_SUBSCRIBE_CHANNEL,
    };
  return {
    command: `streamskope:provider:${id}:command`,
    event: `streamskope:provider:${id}:event`,
    acknowledge: `streamskope:provider:${id}:event-ack`,
    subscribe: `streamskope:provider:${id}:subscribe`,
  };
}
