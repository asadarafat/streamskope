import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type ProfileSource,
  type StreamSkopeBackend,
} from "../contracts";
import type { KafkaApplicationSession, KafkaProfileService } from "../application";
import type { PluginRuntimePort } from "../../../plugins/api";

import {
  failureResponse,
  profilesChangedEvent,
  successResponse,
  translateFacadeFailure,
  type ActivityInput,
} from "./facade-support";

export type PluginHostCommand = Extract<
  HostCommand,
  { readonly command: `plugins.${string}` | "plugin.execute" }
>;
interface Bindings {
  readonly runtime?: PluginRuntimePort;
  readonly session: KafkaApplicationSession;
  readonly profiles: KafkaProfileService;
  readonly execute: StreamSkopeBackend["execute"];
  readonly publish: (event: HostEvent) => void;
  readonly nextSequence: () => number;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly disconnectPluginConnection: (pluginId: string) => Promise<void>;
}
function unavailable(pluginId?: string): Error {
  return Object.assign(
    new Error(
      pluginId === undefined
        ? "Plugin management is unavailable on this host."
        : `The ${pluginId} plugin is not active.`,
    ),
    {
      code: "BACKEND_UNAVAILABLE",
      stage: "backend",
      retryable: false,
      recovery:
        "Install or repair the required plugin in Preferences → Plugins. Saved profile information is retained.",
    },
  );
}
export class PluginFacadeController {
  private readonly unsubscribe: (() => void) | undefined;
  private readonly unsubscribeChanges: (() => void) | undefined;
  constructor(private readonly bindings: Bindings) {
    const runtime = bindings.runtime;
    if (runtime === undefined) return;
    runtime.bindHost({
      execute: bindings.execute,
      connectionActive: () =>
        ["connecting", "connected", "disconnecting"].includes(bindings.session.snapshot().state),
      profiles: async () => (await bindings.profiles.list()).profiles,
      deleteProfile: async (id) => {
        bindings.publish(
          profilesChangedEvent(await bindings.profiles.delete(id), bindings.nextSequence()),
        );
      },
      disconnectPluginConnection: bindings.disconnectPluginConnection,
      recordActivity: bindings.recordActivity,
      failure: (error, context) =>
        translateFacadeFailure(
          error,
          { ...context, activeStateChanged: false, connection: undefined },
          true,
        ).error,
    });
    this.unsubscribe = runtime.subscribe((payload) =>
      bindings.publish({
        event: "plugin.event",
        payload,
        sequence: bindings.nextSequence(),
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    this.unsubscribeChanges = runtime.subscribeChanges((payload) =>
      bindings.publish({
        event: "plugins.changed",
        payload,
        sequence: bindings.nextSequence(),
        version: HOST_PROTOCOL_VERSION,
      }),
    );
  }
  async execute(command: PluginHostCommand, correlationId: string): Promise<HostCommandResponse> {
    const runtime = this.bindings.runtime;
    const base = { id: command.id, ok: true as const, version: HOST_PROTOCOL_VERSION };
    try {
      if (command.command === "plugins.list")
        return {
          ...base,
          command: command.command,
          result: {
            correlationId,
            pluginSnapshot: (await runtime?.list()) ?? { revision: 0, plugins: [] },
          },
        };
      if (command.command === "plugins.catalog")
        return {
          ...base,
          command: command.command,
          result: {
            correlationId,
            pluginCatalog: (await runtime?.catalog()) ?? {
              plugins: [],
              error: "Plugins are unavailable on this host.",
            },
          },
        };
      if (command.command === "plugins.exit.prepare")
        return {
          ...base,
          command: command.command,
          result: { correlationId, pluginExit: (await runtime?.prepareExit()) ?? null },
        };
      if (runtime === undefined)
        throw unavailable("pluginId" in command.payload ? command.payload.pluginId : undefined);
      switch (command.command) {
        case "plugin.execute":
          return {
            ...base,
            command: command.command,
            result: {
              correlationId,
              output: await runtime.execute({
                ...command.payload,
                activationId: command.payload.activationId ?? "",
                requestId: command.id,
                correlationId,
              }),
            },
          };
        case "plugins.install":
        case "plugins.remove":
          return {
            ...base,
            command: command.command,
            result: {
              correlationId,
              pluginSnapshot: await (command.command === "plugins.install"
                ? runtime.install(command.payload.pluginId, command.payload.confirmationToken)
                : runtime.remove(command.payload.pluginId, command.payload.confirmationToken)),
            },
          };
        case "plugins.change.prepare":
          return {
            ...base,
            command: command.command,
            result: {
              correlationId,
              pluginChange: await runtime.prepareChange(
                command.payload.pluginId,
                command.payload.operation,
              ),
            },
          };
        case "plugins.renderer.failed":
          return {
            ...base,
            command: command.command,
            result: {
              correlationId,
              pluginSnapshot: await runtime.rendererFailed(
                command.payload.pluginId,
                command.payload.activationId,
                command.payload.error,
              ),
            },
          };
        case "plugins.restart":
          await runtime.restart();
          return successResponse(command, correlationId);
        case "plugins.exit.resolve":
          return {
            ...base,
            command: command.command,
            result: {
              correlationId,
              allowed: await runtime.resolveExit(command.payload.pluginId, command.payload.action),
            },
          };
      }
    } catch (error) {
      return failureResponse(
        command,
        translateFacadeFailure(
          error,
          { activeStateChanged: false, connection: undefined, correlationId },
          true,
        ).error,
      );
    }
  }
  async validateProfile(source: ProfileSource, brokers: readonly string[]): Promise<void> {
    if (this.bindings.runtime === undefined) throw unavailable(source.pluginId);
    await this.bindings.runtime.validateProfile(source, brokers);
  }
  async withProfileConnection<T>(
    source: ProfileSource,
    brokers: readonly string[],
    connect: () => Promise<T>,
  ): Promise<T> {
    if (this.bindings.runtime === undefined) throw unavailable(source.pluginId);
    return this.bindings.runtime.withProfileConnection(source, brokers, connect);
  }
  async close(): Promise<void> {
    try {
      await this.bindings.runtime?.close();
    } finally {
      this.unsubscribe?.();
      this.unsubscribeChanges?.();
    }
  }
}
