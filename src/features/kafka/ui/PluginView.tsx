import { useEffect, useRef, useState } from "react";
import { useColorScheme } from "@mui/material/styles";

import {
  HOST_PROTOCOL_VERSION,
  type ExternalUrlOpenResult,
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeHost,
} from "../contracts";
import type {
  PluginRenderer,
  PluginViewContext,
  PluginViewMount,
} from "../../../plugins/renderer-api";
import { StudioAlert } from "../../../platform/ui/controls";

/** The stable plugin API supplies the current desktop protocol version at dispatch. */
export function currentPluginHost(
  host: StreamSkopeHost,
  lifetime?: AbortSignal,
  activationId?: string,
): StreamSkopeHost {
  const requireActive = (): void => {
    if (lifetime?.aborted === true) throw new Error("This plugin view is no longer active.");
  };
  return {
    execute: async <Command extends HostCommand>(
      command: Command,
    ): Promise<HostCommandResponse<Command["command"]>> => {
      requireActive();
      const response = await host.execute({
        ...command,
        ...(command.command === "plugin.execute" && activationId !== undefined
          ? { payload: { ...command.payload, activationId } }
          : {}),
        version: HOST_PROTOCOL_VERSION,
      });
      requireActive();
      return response;
    },
    openExternalUrl: async (url): Promise<ExternalUrlOpenResult> => {
      requireActive();
      const result = await host.openExternalUrl(url);
      requireActive();
      return result;
    },
    subscribe: (listener): (() => void) => {
      requireActive();
      const unsubscribe = host.subscribe((event) => {
        if (lifetime?.aborted !== true) listener(event);
      });
      let closed = false;
      const cleanup = (): void => {
        if (closed) return;
        closed = true;
        unsubscribe();
        lifetime?.removeEventListener("abort", cleanup);
      };
      lifetime?.addEventListener("abort", cleanup, { once: true });
      return cleanup;
    },
  };
}

export function PluginView({
  renderer,
  context,
  lifetime,
  activationId,
}: {
  readonly renderer: PluginRenderer;
  readonly context: PluginViewContext;
  readonly lifetime?: AbortSignal;
  readonly activationId?: string;
}): React.JSX.Element {
  const container = useRef<HTMLDivElement>(null);
  const mounted = useRef<PluginViewMount | undefined>(undefined);
  const latestContext = useRef(context);
  const updateContext = useRef<(() => PluginViewContext) | undefined>(undefined);
  const reportFailure = useRef<((failure: unknown) => void) | undefined>(undefined);
  const [error, setError] = useState<string>();
  const { mode, systemMode } = useColorScheme();
  const themeMode = context.themeMode ?? (mode === "system" ? systemMode : mode);
  latestContext.current = { ...context, ...(themeMode === undefined ? {} : { themeMode }) };

  useEffect(() => {
    if (container.current === null || lifetime?.aborted === true) return;
    // A replaced plugin gets its own node while the previous React root disposes asynchronously.
    const mountTarget = document.createElement("div");
    container.current.append(mountTarget);
    const viewLifetime = new AbortController();
    const retire = (): void => viewLifetime.abort();
    lifetime?.addEventListener("abort", retire, { once: true });
    const host = currentPluginHost(context.host, viewLifetime.signal, activationId);
    let failed = false;
    reportFailure.current = (failure): void => {
      if (failed || lifetime?.aborted === true) return;
      failed = true;
      retire();
      const message = failure instanceof Error ? failure.message : "Plugin view failed to open.";
      setError(message);
      if (activationId !== undefined) {
        void context.host
          .execute({
            command: "plugins.renderer.failed",
            id: globalThis.crypto.randomUUID(),
            payload: { pluginId: renderer.id, activationId, error: message },
            version: HOST_PROTOCOL_VERSION,
          })
          .catch(() => undefined);
      }
    };
    const guardedContext = (): PluginViewContext => ({
      ...latestContext.current,
      host,
      onClose: (): void => {
        if (!viewLifetime.signal.aborted) latestContext.current.onClose();
      },
      onProfileReady: (profileId): void => {
        if (!viewLifetime.signal.aborted) latestContext.current.onProfileReady(profileId);
      },
      onExistingDestination: (destination): void => {
        if (!viewLifetime.signal.aborted) latestContext.current.onExistingDestination(destination);
      },
    });
    updateContext.current = guardedContext;
    setError(undefined);
    try {
      mounted.current = renderer.mount(mountTarget, guardedContext());
    } catch (failure) {
      reportFailure.current(failure);
    }
    return (): void => {
      retire();
      lifetime?.removeEventListener("abort", retire);
      updateContext.current = undefined;
      reportFailure.current = undefined;
      const instance = mounted.current;
      mounted.current = undefined;
      mountTarget.remove();
      // Separate React roots cannot be unmounted during their parent's commit.
      queueMicrotask(() => {
        try {
          instance?.dispose();
        } catch {
          // The view is detached; plugin cleanup must not interrupt the remaining workspace.
        }
      });
    };
  }, [renderer, context.host, lifetime, activationId]);

  useEffect(() => {
    try {
      if (updateContext.current !== undefined) mounted.current?.update(updateContext.current());
    } catch (failure) {
      reportFailure.current?.(failure);
    }
  }, [context, themeMode]);

  return (
    <>
      {error === undefined ? null : <StudioAlert severity="error">{error}</StudioAlert>}
      <div ref={container} />
    </>
  );
}
