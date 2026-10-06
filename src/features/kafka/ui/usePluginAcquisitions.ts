import { useCallback, useEffect, useRef, useState } from "react";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type KafkaHostExecute,
  type StreamSkopeHost,
} from "../contracts";
import type { PluginAcquisitionProgress } from "../../../plugins/contracts";

export class PluginAcquisitionCancelled extends Error {
  constructor() {
    super("Plugin acquisition was cancelled.");
  }
}

export interface PluginAcquisitions {
  readonly progress: readonly PluginAcquisitionProgress[];
  readonly execute: KafkaHostExecute;
  readonly cancel: (requestId: string) => Promise<void>;
}

function operation(command: HostCommand): PluginAcquisitionProgress["operation"] | undefined {
  if (command.command === "plugins.catalog" && command.payload.refresh !== false) return "catalog";
  if (command.command === "plugins.package.inspect") return "inspect";
  if (command.command === "plugins.network.test") return "test";
  return undefined;
}

/** Command IDs own progress and cancellation; unrelated local lifecycle work stays independent. */
export function usePluginAcquisitions(host: StreamSkopeHost): PluginAcquisitions {
  const [progress, setProgress] = useState<readonly PluginAcquisitionProgress[]>([]);
  const active = useRef(false);
  const requests = useRef(new Map<string, { cancelled: boolean }>());
  const publish = useCallback((next: PluginAcquisitionProgress): void => {
    if (!active.current) return;
    setProgress((current) => [
      ...current.filter(
        (entry) =>
          entry.requestId !== next.requestId &&
          (entry.operation !== next.operation || entry.state === "running"),
      ),
      next,
    ]);
  }, []);
  const cancelHost = useCallback(
    async (requestId: string): Promise<void> => {
      const response = await host.execute({
        command: "plugins.network.cancel",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { requestId },
      });
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
    },
    [host],
  );
  const cancel = useCallback(
    async (requestId: string): Promise<void> => {
      const request = requests.current.get(requestId);
      if (request === undefined || request.cancelled) return;
      request.cancelled = true;
      setProgress((current) =>
        current.map((entry) =>
          entry.requestId === requestId ? { ...entry, state: "cancelled" } : entry,
        ),
      );
      await cancelHost(requestId);
    },
    [cancelHost],
  );
  const execute = useCallback(
    async <Command extends HostCommand>(
      command: Command,
    ): Promise<HostCommandResponse<Command["command"]>> => {
      const kind = operation(command);
      if (kind === undefined) return host.execute(command);
      const request = { cancelled: false };
      requests.current.set(command.id, request);
      publish({
        requestId: command.id,
        operation: kind,
        phase: kind === "inspect" ? "verify" : "catalog",
        state: "running",
      });
      try {
        const response = await host.execute(command);
        if (request.cancelled || !active.current) {
          // A host that completed while cancellation was in flight may return a
          // valid review receipt. Release it without ever applying it to the UI.
          if (
            response.ok &&
            response.command === "plugins.package.inspect" &&
            "pluginPackage" in response.result
          ) {
            const pluginPackage = response.result.pluginPackage;
            if (
              pluginPackage !== null &&
              typeof pluginPackage === "object" &&
              "candidateId" in pluginPackage &&
              typeof pluginPackage.candidateId === "string"
            )
              await host.execute({
                command: "plugins.package.discard",
                id: crypto.randomUUID(),
                version: HOST_PROTOCOL_VERSION,
                payload: { candidateId: pluginPackage.candidateId },
              });
          }
          throw new PluginAcquisitionCancelled();
        }
        setProgress((current) =>
          current.map((entry) =>
            entry.requestId === command.id && entry.state === "running"
              ? { ...entry, state: response.ok ? "succeeded" : "failed" }
              : entry,
          ),
        );
        return response;
      } catch (error) {
        if (!request.cancelled && active.current)
          setProgress((current) =>
            current.map((entry) =>
              entry.requestId === command.id && entry.state === "running"
                ? { ...entry, state: "failed" }
                : entry,
            ),
          );
        if (request.cancelled) throw new PluginAcquisitionCancelled();
        throw error;
      } finally {
        requests.current.delete(command.id);
      }
    },
    [host, publish],
  );
  useEffect(() => {
    active.current = true;
    const unsubscribe = host.subscribe((event) => {
      if (event.event !== "plugins.network.progress") return;
      const request = requests.current.get(event.payload.requestId);
      if (request === undefined || request.cancelled) return;
      if (event.payload.state === "cancelled") request.cancelled = true;
      publish(event.payload);
    });
    return (): void => {
      active.current = false;
      unsubscribe();
      for (const [id, request] of requests.current) {
        request.cancelled = true;
        void cancelHost(id).catch((): void => undefined);
      }
    };
  }, [host, cancelHost, publish]);
  return { progress, execute, cancel };
}
