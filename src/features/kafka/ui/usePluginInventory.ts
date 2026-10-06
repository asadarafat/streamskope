import { useCallback, useEffect, useRef, useState } from "react";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommandResultMap,
  type StreamSkopeHost,
} from "../contracts";
import type { PluginCatalogSnapshot, PluginSnapshot } from "../../../plugins/contracts";

export type PluginDelivery = HostCommandResultMap["plugins.delivery"]["pluginDelivery"];

export function pluginFailureMessage(error: unknown): string {
  return error instanceof Error ? error.message : "The plugin operation could not be completed.";
}

export interface PluginInventory {
  readonly snapshot: PluginSnapshot;
  readonly catalog: PluginCatalogSnapshot;
  readonly delivery: PluginDelivery | undefined;
  readonly deliveryFailure: string | undefined;
  readonly installedLoading: boolean;
  readonly catalogLoading: boolean;
  readonly deliveryLoading: boolean;
  readonly applySnapshot: (next: PluginSnapshot) => void;
  readonly refreshInstalled: () => Promise<void>;
  readonly refreshCatalog: () => Promise<void>;
  readonly refreshDelivery: () => Promise<void>;
}

/** Installed state and local delivery are never held behind the optional remote lookup. */
export function usePluginInventory(host: StreamSkopeHost): PluginInventory {
  const [snapshot, setSnapshot] = useState<PluginSnapshot>({ revision: 0, plugins: [] });
  const [catalog, setCatalog] = useState<PluginCatalogSnapshot>({ plugins: [] });
  const [delivery, setDelivery] = useState<PluginDelivery>();
  const [deliveryFailure, setDeliveryFailure] = useState<string>();
  const [installedLoading, setInstalledLoading] = useState(true);
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [deliveryLoading, setDeliveryLoading] = useState(true);
  const installedRequest = useRef(0);
  const catalogRequest = useRef(0);
  const deliveryRequest = useRef(0);
  const applySnapshot = useCallback((next: PluginSnapshot): void => {
    setSnapshot((current) => (next.revision >= current.revision ? next : current));
  }, []);
  const refreshInstalled = useCallback(async (): Promise<void> => {
    const request = ++installedRequest.current;
    setInstalledLoading(true);
    try {
      const response = await host.execute({
        command: "plugins.list",
        id: crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (request !== installedRequest.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      applySnapshot(response.result.pluginSnapshot);
    } catch (error) {
      if (request === installedRequest.current)
        setSnapshot((current) => ({ ...current, error: pluginFailureMessage(error) }));
    } finally {
      if (request === installedRequest.current) setInstalledLoading(false);
    }
  }, [host, applySnapshot]);
  const refreshCatalog = useCallback(async (): Promise<void> => {
    const request = ++catalogRequest.current;
    setCatalogLoading(true);
    for (const refresh of [false, true]) {
      try {
        const response = await host.execute({
          command: "plugins.catalog",
          id: crypto.randomUUID(),
          payload: { refresh },
          version: HOST_PROTOCOL_VERSION,
        });
        if (request !== catalogRequest.current) return;
        if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
        const next = response.result.pluginCatalog;
        setCatalog((current) =>
          next.error !== undefined &&
          next.source !== "live" &&
          next.plugins.length === 0 &&
          current.plugins.length > 0
            ? { ...current, source: "cache", error: next.error }
            : next,
        );
      } catch (error) {
        if (request !== catalogRequest.current) return;
        if (refresh)
          setCatalog((current) => ({
            ...current,
            source: current.checkedAt === undefined ? "unavailable" : "cache",
            error: pluginFailureMessage(error),
          }));
      }
    }
    if (request === catalogRequest.current) setCatalogLoading(false);
  }, [host]);
  const refreshDelivery = useCallback(async (): Promise<void> => {
    const request = ++deliveryRequest.current;
    setDeliveryLoading(true);
    try {
      const response = await host.execute({
        command: "plugins.delivery",
        id: crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (request !== deliveryRequest.current) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      setDelivery(response.result.pluginDelivery);
      setDeliveryFailure(undefined);
    } catch (error) {
      if (request === deliveryRequest.current) setDeliveryFailure(pluginFailureMessage(error));
    } finally {
      if (request === deliveryRequest.current) setDeliveryLoading(false);
    }
  }, [host]);
  useEffect(() => {
    const unsubscribe = host.subscribe((event) => {
      if (event.event === "plugins.changed") applySnapshot(event.payload);
    });
    void refreshInstalled();
    void refreshCatalog();
    void refreshDelivery();
    return (): void => {
      unsubscribe();
      installedRequest.current++;
      catalogRequest.current++;
      deliveryRequest.current++;
    };
  }, [host, applySnapshot, refreshInstalled, refreshCatalog, refreshDelivery]);
  return {
    snapshot,
    catalog,
    delivery,
    deliveryFailure,
    installedLoading,
    catalogLoading,
    deliveryLoading,
    applySnapshot,
    refreshInstalled,
    refreshCatalog,
    refreshDelivery,
  };
}
