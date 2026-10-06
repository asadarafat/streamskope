import { useCallback, useEffect, useRef, useState } from "react";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type {
  PluginNetworkSnapshot,
  PluginNetworkTestResult,
  PluginNetworkUpdateInput,
} from "../../../plugins/contracts";

import { PluginAcquisitionCancelled, type PluginAcquisitions } from "./usePluginAcquisitions";
import { pluginFailureMessage } from "./usePluginInventory";

export interface PluginNetwork {
  readonly snapshot: PluginNetworkSnapshot | undefined;
  readonly loading: boolean;
  readonly saving: boolean;
  readonly testing: boolean;
  readonly failure: string | undefined;
  readonly testResult: PluginNetworkTestResult | undefined;
  readonly save: (input: PluginNetworkUpdateInput) => Promise<void>;
  readonly test: () => Promise<void>;
}

/** Network tests describe the applied revision, never an unsaved form or stale response. */
export function usePluginNetwork(
  host: StreamSkopeHost,
  acquisitions: PluginAcquisitions,
): PluginNetwork {
  const [snapshot, setSnapshot] = useState<PluginNetworkSnapshot>();
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [failure, setFailure] = useState<string>();
  const [testResult, setTestResult] = useState<PluginNetworkTestResult>();
  const current = useRef({ active: false, revision: -1, save: 0, test: 0 });
  const apply = useCallback((next: PluginNetworkSnapshot): void => {
    if (!current.current.active || next.revision < current.current.revision) return;
    if (next.revision !== current.current.revision) setTestResult(undefined);
    current.current.revision = next.revision;
    setSnapshot(next);
  }, []);
  useEffect(() => {
    current.current.active = true;
    setLoading(true);
    void host
      .execute({
        command: "plugins.network.get",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      })
      .then((response): void => {
        if (!current.current.active) return;
        if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
        apply(response.result.pluginNetwork);
      })
      .catch((error: unknown): void => {
        if (current.current.active) setFailure(pluginFailureMessage(error));
      })
      .finally((): void => {
        if (current.current.active) setLoading(false);
      });
    return (): void => {
      current.current.active = false;
      current.current.save++;
      current.current.test++;
    };
  }, [host, apply]);
  async function save(input: PluginNetworkUpdateInput): Promise<void> {
    const generation = ++current.current.save;
    current.current.test++;
    setSaving(true);
    setTesting(false);
    setFailure(undefined);
    setTestResult(undefined);
    for (const entry of acquisitions.progress)
      if (entry.operation === "test" && entry.state === "running")
        void acquisitions.cancel(entry.requestId).catch((): void => undefined);
    try {
      const response = await host.execute({
        command: "plugins.network.update",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: input,
      });
      if (!current.current.active || generation !== current.current.save) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      apply(response.result.pluginNetwork);
    } catch (error) {
      if (current.current.active && generation === current.current.save)
        setFailure(pluginFailureMessage(error));
    } finally {
      if (current.current.active && generation === current.current.save) setSaving(false);
    }
  }
  async function test(): Promise<void> {
    const generation = ++current.current.test;
    const revision = current.current.revision;
    setTesting(true);
    setFailure(undefined);
    setTestResult(undefined);
    try {
      const response = await acquisitions.execute({
        command: "plugins.network.test",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (
        !current.current.active ||
        generation !== current.current.test ||
        revision !== current.current.revision
      )
        return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      if (response.result.pluginNetworkTest.settingsRevision === revision)
        setTestResult(response.result.pluginNetworkTest);
    } catch (error) {
      if (
        current.current.active &&
        generation === current.current.test &&
        !(error instanceof PluginAcquisitionCancelled)
      )
        setFailure(pluginFailureMessage(error));
    } finally {
      if (current.current.active && generation === current.current.test) setTesting(false);
    }
  }
  return { snapshot, loading, saving, testing, failure, testResult, save, test };
}
