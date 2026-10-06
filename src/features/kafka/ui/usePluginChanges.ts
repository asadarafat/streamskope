import { useCallback, useEffect, useRef, useState } from "react";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResultMap,
  type StreamSkopeHost,
} from "../contracts";

import type { PluginChangePrompt, PluginManifest } from "../../../plugins/contracts";
import { pluginFailureMessage, type PluginInventory } from "./usePluginInventory";

export type PluginPackageReview = NonNullable<
  HostCommandResultMap["plugins.package.inspect"]["pluginPackage"]
>;
export type PluginInspectionInput = Extract<
  HostCommand,
  { command: "plugins.package.inspect" }
>["payload"];
export type PluginLocalCommand = "plugins.retry" | "plugins.remove";
export type PluginActionTarget = Pick<PluginManifest, "id" | "name"> & {
  readonly version?: string;
};
export interface PluginLocalConfirmation {
  readonly command: PluginLocalCommand;
  readonly plugin: PluginActionTarget;
  readonly prompt: PluginChangePrompt | null;
}
export interface PluginChanges {
  readonly pending: string | undefined;
  readonly failure: string | undefined;
  readonly status: string;
  readonly completed: { readonly id: string; readonly version: string | undefined } | undefined;
  readonly confirmation: PluginLocalConfirmation | undefined;
  readonly review: PluginPackageReview | undefined;
  readonly reviewPrompt: PluginChangePrompt | undefined;
  readonly prepareLocal: (command: PluginLocalCommand, plugin: PluginActionTarget) => Promise<void>;
  readonly confirmLocal: () => Promise<void>;
  readonly cancelLocal: () => void;
  readonly inspect: (input: PluginInspectionInput) => Promise<void>;
  readonly applyReview: () => Promise<void>;
  readonly closeReview: () => Promise<void>;
}

/** UI keeps an opaque host review receipt; selected files and executable bytes stay in the host. */
export function usePluginChanges(
  host: StreamSkopeHost,
  inventory: PluginInventory,
  refreshRenderers: () => Promise<void>,
): PluginChanges {
  const [pending, setPending] = useState<string>();
  const [failure, setFailure] = useState<string>();
  const [status, setStatus] = useState("");
  const [completed, setCompleted] = useState<PluginChanges["completed"]>();
  const [confirmation, setConfirmation] = useState<PluginLocalConfirmation>();
  const [review, setReview] = useState<PluginPackageReview>();
  const [reviewPrompt, setReviewPrompt] = useState<PluginChangePrompt>();
  const lifetime = useRef({
    active: false,
    operation: 0,
    candidateId: undefined as string | undefined,
  });
  const discard = useCallback(
    async (candidateId: string): Promise<void> => {
      const response = await host.execute({
        command: "plugins.package.discard",
        id: crypto.randomUUID(),
        payload: { candidateId },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
    },
    [host],
  );
  useEffect(() => {
    lifetime.current.active = true;
    return (): void => {
      lifetime.current.active = false;
      lifetime.current.operation++;
      const candidateId = lifetime.current.candidateId;
      lifetime.current.candidateId = undefined;
      if (candidateId !== undefined) void discard(candidateId).catch((): void => undefined);
    };
  }, [host, discard]);
  const current = (operation: number): boolean =>
    lifetime.current.active && operation === lifetime.current.operation;
  function begin(id: string, message: string): number {
    const operation = ++lifetime.current.operation;
    setPending(id);
    setFailure(undefined);
    setCompleted(undefined);
    setStatus(message);
    return operation;
  }
  async function executeLocal(
    command: PluginLocalCommand,
    plugin: PluginActionTarget,
    operation: number,
    token?: string,
  ): Promise<void> {
    setStatus(
      command === "plugins.retry"
        ? `Verifying the installed ${plugin.name} package and reloading…`
        : `Removing ${plugin.name}…`,
    );
    try {
      const response = await host.execute({
        command,
        id: crypto.randomUUID(),
        payload: {
          pluginId: plugin.id,
          ...(token === undefined ? {} : { confirmationToken: token }),
        },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!current(operation)) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      inventory.applySnapshot(response.result.pluginSnapshot);
      setConfirmation(undefined);
      if (command === "plugins.retry") await refreshRenderers();
      if (!current(operation)) return;
      if (command === "plugins.retry") setCompleted({ id: plugin.id, version: plugin.version });
      setStatus(
        command === "plugins.retry"
          ? `${plugin.name} ${plugin.version ?? ""} is active.`
          : `${plugin.name} has been removed. Saved connection settings are retained.`,
      );
      void inventory.refreshDelivery();
    } catch (error) {
      if (!current(operation)) return;
      setConfirmation(undefined);
      setFailure(pluginFailureMessage(error));
      setStatus("");
      await inventory.refreshInstalled();
    }
  }
  async function prepareLocal(
    command: PluginLocalCommand,
    plugin: PluginActionTarget,
  ): Promise<void> {
    const operation = begin(plugin.id, `Checking ${plugin.name}…`);
    try {
      const response = await host.execute({
        command: "plugins.change.prepare",
        id: crypto.randomUUID(),
        payload: {
          pluginId: plugin.id,
          operation: command === "plugins.retry" ? "retry" : "remove",
        },
        version: HOST_PROTOCOL_VERSION,
      });
      if (!current(operation)) return;
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      const prompt = response.result.pluginChange;
      if (prompt !== null || command === "plugins.remove") {
        setConfirmation({ command, plugin, prompt });
        setStatus("");
      } else await executeLocal(command, plugin, operation);
    } catch (error) {
      if (current(operation)) {
        setFailure(pluginFailureMessage(error));
        setStatus("");
      }
    } finally {
      if (current(operation)) setPending(undefined);
    }
  }
  async function confirmLocal(): Promise<void> {
    if (confirmation === undefined) return;
    const operation = begin(confirmation.plugin.id, "Applying plugin change…");
    try {
      await executeLocal(
        confirmation.command,
        confirmation.plugin,
        operation,
        confirmation.prompt?.token,
      );
    } finally {
      if (current(operation)) setPending(undefined);
    }
  }
  async function inspect(input: PluginInspectionInput): Promise<void> {
    const operation = begin(
      input.source === "file" ? "file" : input.pluginId,
      input.source === "file"
        ? "Choose a signed plugin package…"
        : "Retrieving and verifying the selected plugin package…",
    );
    try {
      const previous = lifetime.current.candidateId;
      if (previous !== undefined) await discard(previous);
      if (!current(operation)) return;
      lifetime.current.candidateId = undefined;
      setReview(undefined);
      setReviewPrompt(undefined);
      if (!current(operation)) return;
      const response = await host.execute({
        command: "plugins.package.inspect",
        id: crypto.randomUUID(),
        payload: input,
        version: HOST_PROTOCOL_VERSION,
      });
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      const candidate = response.result.pluginPackage;
      if (!current(operation)) {
        if (candidate !== null) await discard(candidate.candidateId);
        return;
      }
      setStatus("");
      if (candidate !== null) {
        lifetime.current.candidateId = candidate.candidateId;
        setReview(candidate);
        void inventory.refreshDelivery();
      }
    } catch (error) {
      if (current(operation)) {
        setFailure(pluginFailureMessage(error));
        setStatus("");
      }
    } finally {
      if (current(operation)) setPending(undefined);
    }
  }
  async function closeReview(): Promise<void> {
    const candidateId = lifetime.current.candidateId;
    lifetime.current.candidateId = undefined;
    lifetime.current.operation++;
    setReview(undefined);
    setReviewPrompt(undefined);
    setPending(undefined);
    setStatus("");
    if (candidateId !== undefined) {
      try {
        await discard(candidateId);
      } catch (error) {
        if (lifetime.current.active) setFailure(pluginFailureMessage(error));
      }
    }
  }
  async function installReviewed(
    candidate: PluginPackageReview,
    operation: number,
    token?: string,
  ): Promise<void> {
    const response = await host.execute({
      command: "plugins.package.install",
      id: crypto.randomUUID(),
      payload: {
        candidateId: candidate.candidateId,
        ...(token === undefined ? {} : { confirmationToken: token }),
      },
      version: HOST_PROTOCOL_VERSION,
    });
    if (!current(operation)) return;
    if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
    inventory.applySnapshot(response.result.pluginSnapshot);
    lifetime.current.candidateId = undefined;
    setReview(undefined);
    setReviewPrompt(undefined);
    await refreshRenderers();
    if (!current(operation)) return;
    setCompleted({ id: candidate.manifest.id, version: candidate.manifest.version });
    setStatus(`${candidate.manifest.name} ${candidate.manifest.version} is installed.`);
    void inventory.refreshDelivery();
  }
  async function applyReview(): Promise<void> {
    if (
      review === undefined ||
      review.status === "blocked" ||
      review.status === "already-installed"
    )
      return;
    const operation = begin(review.manifest.id, `Installing verified ${review.manifest.name}…`);
    try {
      if (reviewPrompt !== undefined) await installReviewed(review, operation, reviewPrompt.token);
      else {
        const response = await host.execute({
          command: "plugins.package.change.prepare",
          id: crypto.randomUUID(),
          payload: { candidateId: review.candidateId },
          version: HOST_PROTOCOL_VERSION,
        });
        if (!current(operation)) return;
        if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
        if (response.result.pluginChange === null) await installReviewed(review, operation);
        else {
          setReviewPrompt(response.result.pluginChange);
          setStatus("");
        }
      }
    } catch (error) {
      if (!current(operation)) return;
      setFailure(pluginFailureMessage(error));
      setStatus("");
      const candidateId = lifetime.current.candidateId;
      lifetime.current.candidateId = undefined;
      setReview(undefined);
      setReviewPrompt(undefined);
      if (candidateId !== undefined) await discard(candidateId).catch((): void => undefined);
      await inventory.refreshInstalled();
    } finally {
      if (current(operation)) setPending(undefined);
    }
  }
  useEffect(() => {
    if (review === undefined || review.status === "blocked" || pending !== undefined) return;
    const timer = setTimeout(
      () => {
        setReview((candidate) =>
          candidate?.candidateId === review.candidateId
            ? {
                ...candidate,
                status: "blocked",
                reason: "This review expired. Inspect the package again before installing.",
              }
            : candidate,
        );
        setReviewPrompt(undefined);
        void discard(review.candidateId).catch((): void => undefined);
      },
      Math.max(0, Math.min(2_147_483_647, Date.parse(review.expiresAt) - Date.now())),
    );
    return (): void => clearTimeout(timer);
  }, [review, pending, discard]);
  return {
    pending,
    failure,
    status,
    completed,
    confirmation,
    review,
    reviewPrompt,
    prepareLocal,
    confirmLocal,
    cancelLocal: (): void => setConfirmation(undefined),
    inspect,
    applyReview,
    closeReview,
  };
}
