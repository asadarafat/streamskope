import { randomUUID } from "node:crypto";

import type { PluginAcquisitionProgress } from "../../../plugins/contracts";

import { MAX_PLUGIN_ARCHIVE_BYTES } from "./package";
import { pluginNetworkProblem } from "./network-errors";
import { pluginProblem } from "./problem";

export interface PluginAcquisitionContext {
  readonly requestId: string;
  readonly signal: AbortSignal;
  readonly assertCurrent: () => void;
  readonly progress: (
    phase: PluginAcquisitionProgress["phase"],
    receivedBytes?: number,
    totalBytes?: number,
  ) => void;
}
interface Owner {
  readonly controller: AbortController;
  readonly remote: boolean;
  readonly operation: PluginAcquisitionProgress["operation"];
  phase: PluginAcquisitionProgress["phase"];
  lastEmitted: number;
  receivedBytes?: number;
  totalBytes?: number;
}
const MAX_OWNERS = 16;
const REMOTE_DEADLINE_MS = 60_000;

function ownedResult<T>(
  task: Promise<T>,
  signal: AbortSignal,
  discardLate?: (result: T) => void,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const abort = (): void => {
      if (settled) return;
      settled = true;
      reject(pluginNetworkProblem(signal.reason, signal));
    };
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    void task.then(
      (result) => {
        signal.removeEventListener("abort", abort);
        if (settled) {
          if (signal.aborted) discardLate?.(result);
          return;
        }
        settled = true;
        resolve(result);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", abort);
        if (settled) return;
        settled = true;
        reject(pluginNetworkProblem(error));
      },
    );
  });
}

/** Cancellation belongs to one acquisition command and never owns plugin installation or cleanup. */
export class PluginAcquisitions {
  private readonly owners = new Map<string, Owner>();
  private readonly listeners = new Set<(value: PluginAcquisitionProgress) => void>();
  private closed = false;
  subscribe(listener: (value: PluginAcquisitionProgress) => void): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
  private emit(requestId: string, owner: Owner, state: PluginAcquisitionProgress["state"]): void {
    const value = {
      requestId,
      operation: owner.operation,
      phase: owner.phase,
      state,
      ...(owner.receivedBytes === undefined ? {} : { receivedBytes: owner.receivedBytes }),
      ...(owner.totalBytes === undefined ? {} : { totalBytes: owner.totalBytes }),
    };
    for (const listener of this.listeners) {
      try {
        listener(value);
      } catch {
        /* Observers cannot change acquisition authority. */
      }
    }
  }
  cancel(requestId: string): void {
    this.owners
      .get(requestId)
      ?.controller.abort(new DOMException("Plugin acquisition cancelled.", "AbortError"));
  }
  cancelRemote(): void {
    for (const [id, owner] of this.owners) if (owner.remote) this.cancel(id);
  }
  async run<T>(
    requestId: string | undefined,
    operation: PluginAcquisitionProgress["operation"],
    remote: boolean,
    task: (context: PluginAcquisitionContext) => Promise<T>,
    discardLate?: (result: T) => void,
  ): Promise<T> {
    const id = requestId ?? randomUUID();
    if (this.closed) throw pluginProblem("Plugin acquisition is closing.");
    if (id.length === 0 || id.length > 128 || /\p{Cc}/u.test(id))
      throw pluginProblem("Invalid plugin acquisition identifier.");
    if (this.owners.has(id))
      throw pluginProblem("This plugin acquisition identifier is already in use.");
    if (this.owners.size >= MAX_OWNERS)
      throw pluginProblem(
        "Too many plugin acquisitions are in progress.",
        "Cancel or complete a download and retry.",
      );
    const owner: Owner = {
      controller: new AbortController(),
      remote,
      operation,
      phase: operation === "inspect" ? "verify" : "catalog",
      lastEmitted: Date.now(),
    };
    this.owners.set(id, owner);
    this.emit(id, owner, "running");
    const timer = remote
      ? setTimeout(
          () =>
            owner.controller.abort(
              new DOMException("Plugin acquisition timed out.", "TimeoutError"),
            ),
          REMOTE_DEADLINE_MS,
        )
      : undefined;
    timer?.unref();
    const context: PluginAcquisitionContext = {
      requestId: id,
      signal: owner.controller.signal,
      assertCurrent: (): void => {
        owner.controller.signal.throwIfAborted();
        if (this.closed || this.owners.get(id) !== owner)
          throw pluginProblem("Plugin acquisition was superseded.");
      },
      progress: (phase, receivedBytes, totalBytes): void => {
        if (owner.controller.signal.aborted || this.owners.get(id) !== owner) return;
        const transition = owner.phase !== phase || receivedBytes === 0;
        owner.phase = phase;
        if (
          receivedBytes !== undefined &&
          Number.isSafeInteger(receivedBytes) &&
          receivedBytes >= 0 &&
          receivedBytes <= MAX_PLUGIN_ARCHIVE_BYTES
        )
          owner.receivedBytes = receivedBytes;
        else delete owner.receivedBytes;
        if (
          totalBytes !== undefined &&
          Number.isSafeInteger(totalBytes) &&
          totalBytes >= (owner.receivedBytes ?? 0) &&
          totalBytes <= MAX_PLUGIN_ARCHIVE_BYTES
        )
          owner.totalBytes = totalBytes;
        else delete owner.totalBytes;
        if (transition || Date.now() - owner.lastEmitted >= 250) {
          owner.lastEmitted = Date.now();
          this.emit(id, owner, "running");
        }
      },
    };
    let result: T | undefined;
    let completed = false;
    try {
      result = await ownedResult(
        Promise.resolve().then(() => task(context)),
        context.signal,
        discardLate,
      );
      completed = true;
      context.assertCurrent();
      this.emit(id, owner, "succeeded");
      return result;
    } catch (error) {
      if (completed) discardLate?.(result as T);
      this.emit(
        id,
        owner,
        owner.controller.signal.aborted &&
          !(
            owner.controller.signal.reason instanceof Error &&
            owner.controller.signal.reason.name === "TimeoutError"
          )
          ? "cancelled"
          : "failed",
      );
      throw remote
        ? pluginNetworkProblem(error, owner.controller.signal)
        : owner.controller.signal.aborted
          ? pluginProblem(
              "Plugin package selection was cancelled.",
              "Select and review the package again when ready.",
            )
          : error;
    } finally {
      clearTimeout(timer);
      if (this.owners.get(id) === owner) this.owners.delete(id);
    }
  }
  close(): void {
    this.closed = true;
    for (const id of this.owners.keys()) this.cancel(id);
    this.listeners.clear();
  }
}
