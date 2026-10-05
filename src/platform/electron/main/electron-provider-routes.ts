import { ipcMain, type BrowserWindow } from "electron";

import type { ProviderHostRegistry, ProviderWireEndpoint } from "../../node/provider-host";
import { providerIpcChannels } from "../preload/channels";

import {
  ElectronControlEventDelivery,
  type ElectronProviderDelivery,
  type ElectronProviderDeliveryBinding,
} from "./provider-delivery";

function assertOwnedSender(sender: unknown, window: BrowserWindow): void {
  if (sender !== window.webContents) {
    throw new Error("Privileged requests must originate from the StreamSkope renderer.");
  }
}

/** Each registered provider owns independent admission and acknowledged delivery cleanup. */
export async function attachElectronProviders(
  window: BrowserWindow,
  registry: ProviderHostRegistry,
  bindings: readonly ElectronProviderDeliveryBinding[],
): Promise<() => Promise<void>> {
  const bindingById = new Map<string, ElectronProviderDeliveryBinding>();
  for (const binding of bindings) {
    if (bindingById.has(binding.id) || registry.get(binding.id) === undefined) {
      throw new Error("Desktop delivery bindings must name distinct registered providers.");
    }
    bindingById.set(binding.id, binding);
  }
  const registered = new Set<string>();
  const owners: Array<{ id: string; close(): Promise<void> }> = [];
  let closed = false;
  let closePromise: Promise<void> | undefined;
  const close = (): Promise<void> => {
    if (closePromise !== undefined) return closePromise;
    closed = true;
    let complete = (): void => undefined;
    let reject = (_error: unknown): void => undefined;
    closePromise = new Promise<void>((resolve, fail) => {
      complete = resolve;
      reject = fail;
    });
    const failures: Error[] = [];
    for (const channel of registered) {
      try {
        ipcMain.removeHandler(channel);
      } catch (cause) {
        failures.push(new Error("Desktop provider handler removal failed.", { cause }));
      }
    }
    registered.clear();
    const attempts = owners.map((owner): Promise<void> => {
      try {
        return owner.close();
      } catch (cause) {
        return Promise.reject(cause);
      }
    });
    void Promise.allSettled(attempts).then((results) => {
      for (const [index, result] of results.entries()) {
        if (result.status === "rejected") {
          failures.push(
            new Error(`Desktop ${owners[index]!.id} event cleanup failed.`, {
              cause: result.reason as unknown,
            }),
          );
        }
      }
      if (failures.length > 0) {
        reject(new AggregateError(failures, "Desktop provider cleanup did not complete."));
      } else complete();
    });
    return closePromise;
  };
  const attach = (endpoint: ProviderWireEndpoint): void => {
    const channels = providerIpcChannels(endpoint.id);
    const binding = bindingById.get(endpoint.id);
    let delivery: ElectronProviderDelivery | undefined;
    let unsubscribe: (() => void) | undefined;
    let lastSequence = 0;
    let generation = 0;
    let recovery: Promise<void> | undefined;
    let recoveryState: "pending" | "confirmed" | "failed" | undefined;
    let recoveryFailures: Error[] = [];
    const assertAdmission = (): void => {
      if (closed) throw new Error("Desktop provider routes are closed.");
      if (recoveryState === "pending") {
        throw new Error(
          "Desktop provider cleanup is pending. Wait for cleanup before reconnecting.",
        );
      }
      if (recoveryState === "failed") {
        throw new Error(
          "Desktop provider cleanup could not be confirmed. Restart StreamSkope before reconnecting.",
        );
      }
    };
    const release = (): Error[] => {
      generation += 1;
      const stop = unsubscribe;
      const currentDelivery = delivery;
      unsubscribe = undefined;
      delivery = undefined;
      const failures: Error[] = [];
      try {
        stop?.();
      } catch (cause) {
        failures.push(new Error("Desktop provider subscription cleanup failed.", { cause }));
      }
      try {
        currentDelivery?.close();
      } catch (cause) {
        failures.push(new Error("Desktop provider delivery cleanup failed.", { cause }));
      }
      return failures;
    };
    const send = (event: Parameters<ElectronProviderDelivery["enqueue"]>[0]): void => {
      if (!window.isDestroyed()) window.webContents.send(channels.event, event);
    };
    const startRecovery = (reason: string, failures: Error[] = []): Promise<void> => {
      if (recovery !== undefined) {
        recoveryFailures.push(...failures);
        return recovery;
      }
      let complete = (): void => undefined;
      let reject = (_error: unknown): void => undefined;
      recovery = new Promise<void>((resolve, fail) => {
        complete = resolve;
        reject = fail;
      });
      recoveryState = "pending";
      // Cleanup admission and Promise ownership precede arbitrary subscription/stop callbacks.
      recoveryFailures = failures;
      recoveryFailures.push(...release());
      const current = generation;
      const operation = recovery;
      void operation.then(
        () => undefined,
        () => undefined,
      );
      const report = (confirmed: boolean): void => {
        if (closed || window.isDestroyed() || generation !== current || recovery !== operation)
          return;
        try {
          const fallback = confirmed
            ? "Desktop provider event delivery failed. The active stream stopped. Reload the workbench to reconnect."
            : "Desktop provider cleanup could not be confirmed. Restart StreamSkope before reconnecting.";
          const instruction = binding?.recoveryInstruction?.(confirmed, reason) ?? fallback;
          send(endpoint.availability(++lastSequence, "unavailable", instruction));
        } catch {
          // A closed or failed transport cannot carry recovery advice.
        }
      };
      const finish = (failed: boolean, cause?: unknown): void => {
        if (failed) {
          recoveryFailures.push(new Error("Desktop provider stream cleanup failed.", { cause }));
        }
        if (recoveryFailures.length > 0) {
          recoveryState = "failed";
          report(false);
          reject(new AggregateError(recoveryFailures, "Desktop provider cleanup failed."));
        } else {
          recoveryState = "confirmed";
          report(true);
          complete();
        }
      };
      try {
        void endpoint.stopStream().then(
          () => finish(false),
          (cause: unknown) => finish(true, cause),
        );
      } catch (cause) {
        // Keep synchronous stop failure settlement asynchronous so a returned disposer joins it.
        void Promise.resolve().then(() => finish(true, cause));
      }
      return operation;
    };
    owners.push({ id: endpoint.id, close: (): Promise<void> => startRecovery("closed") });
    const subscribe = (): void => {
      assertAdmission();
      // A confirmed failure recovery permits a fresh stream; ordinary re-subscribe is local only.
      recovery = undefined;
      recoveryState = undefined;
      const failures = release();
      if (failures.length > 0) {
        startRecovery("event-cleanup", failures);
        assertAdmission();
      }
      const current = generation;
      const fail = (reason: string): void => {
        if (closed || generation !== current || recovery !== undefined) return;
        startRecovery(reason);
      };
      const currentDelivery =
        binding?.create(send, fail) ?? new ElectronControlEventDelivery(send, fail);
      delivery = currentDelivery;
      let stop: () => void;
      try {
        stop = endpoint.subscribe((wire) => {
          if (closed || recovery !== undefined || generation !== current) return;
          try {
            const event = endpoint.parseEvent(wire);
            lastSequence = Math.max(lastSequence, event.sequence);
            currentDelivery.enqueue(event);
          } catch {
            fail("event-validation");
          }
        });
      } catch (cause) {
        startRecovery("event-subscription", [
          new Error("Desktop provider subscription failed.", { cause }),
        ]);
        throw new Error("Desktop provider subscription failed.", { cause });
      }
      if (generation !== current || recovery !== undefined || closed) {
        try {
          stop();
        } catch (cause) {
          recoveryFailures.push(
            new Error("Desktop provider subscription cleanup failed.", { cause }),
          );
        }
      } else unsubscribe = stop;
    };
    ipcMain.handle(channels.command, async (event, value) => {
      assertOwnedSender(event.sender, window);
      assertAdmission();
      return endpoint.dispatch(value);
    });
    registered.add(channels.command);
    ipcMain.handle(channels.acknowledge, (event, sequence: unknown) => {
      assertOwnedSender(event.sender, window);
      if (typeof sequence !== "number" || !Number.isSafeInteger(sequence) || sequence < 0) {
        throw new Error("Invalid desktop event acknowledgement.");
      }
      delivery?.acknowledge(sequence);
    });
    registered.add(channels.acknowledge);
    ipcMain.handle(channels.subscribe, (event, version) => {
      assertOwnedSender(event.sender, window);
      if (version !== endpoint.version)
        throw new Error("Unsupported desktop host subscription version.");
      subscribe();
      return endpoint.version;
    });
    registered.add(channels.subscribe);
    subscribe();
  };
  try {
    for (const endpoint of registry.endpoints()) attach(endpoint);
  } catch (error) {
    try {
      await close();
    } catch (cleanupCause) {
      throw new AggregateError([error, cleanupCause], "Desktop provider startup cleanup failed.");
    }
    throw error;
  }
  return close;
}
