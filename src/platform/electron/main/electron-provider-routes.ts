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

/** Each registered provider owns independent handlers, sequence state and acknowledged delivery. */
export function attachElectronProviders(
  window: BrowserWindow,
  registry: ProviderHostRegistry,
  bindings: readonly ElectronProviderDeliveryBinding[],
): () => void {
  const bindingById = new Map<string, ElectronProviderDeliveryBinding>();
  for (const binding of bindings) {
    if (bindingById.has(binding.id) || registry.get(binding.id) === undefined) {
      throw new Error("Desktop delivery bindings must name distinct registered providers.");
    }
    bindingById.set(binding.id, binding);
  }
  const registered = new Set<string>();
  const owners: Array<{ id: string; close(): void }> = [];
  let closed = false;
  const close = (): void => {
    if (closed) return;
    closed = true;
    const failures: Error[] = [];
    for (const channel of registered) {
      try {
        ipcMain.removeHandler(channel);
      } catch {
        failures.push(new Error("Desktop provider handler removal failed."));
      }
    }
    registered.clear();
    for (const owner of owners) {
      try {
        owner.close();
      } catch {
        failures.push(new Error(`Desktop ${owner.id} event cleanup failed.`));
      }
    }
    if (failures.length)
      throw new AggregateError(failures, "Desktop provider routes did not close cleanly.");
  };
  const attach = (endpoint: ProviderWireEndpoint): void => {
    const channels = providerIpcChannels(endpoint.id);
    const binding = bindingById.get(endpoint.id);
    let delivery: ElectronProviderDelivery | undefined;
    let unsubscribe: (() => void) | undefined;
    let lastSequence = 0;
    let generation = 0;
    const release = (): void => {
      generation += 1;
      const stop = unsubscribe;
      unsubscribe = undefined;
      try {
        stop?.();
      } finally {
        delivery?.close();
      }
    };
    owners.push({ id: endpoint.id, close: release });
    const subscribe = (): void => {
      release();
      const current = generation;
      let failed = false;
      const send = (event: Parameters<ElectronProviderDelivery["enqueue"]>[0]): void => {
        if (!window.isDestroyed()) window.webContents.send(channels.event, event);
      };
      const fail = (reason: string): void => {
        if (failed || generation !== current) return;
        failed = true;
        let subscriptionClosed = true;
        try {
          const stop = unsubscribe;
          unsubscribe = undefined;
          stop?.();
        } catch {
          subscriptionClosed = false;
        }
        currentDelivery.close();
        let recovery: Promise<string>;
        try {
          recovery = subscriptionClosed
            ? (binding?.failureRecovery(reason) ??
              Promise.resolve(
                "Desktop provider event delivery failed. Reload the workbench or restart StreamSkope to reconnect.",
              ))
            : Promise.resolve(
                "Desktop provider event cleanup could not be confirmed. Restart StreamSkope to reconnect.",
              );
        } catch {
          recovery = Promise.resolve(
            "Desktop provider event cleanup could not be confirmed. Restart StreamSkope to reconnect.",
          );
        }
        const report = (instruction: string): void => {
          if (
            closed ||
            window.isDestroyed() ||
            delivery !== currentDelivery ||
            generation !== current
          )
            return;
          try {
            send(endpoint.availability(++lastSequence, "unavailable", instruction));
          } catch {
            // Delivery is already closed; an unavailable transport cannot carry recovery advice.
          }
        };
        void recovery.then(report, () =>
          report(
            "Desktop provider event cleanup could not be confirmed. Restart StreamSkope to reconnect.",
          ),
        );
      };
      const currentDelivery =
        binding?.create(send, fail) ?? new ElectronControlEventDelivery(send, fail);
      delivery = currentDelivery;
      unsubscribe = endpoint.subscribe((wire) => {
        if (failed || generation !== current) return;
        try {
          const event = endpoint.parseEvent(wire);
          lastSequence = Math.max(lastSequence, event.sequence);
          currentDelivery.enqueue(event);
        } catch {
          fail("event-validation");
        }
      });
      if (failed) {
        const stop = unsubscribe;
        unsubscribe = undefined;
        stop();
      }
    };
    ipcMain.handle(channels.command, async (event, value) => {
      assertOwnedSender(event.sender, window);
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
      close();
    } catch (cleanupCause) {
      throw new Error("Desktop provider startup cleanup failed.", { cause: cleanupCause });
    }
    throw error;
  }
  return close;
}
