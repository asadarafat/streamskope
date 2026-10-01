import { expect, it, vi } from "vitest";

import { EdaCaptureBackend } from "../../plugins/eda/backend";
import type { ProfileEdaCaptureSource } from "../../plugins/eda/contracts";
import { edaBackendHost, edaCapturePort } from "../support/eda-backend";

it.each(["keep", "cancel", "cleanup", "failed-cleanup"] as const)(
  "handles plugin exit choice %s without losing cleanup evidence",
  async (choice) => {
    const source: ProfileEdaCaptureSource = {
      kind: "eda-capture",
      state: "ready",
      broker: "127.0.0.1:19092",
      clusterBroker: "capture:9092",
      exporterName: "streamskope-capture",
      workloadName: "streamskope-redpanda",
      topics: [],
      source: {
        apiVersion: "kafka.eda.nokia.com/v1",
        kind: "Producer",
        namespace: "eda-system",
        name: "original",
      },
    };
    const order: string[] = [];
    const host = edaBackendHost();
    const disconnectOwnedConnection = vi.fn(() => Promise.resolve());
    const backend = new EdaCaptureBackend(
      { ...host, disconnectOwnedConnection },
      edaCapturePort({
        status: () => ({ state: "failed", tunnel: "closed", source, detail: "Cleanup pending" }),
        stop: () => {
          order.push("cleanup");
          return choice === "failed-cleanup"
            ? Promise.reject(new Error("fixture denial"))
            : Promise.resolve();
        },
      }),
    );
    expect(await backend.beforeExit()).toMatchObject({
      title: "Temporary EDA capture",
      cancelAction: "cancel",
    });
    if (choice === "failed-cleanup")
      await expect(backend.resolveExit("cleanup")).rejects.toThrow(
        /cleanup could not be confirmed/u,
      );
    else await expect(backend.resolveExit(choice)).resolves.toBe(choice !== "cancel");
    expect(disconnectOwnedConnection).toHaveBeenCalledTimes(
      choice === "cleanup" || choice === "failed-cleanup" ? 1 : 0,
    );
    expect(order).toEqual(choice === "cleanup" || choice === "failed-cleanup" ? ["cleanup"] : []);
    if (choice === "failed-cleanup")
      expect(await backend.beforeExit()).toMatchObject({ title: "Temporary EDA capture" });
  },
);
