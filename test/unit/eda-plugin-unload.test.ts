import { expect, it, vi } from "vitest";

import { EdaCaptureBackend } from "../../plugins/eda/backend";
import type {
  EdaCaptureApplicationStatus,
  EdaCaptureDeployment,
  EdaCaptureSessionStatus,
  ProfileEdaCaptureSource,
} from "../../plugins/eda/contracts";
import { parsePluginJson } from "../../src/plugins/validation";
import { edaBackendHost, edaCapturePort } from "../support/eda-backend";

const source: ProfileEdaCaptureSource = {
  kind: "eda-capture",
  state: "ready",
  sessionId: "owned-session",
  broker: "127.0.0.1:19092",
  clusterBroker: "capture:9092",
  exporterName: "copied-exporter",
  workloadName: "capture-broker",
  topics: [],
  source: {
    apiVersion: "kafka.eda.nokia.com/v1",
    kind: "Producer",
    namespace: "eda-system",
    name: "original-exporter",
  },
};
const edaApi = {
  baseUrl: "https://eda.example.test",
  username: "operator",
  password: "fixture-secret",
};
const statusRequest = {
  method: "edaCapture.status",
  input: {},
  requestId: "status",
  correlationId: "status",
};

it.each(["update", "remove"] as const)(
  "warns before %s and cleans up without deleting saved profiles or issuing global disconnect",
  async (reason) => {
    const order: string[] = [];
    const deleteProfile = vi.fn(() => Promise.resolve());
    const host = edaBackendHost({
      deleteProfile,
      disconnectOwnedConnection: () => {
        order.push("disconnect-owned");
        return Promise.resolve();
      },
    });
    const execute = vi.spyOn(host, "execute");
    const stop = vi.fn(() => {
      order.push("cleanup");
      return Promise.resolve();
    });
    const close = vi.fn(() => Promise.resolve());
    const backend = new EdaCaptureBackend(
      host,
      edaCapturePort({
        status: () => ({ state: "ready", tunnel: "open", source, detail: "Ready" }),
        stop,
        close,
      }),
    );
    const warning = await backend.beforeChange();
    expect(warning?.message).toBe("Stop EDA capture before changing the plugin?");
    expect(warning?.detail).toContain("Saved connection settings are retained");
    expect(order).toEqual([]);
    await backend.prepareUnload(reason);
    expect(order).toEqual(["disconnect-owned", "cleanup"]);
    expect(stop).toHaveBeenCalledWith(source, true);
    expect(deleteProfile).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
    await expect(backend.execute(statusRequest)).resolves.toMatchObject({ ok: true });
    await backend.close();
    expect(close).toHaveBeenCalledOnce();
  },
);

it("keeps a failed cleanup usable for status and retry with the same recovery source", async () => {
  const stop = vi
    .fn<() => Promise<void>>()
    .mockRejectedValueOnce(new Error("EDA unavailable"))
    .mockResolvedValue();
  const close = vi.fn(() => Promise.resolve());
  const backend = new EdaCaptureBackend(
    edaBackendHost(),
    edaCapturePort({
      status: () => ({ state: "ready", tunnel: "open", source, detail: "Ready" }),
      stop,
      close,
    }),
  );
  await expect(backend.prepareUnload("remove")).rejects.toThrow("plugin remains active");
  expect(close).not.toHaveBeenCalled();
  await expect(backend.execute(statusRequest)).resolves.toMatchObject({
    ok: true,
    result: { captureSession: { source } },
  });
  expect(await backend.beforeChange()).toBeDefined();
  await backend.prepareUnload("remove");
  expect(stop).toHaveBeenCalledTimes(2);
  expect(stop).toHaveBeenLastCalledWith(source, true);
  await backend.close();
});

it("cancels and drains deployment only after unload preparation, then cleans its recovery session", async () => {
  let session: EdaCaptureSessionStatus = { state: "idle", tunnel: "closed", detail: "Idle" };
  let deploymentSignal: AbortSignal | undefined;
  let failDeployment!: (error: Error) => void;
  const deployment = new Promise<EdaCaptureDeployment>((_resolve, reject) => {
    failDeployment = reject;
  });
  const stop = vi.fn(() => Promise.resolve());
  const backend = new EdaCaptureBackend(
    edaBackendHost(),
    edaCapturePort({
      status: () => session,
      deploy: (_input, _progress, signal) => {
        deploymentSignal = signal;
        return deployment;
      },
      stop,
    }),
  );
  const request = backend.execute({
    method: "edaCapture.deploy",
    input: parsePluginJson({
      context: "eda-agent",
      edaApi,
      imageDelivery: "configured",
      localPort: 19092,
      source: source.source,
    }),
    requestId: "deployment",
    correlationId: "deployment",
  });
  expect(await backend.beforeChange()).toBeDefined();
  expect(deploymentSignal?.aborted).toBe(false);
  const unloading = backend.prepareUnload("update");
  expect(deploymentSignal?.aborted).toBe(true);
  expect(stop).not.toHaveBeenCalled();
  session = { state: "cancelled", tunnel: "closed", source, detail: "Remote cleanup pending" };
  failDeployment(new Error("Cancelled deploy; remote removal failed"));
  await expect(request).resolves.toMatchObject({ ok: false });
  await unloading;
  expect(stop).toHaveBeenCalledWith(source, true);
  await backend.close();
});

it("waits for a submitted application installer and rejects new work while draining", async () => {
  let finishInstallation!: (value: EdaCaptureApplicationStatus) => void;
  const installation = new Promise<EdaCaptureApplicationStatus>((resolve) => {
    finishInstallation = resolve;
  });
  const disconnect = vi.fn(() => Promise.resolve());
  const close = vi.fn(() => Promise.resolve());
  const backend = new EdaCaptureBackend(
    edaBackendHost({ disconnectOwnedConnection: disconnect }),
    edaCapturePort({ installApplication: () => installation, close }),
  );
  const pending = backend.execute({
    method: "edaCapture.application.install",
    input: { edaApi },
    requestId: "install",
    correlationId: "install",
  });
  expect((await backend.beforeChange())?.detail).toContain(
    "Application installation already submitted",
  );
  const unloading = backend.prepareUnload("remove");
  await Promise.resolve();
  expect(disconnect).not.toHaveBeenCalled();
  expect(close).not.toHaveBeenCalled();
  await expect(backend.execute(statusRequest)).rejects.toThrow("shutting down");
  finishInstallation({
    state: "installed",
    appId: "capture.streamskope.io",
    publisher: "StreamSkope",
    version: "v26.8.2",
  });
  await expect(pending).resolves.toMatchObject({ ok: true });
  await unloading;
  expect(disconnect).toHaveBeenCalledOnce();
  await backend.close();
});

it("needs no interruption warning when idle", async () => {
  const backend = new EdaCaptureBackend(edaBackendHost(), edaCapturePort());
  expect(await backend.beforeChange()).toBeUndefined();
  await backend.prepareUnload("remove");
  await backend.close();
});

it("distinguishes replacement capture sessions in lifecycle consent", async () => {
  let currentSource = source;
  const backend = new EdaCaptureBackend(
    edaBackendHost(),
    edaCapturePort({
      status: () => ({ state: "ready", tunnel: "open", source: currentSource, detail: "Ready" }),
    }),
  );
  const first = await backend.beforeChange();
  currentSource = { ...source, sessionId: "replacement-session" };
  const replacement = await backend.beforeChange();
  expect(first?.message).toBe(replacement?.message);
  expect(first?.detail).toBe(replacement?.detail);
  expect(first?.stateKey).not.toBe(replacement?.stateKey);
  await backend.close();
});
