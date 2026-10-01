import { createServer, type Server } from "node:net";

import { expect, it, vi } from "vitest";

import { EdaCaptureBackend } from "../../plugins/eda/backend";
import { EdaAgentCapture } from "../../plugins/eda/backend/eda-agent-capture";
import { EdaApiClient } from "../../plugins/eda/backend/eda-api-client";
import {
  EDA_CAPTURE_APPLICATION,
  EDA_TARGET_VERSION,
  parseEdaCaptureResponse,
} from "../../plugins/eda/contracts";
import { parsePluginJson } from "../../src/plugins/validation";
import { edaBackendHost } from "../support/eda-backend";

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error === undefined ? resolve() : reject(error)));
  });
}

it("reports an occupied local capture port through the plugin response and can retry after it is freed", async () => {
  vi.spyOn(EdaApiClient.prototype, "clusterVersion").mockResolvedValue({
    releaseVersion: EDA_TARGET_VERSION,
    buildVersion: EDA_TARGET_VERSION,
  });
  vi.spyOn(EdaApiClient.prototype, "captureApplicationStatus").mockResolvedValue({
    ...EDA_CAPTURE_APPLICATION,
    state: "installed",
  });
  vi.spyOn(EdaApiClient.prototype, "getProducer").mockResolvedValue({
    spec: { exports: [{ topic: "interfaces" }] },
  });
  const createSession = vi
    .spyOn(EdaApiClient.prototype, "createCaptureSession")
    .mockImplementation((input) => Promise.resolve({ id: input.id, phase: "Ready" }));
  vi.spyOn(EdaApiClient.prototype, "getCaptureSession").mockImplementation((id) =>
    Promise.resolve({ id, phase: "Ready" }),
  );
  const removeSession = vi.spyOn(EdaApiClient.prototype, "removeCaptureSession");
  const probe = vi.fn(() => Promise.resolve(["interfaces"]));
  const capture = new EdaAgentCapture(probe);
  const activity: string[] = [];
  const backend = new EdaCaptureBackend(
    edaBackendHost({ recordActivity: (entry) => activity.push(entry.detail) }),
    capture,
  );
  const listener = createServer();
  try {
    await new Promise<void>((resolve, reject) => {
      listener.once("error", reject);
      listener.listen(0, "127.0.0.1", () => {
        listener.off("error", reject);
        resolve();
      });
    });
    const address = listener.address();
    if (address === null || typeof address === "string") throw new Error("Expected a TCP port");
    const endpoint = `127.0.0.1:${String(address.port)}`;
    const request = {
      method: "edaCapture.deploy",
      input: parsePluginJson({
        context: "eda-agent",
        edaApi: {
          baseUrl: "https://eda.example.test",
          username: "operator",
          password: "fixture-secret",
        },
        imageDelivery: "configured",
        localPort: address.port,
        source: {
          apiVersion: "kafka.eda.nokia.com/v1",
          kind: "Producer",
          namespace: "eda",
          name: "interfaces",
        },
      }),
      requestId: "occupied-port",
      correlationId: "capture-port-conflict",
    };
    const response = parseEdaCaptureResponse(await backend.execute(request));
    expect(response).toMatchObject({
      ok: false,
      error: {
        code: "BACKEND_UNAVAILABLE",
        stage: "backend",
        target: endpoint,
        retryable: true,
        summary: `The local Kafka endpoint ${endpoint} is already in use.`,
      },
    });
    if (response.ok) throw new Error("Capture unexpectedly succeeded on an occupied port");
    expect(response.error.recovery).toMatch(/different Local Kafka port.*stop.*listener/u);
    expect(activity.join(" ")).toContain(`${endpoint} is already in use`);
    expect(activity.join(" ")).not.toContain("fixture-secret");
    expect(capture.status()).toMatchObject({ state: "failed", tunnel: "closed" });
    expect(capture.status().source).toBeUndefined();
    expect(createSession).not.toHaveBeenCalled();
    expect(removeSession).not.toHaveBeenCalled();
    expect(probe).not.toHaveBeenCalled();
    expect(listener.listening).toBe(true);

    await closeServer(listener);
    const retried = parseEdaCaptureResponse(
      await backend.execute({ ...request, requestId: "retry-freed-port" }),
    );
    expect(retried).toMatchObject({ ok: true, result: { deployment: { broker: endpoint } } });
    expect(capture.status()).toMatchObject({ state: "ready", tunnel: "open" });
    expect(createSession).toHaveBeenCalledOnce();
    expect(probe).toHaveBeenCalledWith([endpoint], expect.any(AbortSignal));
  } finally {
    await capture.close();
    if (listener.listening) await closeServer(listener);
  }
});
