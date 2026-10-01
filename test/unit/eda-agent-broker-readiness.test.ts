import { afterEach, expect, it, vi } from "vitest";

import { EdaAgentCapture } from "../../plugins/eda/backend/eda-agent-capture";

const external = vi.hoisted(() => ({
  listTopics: vi.fn<() => Promise<string[]>>(),
  closeTunnel: vi.fn<() => Promise<void>>(),
  removeSession: vi.fn<() => Promise<void>>(),
  renewSession: vi.fn<() => Promise<void>>(),
}));

vi.mock("../../plugins/eda/backend/eda-agent-tunnel", () => ({
  EdaAgentTunnel: class {
    static listen(): Promise<{ activate(): void; close: typeof external.closeTunnel }> {
      return Promise.resolve({ activate(): void {}, close: external.closeTunnel });
    }
  },
}));

vi.mock("../../plugins/eda/backend/eda-api-client", () => ({
  EdaApiClient: class {
    clusterVersion(): Promise<{ releaseVersion: string; buildVersion: string }> {
      return Promise.resolve({ releaseVersion: "v26.8.2", buildVersion: "v26.8.2" });
    }
    captureApplicationStatus(): Promise<{ state: string }> {
      return Promise.resolve({ state: "installed" });
    }
    getProducer(): Promise<{ spec: { exports: { topic: string }[] } }> {
      return Promise.resolve({
        spec: { exports: [{ topic: "eda-current-alarms" }, { topic: "eda-nodes" }] },
      });
    }
    createCaptureSession(): Promise<{ phase: string }> {
      return Promise.resolve({ phase: "Pending" });
    }
    getCaptureSession(): Promise<{ phase: string }> {
      return Promise.resolve({ phase: "Ready" });
    }
    removeCaptureSession = external.removeSession;
    renewCaptureSession = external.renewSession;
  },
  EdaApiError: class extends Error {},
}));

afterEach(() => {
  vi.clearAllMocks();
  vi.useRealTimers();
});

it.each(["success", "failure"] as const)(
  "drains a late lease renewal %s without resurrecting a closed capture",
  async (outcome) => {
    vi.useFakeTimers();
    external.listTopics.mockResolvedValue([]);
    let finishTunnel!: () => void;
    const tunnel = new Promise<void>((resolve) => {
      finishTunnel = resolve;
    });
    let finishRenewal!: () => void;
    let failRenewal!: (error: Error) => void;
    const renewal = new Promise<void>((resolve, reject) => {
      finishRenewal = resolve;
      failRenewal = reject;
    });
    external.closeTunnel.mockReturnValue(tunnel);
    external.renewSession.mockReturnValue(renewal);
    const capture = new EdaAgentCapture(external.listTopics);
    await capture.deploy({
      context: "eda-agent",
      edaApi: { baseUrl: "https://eda.example.test", username: "operator", password: "secret" },
      imageDelivery: "configured",
      localPort: 19092,
      source: {
        apiVersion: "kafka.eda.nokia.com/v1",
        kind: "ClusterProducer",
        name: "source",
        namespace: "eda-system",
      },
    });
    await vi.advanceTimersByTimeAsync(300_000);
    expect(external.renewSession).toHaveBeenCalledOnce();
    let closed = false;
    const closing = capture.close().then(() => {
      closed = true;
    });
    finishTunnel();
    await Promise.resolve();
    expect(closed).toBe(false);
    if (outcome === "success") finishRenewal();
    else failRenewal(new Error("Late lease rejection"));
    await closing;
    expect(capture.status()).toMatchObject({ state: "stopped", tunnel: "closed" });
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(600_000);
    expect(external.renewSession).toHaveBeenCalledOnce();
  },
);

it("accepts a reachable capture broker before an idle exporter creates its topics", async () => {
  external.listTopics.mockResolvedValue([]);
  external.closeTunnel.mockResolvedValue();
  external.removeSession.mockResolvedValue();
  const capture = new EdaAgentCapture(external.listTopics);
  try {
    await expect(
      capture.deploy({
        context: "eda-agent",
        edaApi: { baseUrl: "https://eda.example.test", username: "operator", password: "secret" },
        imageDelivery: "configured",
        localPort: 19092,
        source: {
          apiVersion: "kafka.eda.nokia.com/v1",
          kind: "ClusterProducer",
          name: "source",
          namespace: "eda-system",
        },
      }),
    ).resolves.toMatchObject({ topics: ["eda-current-alarms", "eda-nodes"] });
    expect(external.listTopics).toHaveBeenCalledTimes(1);
    expect(capture.status().detail).toContain("have not been created yet");
  } finally {
    await capture.close();
  }
});
