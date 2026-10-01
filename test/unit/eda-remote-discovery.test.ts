import { afterEach, describe, expect, it, vi } from "vitest";

import { EdaApiClient } from "../../plugins/eda/backend/eda-api-client";
import { EdaAgentCapture } from "../../plugins/eda/backend/eda-agent-capture";
import { parseEdaCaptureInspection } from "../../plugins/eda/contracts/eda-capture-validation";

afterEach(() => vi.unstubAllEnvs());

describe("remote EDA discovery", () => {
  it("discovers with the application adapter without reading the local kubeconfig", async () => {
    vi.spyOn(EdaApiClient.prototype, "clusterVersion").mockResolvedValue({
      releaseVersion: "v26.8.2",
      buildVersion: "v26.8.2",
    });
    const list = vi.spyOn(EdaApiClient.prototype, "listProducers").mockResolvedValue([
      {
        apiVersion: "kafka.eda.nokia.com/v1",
        kind: "Producer",
        metadata: { name: "interfaces", namespace: "eda-system" },
        spec: {
          brokers: "kafka-a.example.test:9093,kafka-b.example.test:9093",
          exports: [{ topic: "interfaces" }],
          sasl: { password: "never-return-this" },
        },
      },
    ]);
    vi.stubEnv("KUBECONFIG", "/not-present/config");
    const capture = new EdaAgentCapture(() => Promise.reject(new Error("No capture expected")));
    const result = await capture.inspect({
      edaApi: {
        baseUrl: "https://remote.example.test",
        username: "operator",
        password: "fixture",
      },
    });
    expect(list).toHaveBeenCalledOnce();
    expect(result.sources).toEqual([
      {
        apiVersion: "kafka.eda.nokia.com/v1",
        kind: "Producer",
        name: "interfaces",
        namespace: "eda-system",
        topics: ["interfaces"],
        brokers: ["kafka-a.example.test:9093", "kafka-b.example.test:9093"],
      },
    ]);
    expect(JSON.stringify(result)).not.toContain("never-return-this");
    expect(result.context).toBeUndefined();
    expect(parseEdaCaptureInspection(result)).toEqual(result);
    expect(await capture.preflight()).toMatchObject({ state: "configured", context: "eda-agent" });
    await capture.close();
  });
});
