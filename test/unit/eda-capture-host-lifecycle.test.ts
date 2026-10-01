import { describe, expect, it } from "vitest";

import { EdaCaptureBackend } from "../../plugins/eda/backend";
import { edaBackendHost, edaCapturePort } from "../support/eda-backend";
import { toPluginProfileSource } from "../../plugins/eda/contracts";
import type { ProfileSummary } from "../../src/features/kafka/contracts";
import { parsePluginJson } from "../../src/plugins/validation";
import type {
  EdaCaptureHostStatus,
  EdaCaptureSessionStatus,
  ProfileEdaCaptureSource,
} from "../../plugins/eda/contracts";

describe("explicit EDA capture host", () => {
  it("removes only recovery profiles for the exact cleaned-up session", async () => {
    const source: ProfileEdaCaptureSource = {
      kind: "eda-capture",
      state: "ready",
      broker: "127.0.0.1:19092",
      clusterBroker: "capture:9092",
      edaApiUrl: "https://eda.example.test",
      context: "eda-agent",
      sessionId: "current-session",
      exporterName: "capture",
      workloadName: "broker",
      topics: [],
      source: {
        apiVersion: "kafka.eda.nokia.com/v1",
        kind: "Producer",
        namespace: "eda-system",
        name: "interfaces",
      },
    };
    function profile(id: string, captureSource: ProfileEdaCaptureSource): ProfileSummary {
      return {
        id,
        name: id,
        transport: "plaintext",
        brokers: [captureSource.broker],
        active: false,
        createdAt: "2026-09-30",
        updatedAt: "2026-09-30",
        source: toPluginProfileSource(captureSource),
      };
    }
    const legacySource = { ...source };
    delete legacySource.sessionId;
    const profiles = [
      profile("current", source),
      profile("older", { ...source, sessionId: "older-session" }),
      profile("legacy-without-session", legacySource),
      profile("another-cluster", { ...source, edaApiUrl: "https://another.example.test" }),
    ];
    const removed: string[] = [];
    const backend = new EdaCaptureBackend(
      edaBackendHost({
        profiles: () => Promise.resolve(profiles),
        deleteProfile: (id) => {
          removed.push(id);
          return Promise.resolve();
        },
      }),
      edaCapturePort(),
    );
    await expect(
      backend.execute({
        method: "edaCapture.remove",
        input: parsePluginJson({ source }),
        requestId: "remove",
        correlationId: "cleanup",
      }),
    ).resolves.toMatchObject({ ok: true });
    expect(removed).toEqual(["current"]);
  });

  it("requires a matching live session and exact endpoint before connecting a saved capture", async () => {
    const source: ProfileEdaCaptureSource = {
      kind: "eda-capture",
      state: "ready",
      broker: "127.0.0.1:19092",
      clusterBroker: "capture:9092",
      edaApiUrl: "https://eda.example.test",
      context: "explicit-host",
      sessionId: "current-session",
      exporterName: "streamskope-capture",
      workloadName: "streamskope-redpanda",
      topics: [],
      source: {
        apiVersion: "kafka.eda.nokia.com/v1",
        kind: "Producer",
        namespace: "eda-system",
        name: "interfaces",
      },
    };
    let status: EdaCaptureSessionStatus = {
      state: "ready",
      tunnel: "open",
      source,
      detail: "Ready",
    };
    const controller = new EdaCaptureBackend(edaBackendHost(), {
      status: (): EdaCaptureSessionStatus => status,
      preflight: (): Promise<EdaCaptureHostStatus> =>
        Promise.resolve({ state: "configured", detail: "Configured" }),
      close: (): Promise<void> => Promise.resolve(),
      stop: (): Promise<void> => Promise.resolve(),
      inspect: (): Promise<never> => Promise.reject(new Error("Unexpected inspection")),
      deploy: (): Promise<never> => Promise.reject(new Error("Unexpected deployment")),
    });
    await expect(
      controller.validateProfile(toPluginProfileSource(source).data, [source.broker]),
    ).resolves.toBeUndefined();
    await expect(
      controller.validateProfile(toPluginProfileSource(source).data, ["other:9092"]),
    ).rejects.toThrow(/matching running tunnel/u);
    await expect(
      controller.validateProfile(
        toPluginProfileSource({ ...source, sessionId: "old-session" }).data,
        [source.broker],
      ),
    ).rejects.toThrow(/matching running tunnel/u);
    status = { state: "idle", tunnel: "closed", detail: "Restarted" };
    await expect(
      controller.validateProfile(toPluginProfileSource(source).data, [source.broker]),
    ).rejects.toThrow(/matching running tunnel/u);
  });
});
