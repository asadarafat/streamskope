import { describe, expect, it } from "vitest";

import { translateFacadeFailure } from "../../src/features/kafka/facade/facade-support";
import {
  HOST_PROTOCOL_VERSION,
  type EdaCaptureDeployment,
  type ProfileEdaCaptureSource,
} from "../../plugins/eda/contracts";
import type { EdaCapturePort } from "../../plugins/eda/backend/eda-capture-port";
import {
  executeEdaCaptureCommand,
  type EdaCaptureCommand,
  type EdaCaptureFacadeBindings,
} from "../../plugins/eda/backend/eda-capture-facade";

const inspectCommand: EdaCaptureCommand = {
  command: "edaCapture.inspect",
  id: "inspect-1",
  payload: {
    edaApi: {
      baseUrl: "https://eda.example.test",
      password: "password",
      username: "admin",
    },
  },
  version: HOST_PROTOCOL_VERSION,
};

const capture: EdaCapturePort = {
  preflight: () =>
    Promise.resolve({ state: "unavailable", detail: "Not configured in this fixture." }),
  status: () => ({ state: "idle", tunnel: "closed", detail: "No capture." }),
  stop: () => Promise.resolve(),
  close: () => Promise.resolve(),
  deploy: () => Promise.reject(new Error("Deployment was not expected.")),
  inspect: () =>
    Promise.resolve({
      context: "kind-eda",
      contexts: ["kind-eda"],
      imageSetup: { state: "unconfigured" },
      namespace: "eda-system",
      sources: [],
    }),
};

describe("EDA capture facade", () => {
  it("redacts both operator and one-time administrator credentials", async () => {
    const activity: string[] = [];
    const response = await executeEdaCaptureCommand(
      {
        command: "edaCapture.application.install",
        id: "install",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          authorization: { username: "platform-admin", password: "one-time-secret" },
          edaApi: {
            baseUrl: "https://eda.example.test",
            username: "operator",
            password: "operator-secret",
          },
        },
      },
      "installation",
      {
        capture: {
          ...capture,
          installApplication: () =>
            Promise.reject(new Error("operator-secret one-time-secret rejected")),
        },
        failure: (error, context) =>
          translateFacadeFailure(
            error,
            { ...context, activeStateChanged: false, connection: undefined },
            true,
          ).error,
        recordActivity: (entry) => activity.push(entry.detail),
      },
    );
    expect(response.ok).toBe(false);
    expect(activity.join(" ")).not.toContain("operator-secret");
    expect(activity.join(" ")).not.toContain("one-time-secret");
  });
  it.each([false, true])(
    "removes linked profiles only after confirmed remote cleanup (failure=%s)",
    async (fail) => {
      const order: string[] = [];
      const source: ProfileEdaCaptureSource = {
        kind: "eda-capture",
        state: "ready",
        broker: "127.0.0.1:19092",
        clusterBroker: "capture:9092",
        exporterName: "streamskope-capture",
        workloadName: "streamskope-redpanda",
        topics: [],
        edaApiUrl: "https://eda.example.test",
        context: "remote",
        source: {
          apiVersion: "kafka.eda.nokia.com/v1",
          kind: "Producer",
          namespace: "eda-system",
          name: "original",
        },
      };
      const result = await executeEdaCaptureCommand(
        {
          command: "edaCapture.remove",
          id: "remove",
          version: HOST_PROTOCOL_VERSION,
          payload: { source },
        },
        "cleanup",
        {
          capture: {
            ...capture,
            stop: () => {
              order.push("remote");
              return fail ? Promise.reject(new Error("fixture denial")) : Promise.resolve();
            },
          },
          failure: (error, context) =>
            translateFacadeFailure(
              error,
              { ...context, activeStateChanged: false, connection: undefined },
              true,
            ).error,
          recordActivity: () => undefined,
          removeCaptureProfiles: (target) => {
            expect(target).toEqual(source);
            order.push("profiles");
            return Promise.resolve();
          },
        },
      );
      expect(result.ok).toBe(!fail);
      expect(order).toEqual(fail ? ["remote"] : ["remote", "profiles"]);
    },
  );
  it("cancels the owned deployment and rejects overlap without starting another adapter operation", async () => {
    const operations = new Map<string, AbortController>();
    let calls = 0;
    const command: EdaCaptureCommand = {
      command: "edaCapture.deploy",
      id: "cancellable",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        context: "explicit-host",
        edaApi: { baseUrl: "https://eda.example.test", username: "admin", password: "fixture" },
        imageDelivery: "configured",
        localPort: 19092,
        source: {
          apiVersion: "kafka.eda.nokia.com/v1",
          kind: "Producer",
          namespace: "eda-system",
          name: "interfaces",
        },
      },
    };
    const bindings: EdaCaptureFacadeBindings = {
      operations,
      failure: (error, context) =>
        translateFacadeFailure(
          error,
          { ...context, activeStateChanged: false, connection: undefined },
          true,
        ).error,
      recordActivity: (): void => undefined,
      capture: {
        ...capture,
        deploy: (
          _input: unknown,
          _progress: unknown,
          signal?: AbortSignal,
        ): Promise<EdaCaptureDeployment> => {
          calls += 1;
          return new Promise((_resolve, reject) =>
            signal?.addEventListener(
              "abort",
              () => reject(new Error("Cancelled fixture deployment")),
              { once: true },
            ),
          );
        },
      },
    };
    const pending = executeEdaCaptureCommand(command, "first", bindings);
    expect(operations.size).toBe(1);
    expect(
      (await executeEdaCaptureCommand({ ...command, id: "second" }, "second", bindings)).ok,
    ).toBe(false);
    expect(calls).toBe(1);
    await executeEdaCaptureCommand(
      {
        command: "edaCapture.cancel",
        id: "cancel",
        version: HOST_PROTOCOL_VERSION,
        payload: { requestId: command.id },
      },
      "cancel",
      bindings,
    );
    expect((await pending).ok).toBe(false);
    expect(operations.size).toBe(0);
    expect(
      (
        await executeEdaCaptureCommand(command, "connected", {
          ...bindings,
          connectionActive: () => true,
        })
      ).ok,
    ).toBe(false);
    expect(calls).toBe(1);
  });
  it("returns typed inspection evidence and records safe activity", async () => {
    const activity: string[] = [];
    const response = await executeEdaCaptureCommand(inspectCommand, "correlation-1", {
      capture,
      failure: (error, context) =>
        translateFacadeFailure(
          error,
          { ...context, activeStateChanged: false, connection: undefined },
          true,
        ).error,
      recordActivity: (entry) => activity.push(entry.detail),
    });

    expect(response).toMatchObject({
      ok: true,
      result: {
        correlationId: "correlation-1",
        inspection: { context: "kind-eda", namespace: "eda-system" },
      },
    });
    expect(activity).toEqual(["Found 0 EDA Kafka export source(s) in eda-system."]);
  });

  it("returns a bounded unsupported-operation failure when the adapter is absent", async () => {
    const response = await executeEdaCaptureCommand(inspectCommand, "correlation-2", {
      capture: undefined,
      failure: (error, context) =>
        translateFacadeFailure(
          error,
          { ...context, activeStateChanged: false, connection: undefined },
          true,
        ).error,
      recordActivity: () => undefined,
    });

    expect(response).toMatchObject({
      error: {
        code: "UNSUPPORTED_OPERATION",
        stage: "backend",
      },
      ok: false,
    });
  });

  it("redacts embedded registry private material from failure activity", async () => {
    const privateKey = "private-key-must-not-reach-activity";
    const activity: string[] = [];
    const response = await executeEdaCaptureCommand(
      {
        command: "edaCapture.deploy",
        id: "deploy-1",
        payload: {
          context: "kind-eda",
          edaApi: {
            baseUrl: "https://eda.example.test",
            password: privateKey,
            username: "admin",
          },
          image: "docker.example/redpanda:v1",
          imageDelivery: "embedded",
          localPort: 19_092,
          registry: {
            advertisedHost: "bridge.example.test",
            certificatePem: "certificate",
            port: 5_443,
            privateKeyPem: privateKey,
          },
          source: {
            apiVersion: "kafka.eda.nokia.com/v1",
            kind: "ClusterProducer",
            name: "existing-export",
            namespace: "eda-system",
          },
        },
        version: HOST_PROTOCOL_VERSION,
      },
      "correlation-3",
      {
        capture: {
          ...capture,
          deploy: () => Promise.reject(new Error(`failed with ${privateKey}`)),
        },
        failure: (error, context) =>
          translateFacadeFailure(
            error,
            { ...context, activeStateChanged: false, connection: undefined },
            true,
          ).error,
        recordActivity: (entry) => activity.push(entry.detail),
      },
    );

    expect(response.ok).toBe(false);
    expect(JSON.stringify(response)).not.toContain(privateKey);
    expect(activity.join(" ")).not.toContain(privateKey);
  });

  it("publishes backend capture phases with the command request ID", async () => {
    const progress: unknown[] = [];
    const response = await executeEdaCaptureCommand(
      {
        command: "edaCapture.deploy",
        id: "deploy-progress",
        payload: {
          context: "kind-eda",
          edaApi: {
            baseUrl: "https://eda.example.test",
            password: "password",
            username: "admin",
          },
          imageDelivery: "configured",
          localPort: 19_092,
          source: {
            apiVersion: "kafka.eda.nokia.com/v1",
            kind: "ClusterProducer",
            name: "existing-export",
            namespace: "eda-system",
          },
        },
        version: HOST_PROTOCOL_VERSION,
      },
      "correlation-progress",
      {
        capture: {
          ...capture,
          deploy: (_input, observer): Promise<EdaCaptureDeployment> => {
            observer?.("waiting-broker", "Waiting for Redpanda.");
            return Promise.resolve({
              broker: "127.0.0.1:19092",
              clusterBroker: "streamskope-redpanda.eda-system.svc:9092",
              context: "kind-eda",
              exporterName: "streamskope-capture",
              namespace: "eda-system",
              profileName: "EDA capture · existing-export",
              topics: ["interfaces"],
              workloadName: "streamskope-redpanda",
            });
          },
        },
        publishProgress: (event) => progress.push(event),
        failure: (error, context) =>
          translateFacadeFailure(
            error,
            { ...context, activeStateChanged: false, connection: undefined },
            true,
          ).error,
        recordActivity: () => undefined,
      },
    );

    expect(response.ok).toBe(true);
    expect(progress).toEqual([
      {
        detail: "Waiting for Redpanda.",
        phase: "waiting-broker",
        requestId: "deploy-progress",
      },
    ]);
  });
});
