import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseEdaCaptureDeployInput,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
} from "../../plugins/eda/contracts";

const source = {
  apiVersion: "kafka.eda.nokia.com/v1",
  kind: "ClusterProducer",
  name: "existing-export",
  namespace: "eda-system",
} as const;

describe("EDA capture host contract", () => {
  it.each([
    {
      command: "edaCapture.contexts",
      payload: { kubeconfig: "private-kubernetes-credentials" },
      result: { contexts: [{ name: "production", server: "https://k8s.example.test" }] },
    },
    {
      command: "edaCapture.configure",
      payload: {
        kubeconfig: "private-kubernetes-credentials",
        context: "production",
        edaApiUrl: "https://eda.example.test",
        image: "registry.example.test/redpanda:v24.3.7",
      },
      result: { captureHost: { state: "configured", detail: "Configured." } },
    },
  ])("rejects the retired $command request and response", ({ command, payload, result }) => {
    expect(() =>
      parseHostCommand({
        command,
        id: "retired-command",
        payload,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommandResponse({
        command,
        id: "retired-command",
        ok: true,
        result: { correlationId: "retired-command", ...result },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(HostContractValidationError);
  });

  it("rejects incompatible EDA plugin protocol versions", () => {
    expect(HOST_PROTOCOL_VERSION).toBe(1);
    expect(() =>
      parseHostCommand({ command: "edaCapture.status", id: "old-plugin", payload: {}, version: 0 }),
    ).toThrow(/command.version/u);
    expect(() =>
      parseHostCommandResponse({
        command: "edaCapture.status",
        id: "old-plugin",
        ok: true,
        result: {},
        version: 0,
      }),
    ).toThrow(/response.version/u);
    expect(() =>
      parseHostEvent({ event: "edaCapture.progress", payload: {}, sequence: 1, version: 0 }),
    ).toThrow(/event.version/u);
  });

  it("parses bounded request-scoped EDA application approval", () => {
    const command = parseHostCommand({
      command: "edaCapture.application.install",
      id: "install-capture",
      payload: {
        authorization: { username: "platform-admin", password: "one-time-secret" },
        edaApi: {
          baseUrl: "https://eda.example.test",
          username: "operator",
          password: "operator-secret",
        },
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(command).toMatchObject({
      command: "edaCapture.application.install",
      payload: { authorization: { username: "platform-admin" } },
    });
    expect(() =>
      parseHostCommand({
        command: "edaCapture.application.install",
        id: "install-capture",
        payload: {
          authorization: { username: "platform-admin", password: "secret", retained: true },
          edaApi: {
            baseUrl: "https://eda.example.test",
            username: "operator",
            password: "operator-secret",
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(/retained/u);
  });
  it("parses bounded EDA API inspection and deployment commands", () => {
    expect(
      parseHostCommand({
        command: "edaCapture.inspect",
        id: "inspect-1",
        payload: {
          edaApi: {
            baseUrl: "https://eda.example.test:9443",
            password: "password",
            username: "admin",
            verifyTls: false,
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "edaCapture.inspect",
      payload: {
        edaApi: {
          baseUrl: "https://eda.example.test:9443",
          username: "admin",
          verifyTls: false,
        },
      },
    });
    expect(
      parseHostCommand({
        command: "edaCapture.inspect",
        id: "inspect-api",
        payload: {
          edaApi: {
            baseUrl: "https://eda.example.test:9443",
            password: "password",
            username: "admin",
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      payload: {
        edaApi: { baseUrl: "https://eda.example.test:9443", username: "admin" },
      },
    });
    expect(
      parseHostCommand({
        command: "edaCapture.deploy",
        id: "deploy-1",
        payload: {
          context: "kind-eda",
          edaApi: {
            baseUrl: "https://eda.example.test:9443",
            password: "password",
            username: "admin",
          },
          image: "registry.example/stream/redpanda:v24.3.7",
          imageDelivery: "cluster",
          imagePullSecret: "registry-pull",
          localPort: 19_092,
          source,
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      command: "edaCapture.deploy",
      payload: { imageDelivery: "cluster", imagePullSecret: "registry-pull", source },
    });
    expect(
      parseEdaCaptureDeployInput({
        context: "kind-eda",
        edaApi: {
          baseUrl: "https://eda.example.test:9443",
          password: "password",
          username: "admin",
        },
        image: "docker.redpanda.com/redpandadata/redpanda:v25.1.1",
        imageDelivery: "embedded",
        localPort: 19_092,
        registry: {
          advertisedHost: "bridge.example.test",
          certificatePem: "certificate",
          port: 5_443,
          privateKeyPem: "private-key",
        },
        source,
      }),
    ).toMatchObject({
      imageDelivery: "embedded",
      registry: { advertisedHost: "bridge.example.test", port: 5_443 },
    });
    expect(
      parseEdaCaptureDeployInput({
        context: "kind-eda",
        edaApi: {
          baseUrl: "https://eda.example.test:9443",
          password: "password",
          username: "admin",
        },
        imageDelivery: "configured",
        localPort: 19_092,
        source,
      }),
    ).toEqual({
      context: "kind-eda",
      edaApi: {
        baseUrl: "https://eda.example.test:9443",
        password: "password",
        username: "admin",
        verifyTls: true,
      },
      imageDelivery: "configured",
      localPort: 19_092,
      source,
    });
  });

  it("rejects capture commands that omit EDA API credentials", () => {
    expect(() =>
      parseHostCommand({
        command: "edaCapture.inspect",
        id: "inspect-without-api",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toThrow(/edaApi/u);
    expect(() =>
      parseEdaCaptureDeployInput({
        context: "kind-eda",
        imageDelivery: "configured",
        localPort: 19_092,
        source,
      }),
    ).toThrow(/edaApi/u);
  });

  it("rejects URL-style images, privileged ports, and undeclared fields", () => {
    expect(() =>
      parseEdaCaptureDeployInput({
        context: "kind-eda",
        edaApi: {
          baseUrl: "https://eda.example.test:9443",
          password: "password",
          username: "admin",
        },
        image: "http://host:5000/redpanda:latest",
        imageDelivery: "cluster",
        localPort: 19_092,
        source,
      }),
    ).toThrow(/OCI image reference/u);
    expect(() =>
      parseEdaCaptureDeployInput({
        context: "kind-eda",
        edaApi: {
          baseUrl: "https://eda.example.test:9443",
          password: "password",
          username: "admin",
        },
        image: "registry.example/redpanda:v1",
        imageDelivery: "cluster",
        localPort: 92,
        source,
      }),
    ).toThrow(/at least 1024/u);
    expect(() =>
      parseEdaCaptureDeployInput({
        context: "kind-eda",
        edaApi: {
          baseUrl: "https://eda.example.test:9443",
          password: "password",
          username: "admin",
        },
        image: "registry.example/redpanda:v1",
        imageDelivery: "cluster",
        localPort: 19_092,
        source,
        token: "must-not-cross",
      }),
    ).toThrow(/token.*not declared/u);
  });

  it("parses only bounded typed inspection and deployment responses", () => {
    expect(
      parseHostCommandResponse({
        command: "edaCapture.inspect",
        id: "inspect-1",
        ok: true,
        result: {
          correlationId: "correlation-1",
          inspection: {
            context: "kind-eda",
            contexts: ["kind-eda"],
            imageSetup: { state: "unconfigured" },
            namespace: "eda-system",
            sources: [{ ...source, topics: ["interfaces"] }],
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      result: {
        inspection: {
          sources: [{ kind: "ClusterProducer", topics: ["interfaces"] }],
        },
      },
    });
    expect(
      parseHostCommandResponse({
        command: "edaCapture.deploy",
        id: "deploy-1",
        ok: true,
        result: {
          correlationId: "correlation-2",
          deployment: {
            broker: "127.0.0.1:19092",
            clusterBroker: "streamskope-redpanda.eda-system.svc:9092",
            context: "kind-eda",
            exporterName: "streamskope-capture",
            namespace: "eda-system",
            profileName: "EDA capture · existing-export",
            topics: ["interfaces"],
            workloadName: "streamskope-redpanda",
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      result: {
        deployment: {
          broker: "127.0.0.1:19092",
          clusterBroker: "streamskope-redpanda.eda-system.svc:9092",
          exporterName: "streamskope-capture",
        },
      },
    });
  });

  it("parses bounded capture progress events", () => {
    expect(
      parseHostEvent({
        event: "edaCapture.progress",
        payload: {
          detail: "Waiting for Redpanda.",
          phase: "waiting-broker",
          requestId: "deploy-1",
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      }),
    ).toMatchObject({
      event: "edaCapture.progress",
      payload: { phase: "waiting-broker", requestId: "deploy-1" },
    });
  });
});
