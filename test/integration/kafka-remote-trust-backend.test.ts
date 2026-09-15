import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostEvent,
  type RemoteSshTargetInput,
} from "../../src/kafka/contracts";
import type {
  KafkaRemoteHostKeyRequest,
  KafkaRemoteMaterialRequest,
  KafkaRemotePasswordRequest,
  KafkaRemoteTrustPort,
} from "../../src/kafka/application";
import { createKafkaBackend } from "../../src/main";

const target = {
  host: "kafka-lab.example.test",
  hostKeyFingerprint: `SHA256:${"A".repeat(43)}`,
  password: "ssh-password",
  port: 22,
  username: "operator",
} as const satisfies RemoteSshTargetInput;

async function openEditor(
  backend: ReturnType<typeof createKafkaBackend>,
): Promise<import("../../src/kafka/contracts/remote-trust-types").TrustAcquisitionEditor> {
  const response = await backend.execute({
    command: "trustAcquisition.editor.open",
    id: "open-editor",
    version: HOST_PROTOCOL_VERSION,
    payload: {},
  });
  if (!response.ok || !("editor" in response.result))
    throw new Error("Expected a host-owned editor.");
  return response.result.editor;
}

class ControlledRemoteTrustPort implements KafkaRemoteTrustPort {
  discoveryCalls: KafkaRemoteHostKeyRequest[] = [];
  materialCalls: KafkaRemoteMaterialRequest[] = [];
  passwordCalls: KafkaRemotePasswordRequest[] = [];

  constructor(private readonly truststore: Uint8Array) {}

  discoverHostKey(request: KafkaRemoteHostKeyRequest): Promise<string> {
    this.discoveryCalls.push(request);
    return Promise.resolve(target.hostKeyFingerprint);
  }

  fetchMaterial(request: KafkaRemoteMaterialRequest): Promise<Uint8Array> {
    this.materialCalls.push(request);
    return Promise.resolve(this.truststore.slice());
  }

  fetchPassword(request: KafkaRemotePasswordRequest): Promise<string> {
    this.passwordCalls.push(request);
    return Promise.resolve("password\n");
  }
}

describe("Kafka remote-trust backend composition", () => {
  it("dispatches cancellation while discovery is pending and reports a cancelled outcome", async () => {
    const remote = new ControlledRemoteTrustPort(new Uint8Array());
    let markEntered!: () => void;
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    vi.spyOn(remote, "discoverHostKey").mockImplementation(
      (_request, signal?: AbortSignal) =>
        new Promise((_resolve, reject) => {
          signal?.addEventListener(
            "abort",
            () => reject(signal.reason instanceof Error ? signal.reason : new Error("Cancelled")),
            { once: true },
          );
          markEntered();
        }),
    );
    const backend = createKafkaBackend(undefined, undefined, undefined, undefined, remote);
    const events: HostEvent[] = [];
    backend.subscribe((event) => events.push(event));
    try {
      const editor = await openEditor(backend);
      const pending = backend.execute({
        command: "trustAcquisition.hostKey.discover",
        id: "pending-discovery",
        version: HOST_PROTOCOL_VERSION,
        payload: { target: { host: target.host, port: 22 }, editor },
      });
      await entered;
      await expect(
        backend.execute({
          command: "trustAcquisition.cancel",
          id: "cancel-discovery",
          version: HOST_PROTOCOL_VERSION,
          payload: { requestId: "pending-discovery", editorId: editor.id },
        }),
      ).resolves.toMatchObject({ ok: true });
      await expect(pending).resolves.toMatchObject({
        ok: false,
        error: { code: "CANCELLED", activeStateChanged: false, stage: "acquisition" },
      });
      expect(remote.passwordCalls).toEqual([]);
      expect(remote.materialCalls).toEqual([]);
      expect(JSON.stringify(events)).toContain('"outcome":"cancelled"');
      expect(JSON.stringify(events)).not.toContain(target.password);
    } finally {
      await backend.shutdown();
    }
  });
  it("shares one acquisition owner across commands and protected profile commit", async () => {
    const remote = new ControlledRemoteTrustPort(
      await readFile(join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks")),
    );
    const backend = createKafkaBackend(undefined, undefined, undefined, undefined, remote);
    const editor = await openEditor(backend);
    const events: HostEvent[] = [];
    backend.subscribe((event) => {
      events.push(event);
    });

    const discovery = await backend.execute({
      command: "trustAcquisition.hostKey.discover",
      id: "discover-host-key",
      payload: {
        editor,
        target: {
          host: target.host,
          port: target.port,
        },
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(discovery).toMatchObject({
      ok: true,
      result: {
        hostKey: {
          fingerprint: target.hostKeyFingerprint,
          target: {
            host: target.host,
            port: target.port,
          },
        },
      },
    });
    expect(remote.discoveryCalls).toEqual([
      {
        target: {
          host: target.host,
          port: target.port,
        },
      },
    ]);
    if (
      !discovery.ok ||
      !("hostKey" in discovery.result) ||
      discovery.result.hostKey.review === undefined
    )
      throw new Error("Expected identity review");

    const material = await backend.execute({
      command: "trustAcquisition.material.fetch",
      id: "fetch-material",
      payload: {
        editor,
        identityId: discovery.result.hostKey.review.id,
        acceptIdentity: true,
        kind: "jks",
        label: "remote.truststore.jks",
        target,
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(material).toMatchObject({
      ok: true,
      result: {
        acquisition: {
          material: {
            kind: "jks",
            label: "remote.truststore.jks",
            templateName: "nsp-25-4",
          },
          password: { present: true, templateName: "nsp-25-11" },
        },
      },
    });
    if (!material.ok || !("acquisition" in material.result)) {
      throw new Error("Expected a complete safe trust acquisition response.");
    }
    const acquisitionId = material.result.acquisition.id;
    expect(acquisitionId.length).toBeGreaterThan(0);

    await expect(
      backend.execute({
        command: "profiles.create",
        id: "create-profile",
        payload: {
          profile: {
            brokers: ["kafka-lab.example.test:9093"],
            name: "Remote Kafka lab",
            trust: {
              kind: "jks",
              label: "remote.truststore.jks",
              material: { acquisitionId, mode: "acquired", editorId: editor.id },
              password: { acquisitionId, mode: "acquired", editorId: editor.id },
            },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });

    await expect(
      backend.execute({
        command: "connection.test",
        id: "reuse-consumed",
        payload: {
          brokers: ["kafka-lab.example.test:9093"],
          name: "Remote Kafka lab",
          tls: {
            acquisitionId,
            enabled: true,
            kind: "jks",
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      error: {
        code: "ACQUISITION_NOT_FOUND",
        stage: "acquisition",
      },
      ok: false,
    });

    expect(remote.passwordCalls).toHaveLength(1);
    expect(remote.materialCalls).toHaveLength(1);
    expect(remote.materialCalls[0]?.command).not.toContain("{storepass}");
    const visible = JSON.stringify(events);
    expect(visible).not.toMatch(/ssh-password|operator|AQID|BEGIN CERTIFICATE/);
    await backend.shutdown();
  });
});
