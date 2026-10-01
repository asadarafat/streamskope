import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostSecureConnectionInput,
  type SecureConnectionInput,
} from "../../src/features/kafka/contracts";
import type {
  KafkaResolvedTrustAcquisition,
  KafkaTrustAcquisitionServicePort,
} from "../../src/features/kafka/application";
import {
  createFacade,
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

const acquiredConnection: HostSecureConnectionInput = {
  brokers: ["kafka-lab.example.test:9093"],
  name: "Remote Kafka lab",
  oauth: {
    clientId: "streamskope",
    clientSecret: "oauth-secret",
    scope: "kafka",
    tokenEndpoint: "https://kafka-lab.example.test/oauth/token",
  },
  tls: {
    acquisitionId: "acquisition-1",
    enabled: true,
    kind: "jks",
  },
};

type ConnectionCommand = Extract<
  HostCommand,
  { readonly command: "connection.connect" | "connection.test" }
>;

function connectionCommand(
  command: "connection.connect" | "connection.test",
  id: string,
): ConnectionCommand {
  return {
    command,
    id,
    payload: acquiredConnection,
    version: HOST_PROTOCOL_VERSION,
  };
}

class RecordingAcquisitionService implements KafkaTrustAcquisitionServicePort {
  fetchHttpsMaterial(): never {
    throw new Error("HTTPS fetch was not requested.");
  }
  openEditor(): { id: string; generation: number } {
    return { id: "editor", generation: 1 };
  }
  closeEditor(_editorId: string): void {}
  advanceEditor(_editorId: string, _generation: number): void {}
  apply(_acquisitionId: string, _editorId: string): void {}
  capabilities(): { sshAgent: "unavailable" } {
    return { sshAgent: "unavailable" };
  }
  cancel(): never {
    throw new Error("Cancellation was not requested.");
  }
  clearCalls = 0;
  consumeCalls: string[] = [];
  resolveCalls: Array<{ readonly id: string; readonly kind: string; readonly editorId?: string }> =
    [];
  resolution: KafkaResolvedTrustAcquisition = {
    caPem: "-----BEGIN CERTIFICATE-----\nremote-ca\n-----END CERTIFICATE-----",
    id: "acquisition-1",
    kind: "jks",
    label: "remote.truststore.jks",
    material: "binary-material",
    password: "trust-password",
  };

  clear(): void {
    this.clearCalls += 1;
  }

  consume(acquisitionId: string): void {
    this.consumeCalls.push(acquisitionId);
  }

  discard(): void {
    throw new Error("Discard was not requested.");
  }

  discoverHostKey(): never {
    throw new Error("Host-key discovery was not requested.");
  }

  fetchMaterial(): never {
    throw new Error("Material fetch was not requested.");
  }

  fetchPassword(): never {
    throw new Error("Password fetch was not requested.");
  }

  resolve(
    acquisitionId: string,
    expectedKind: "jks" | "pem" | "pkcs12",
    editorId?: string,
  ): KafkaResolvedTrustAcquisition {
    this.resolveCalls.push({
      id: acquisitionId,
      kind: expectedKind,
      ...(editorId === undefined ? {} : { editorId }),
    });
    return this.resolution;
  }
}

describe("Kafka acquired-trust connection facade", () => {
  it("cancels a pending acquired connection when its editor closes without consuming the candidate", async () => {
    const acquisitions = new RecordingAcquisitionService();
    const owner = new AbortController();
    acquisitions.resolution = { ...acquisitions.resolution, lifetimeSignal: owner.signal };
    const port = new RecordingConnectionPort();
    let ready!: () => void;
    const entered = new Promise<void>((resolve) => {
      ready = resolve;
    });
    let release!: (connection: RecordingActiveConnection) => void;
    let observed: AbortSignal | undefined;
    port.openOperations.push((_connection, signal) => {
      observed = signal;
      ready();
      return new Promise((resolve) => {
        release = resolve;
      });
    });
    const facade = createFacade(port, undefined, undefined, acquisitions);
    const pending = facade.execute(connectionCommand("connection.connect", "close-during-connect"));
    await entered;
    owner.abort(new Error("Editor closed"));
    release(new RecordingActiveConnection());
    const result = await pending;
    expect(observed?.aborted).toBe(true);
    expect(result.ok).toBe(false);
    expect(acquisitions.consumeCalls).toEqual([]);
    await facade.shutdown();
  });
  it("passes the owning editor for a scoped ad hoc connection test", async () => {
    const acquisitions = new RecordingAcquisitionService();
    const port = new RecordingConnectionPort();
    const facade = createFacade(port, undefined, undefined, acquisitions);
    await facade.execute({
      command: "connection.test",
      id: "scoped-test",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        ...acquiredConnection,
        tls: { acquisitionId: "acquisition-1", kind: "jks", enabled: true, editorId: "owner" },
      },
    });
    expect(acquisitions.resolveCalls).toEqual([
      { id: "acquisition-1", kind: "jks", editorId: "owner" },
    ]);
    expect(acquisitions.consumeCalls).toEqual([]);
    await facade.shutdown();
  });
  it("resolves an acquired CA for a connection test without consuming it", async () => {
    const acquisitions = new RecordingAcquisitionService();
    const port = new RecordingConnectionPort();
    let tested: SecureConnectionInput | undefined;
    port.testOperations.push((connection) => {
      tested = connection;
      return Promise.resolve({
        checks: ["oauth", "tls", "kafka-authentication", "metadata"],
        topicCount: 2,
      });
    });
    const facade = createFacade(port, undefined, undefined, acquisitions);

    await expect(
      facade.execute(connectionCommand("connection.test", "test-acquired")),
    ).resolves.toMatchObject({ ok: true });

    expect(tested).toEqual({
      ...acquiredConnection,
      tls: {
        caPem: acquisitions.resolution.caPem,
        enabled: true,
      },
    });
    expect(acquisitions.resolveCalls).toEqual([{ id: "acquisition-1", kind: "jks" }]);
    expect(acquisitions.consumeCalls).toEqual([]);
  });

  it("consumes the acquisition only after Kafka confirms a connection", async () => {
    const acquisitions = new RecordingAcquisitionService();
    const port = new RecordingConnectionPort();
    let opened: SecureConnectionInput | undefined;
    port.openOperations.push((connection) => {
      opened = connection;
      return Promise.resolve(new RecordingActiveConnection());
    });
    const facade = createFacade(port, undefined, undefined, acquisitions);

    await expect(
      facade.execute(connectionCommand("connection.connect", "connect-acquired")),
    ).resolves.toMatchObject({ ok: true });

    expect(opened?.tls).toEqual({
      caPem: acquisitions.resolution.caPem,
      enabled: true,
    });
    expect(acquisitions.consumeCalls).toEqual(["acquisition-1"]);
  });

  it("retains the acquisition when the Kafka connection fails", async () => {
    const acquisitions = new RecordingAcquisitionService();
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.reject(new Error("Kafka rejected the connection.")));
    const facade = createFacade(port, undefined, undefined, acquisitions);

    await expect(
      facade.execute(connectionCommand("connection.connect", "connect-failed")),
    ).resolves.toMatchObject({ ok: false });

    expect(acquisitions.resolveCalls).toHaveLength(1);
    expect(acquisitions.consumeCalls).toEqual([]);
  });

  it("rejects acquired trust when the host acquisition owner is unavailable", async () => {
    const port = new RecordingConnectionPort();
    const facade = createFacade(port);

    await expect(
      facade.execute(connectionCommand("connection.test", "test-unavailable")),
    ).resolves.toMatchObject({
      error: {
        code: "BACKEND_UNAVAILABLE",
        stage: "backend",
      },
      ok: false,
    });
    expect(port.testOperations).toEqual([]);
  });
});
