import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type RemoteTrustAcquisitionSummary,
} from "../../src/kafka/contracts";
import type { KafkaTrustAcquisitionServicePort } from "../../src/kafka/application";
import {
  executeTrustAcquisitionCommand,
  type TrustAcquisitionFacadeBindings,
} from "../../src/kafka/facade/trust-acquisition-facade";
import { createFacade, RecordingConnectionPort } from "../support/kafka-backend-facade-fixture";

const target = {
  host: "kafka-lab.example.test",
  hostKeyFingerprint: `SHA256:${"A".repeat(43)}`,
  password: "ssh-password",
  port: 22,
  username: "operator",
} as const;

const summary: RemoteTrustAcquisitionSummary = {
  expiresAt: "2026-07-26T13:10:00.000Z",
  id: "acquisition-1",
  material: null,
  password: {
    present: true,
    templateName: "Remote password",
  },
  target: {
    host: "kafka-lab.example.test",
    hostKeyFingerprint: target.hostKeyFingerprint,
    port: 22,
  },
};

class FakeAcquisitionService implements KafkaTrustAcquisitionServicePort {
  fetchHttpsMaterial(): Promise<RemoteTrustAcquisitionSummary> {
    return Promise.reject(new Error("HTTPS is not configured in this SSH fixture"));
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
  cancelCalls: string[] = [];
  cancel(requestId: string): void {
    this.cancelCalls.push(requestId);
  }
  clearCalls = 0;
  consumeCalls: string[] = [];
  discardCalls: string[] = [];
  discoveryCalls: unknown[] = [];
  failure: Error | undefined;
  materialCalls: unknown[] = [];
  passwordCalls: unknown[] = [];

  clear(): void {
    this.clearCalls += 1;
  }

  consume(acquisitionId: string): void {
    this.consumeCalls.push(acquisitionId);
  }

  discard(acquisitionId: string): void {
    this.discardCalls.push(acquisitionId);
    if (this.failure !== undefined) {
      throw this.failure;
    }
  }

  discoverHostKey(input: unknown): Promise<{
    readonly fingerprint: string;
    readonly target: { readonly host: string; readonly port: number };
  }> {
    this.discoveryCalls.push(input);
    return Promise.resolve({
      fingerprint: target.hostKeyFingerprint,
      target: {
        host: target.host,
        port: target.port,
      },
    });
  }

  fetchMaterial(input: unknown): Promise<RemoteTrustAcquisitionSummary> {
    this.materialCalls.push(input);
    return this.failure === undefined
      ? Promise.resolve({
          ...summary,
          material: {
            byteCount: 4_096,
            kind: "jks",
            label: "nsp.truststore",
            templateName: "Remote trust",
          },
        })
      : Promise.reject(this.failure);
  }

  fetchPassword(input: unknown): Promise<RemoteTrustAcquisitionSummary> {
    this.passwordCalls.push(input);
    return this.failure === undefined ? Promise.resolve(summary) : Promise.reject(this.failure);
  }

  resolve(): never {
    throw new Error("Resolution is not used in acquisition command tests.");
  }
}

function passwordCommand(): Extract<
  HostCommand,
  { readonly command: "trustAcquisition.password.fetch" }
> {
  return {
    command: "trustAcquisition.password.fetch",
    id: "fetch-password",
    payload: { target },
    version: HOST_PROTOCOL_VERSION,
  };
}

function bindings(acquisitions: FakeAcquisitionService): {
  readonly activity: TrustAcquisitionFacadeBindings["recordActivity"] extends (
    input: infer T,
  ) => void
    ? T[]
    : never;
  readonly value: TrustAcquisitionFacadeBindings;
} {
  const activity: Parameters<TrustAcquisitionFacadeBindings["recordActivity"]>[0][] = [];
  return {
    activity,
    value: {
      acquisitions,
      available: true,
      recordActivity: (input): void => {
        activity.push(input);
      },
    },
  };
}

describe("Kafka remote-trust facade", () => {
  it("rejects unscoped discovery and material acquisition before remote work", async () => {
    const acquisitions = new FakeAcquisitionService();
    const { value } = bindings(acquisitions);
    for (const command of [
      {
        command: "trustAcquisition.hostKey.discover",
        payload: { target: { host: target.host, port: 22 } },
      },
      {
        command: "trustAcquisition.material.fetch",
        payload: { target, kind: "pem", label: "trust" },
      },
    ] as const) {
      await expect(
        executeTrustAcquisitionCommand(
          { ...command, id: "unscoped", version: HOST_PROTOCOL_VERSION },
          "ownership",
          value,
        ),
      ).resolves.toMatchObject({ ok: false });
    }
    expect(acquisitions.discoveryCalls).toEqual([]);
    expect(acquisitions.materialCalls).toEqual([]);
  });
  it("reports agent capability without starting SSH or exposing a socket path", async () => {
    const acquisitions = new FakeAcquisitionService();
    const { value } = bindings(acquisitions);
    const response = await executeTrustAcquisitionCommand(
      {
        command: "trustAcquisition.capabilities",
        id: "capabilities",
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      },
      "capability-review",
      value,
    );
    expect(response).toMatchObject({ ok: true, result: { sshAgent: "unavailable" } });
    expect(acquisitions.discoveryCalls).toEqual([]);
    expect(acquisitions.passwordCalls).toEqual([]);
    expect(acquisitions.materialCalls).toEqual([]);
  });
  it("reports automatic host identity discovery without authentication inputs", async () => {
    const acquisitions = new FakeAcquisitionService();
    const { activity, value } = bindings(acquisitions);
    const command: Extract<HostCommand, { readonly command: "trustAcquisition.hostKey.discover" }> =
      {
        command: "trustAcquisition.hostKey.discover",
        id: "discover-host-key",
        payload: {
          editor: { id: "editor", generation: 1 },
          target: {
            host: target.host,
            port: target.port,
          },
        },
        version: HOST_PROTOCOL_VERSION,
      };

    await expect(
      executeTrustAcquisitionCommand(command, "correlation-discovery", value),
    ).resolves.toEqual({
      command: command.command,
      id: command.id,
      ok: true,
      result: {
        correlationId: "correlation-discovery",
        hostKey: {
          fingerprint: target.hostKeyFingerprint,
          target: command.payload.target,
        },
      },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(acquisitions.discoveryCalls).toEqual([command.payload]);
    expect(activity).toEqual([
      {
        correlationId: "correlation-discovery",
        detail: `Discovered SSH host identity ${target.hostKeyFingerprint}; no authentication or remote template was attempted.`,
        object: "kafka-lab.example.test:22",
        operation: "Discover SSH host identity",
        outcome: "succeeded",
        severity: "info",
      },
    ]);
    expect(JSON.stringify({ activity })).not.toContain(target.password);
    expect(JSON.stringify({ activity })).not.toContain(target.username);
  });

  it("rejects the retired password-only command before invoking acquisition", async () => {
    const acquisitions = new FakeAcquisitionService();
    const { activity, value } = bindings(acquisitions);

    const response = await executeTrustAcquisitionCommand(
      passwordCommand(),
      "correlation-1",
      value,
    );

    expect(response).toMatchObject({ ok: false, error: { code: "VALIDATION" } });
    expect(response).not.toHaveProperty("result");
    expect(acquisitions.passwordCalls).toEqual([]);
    expect(activity[0]).toMatchObject({ outcome: "failed" });
    const visible = JSON.stringify({ activity, response });
    expect(visible).not.toContain("ssh-password");
    expect(visible).not.toContain(target.username);
    expect(visible).toContain("Acquire trust material");
  });

  it("passes only declared material input and reports its safe aggregate result", async () => {
    const acquisitions = new FakeAcquisitionService();
    const { activity, value } = bindings(acquisitions);
    const command: Extract<HostCommand, { readonly command: "trustAcquisition.material.fetch" }> = {
      command: "trustAcquisition.material.fetch",
      id: "fetch-material",
      payload: {
        editor: { id: "editor", generation: 1 },
        acquisitionId: "acquisition-1",
        kind: "jks",
        label: "nsp.truststore",
        target,
      },
      version: HOST_PROTOCOL_VERSION,
    };

    const response = await executeTrustAcquisitionCommand(command, "correlation-2", value);

    expect(acquisitions.materialCalls).toEqual([command.payload]);
    expect(response).toMatchObject({
      ok: true,
      result: {
        acquisition: {
          material: {
            byteCount: 4_096,
            kind: "jks",
            label: "nsp.truststore",
            templateName: "Remote trust",
          },
        },
        correlationId: "correlation-2",
      },
    });
    expect(activity[0]).toMatchObject({
      object: "kafka-lab.example.test:22",
      operation: "Fetch remote trust material",
      outcome: "succeeded",
    });
    expect(activity[0]?.detail).toContain("Remote password template Remote password");
    expect(activity[0]?.detail).toContain("trust material template Remote trust");
  });

  it("discards by opaque identifier without fabricating acquisition content", async () => {
    const acquisitions = new FakeAcquisitionService();
    const { activity, value } = bindings(acquisitions);
    const command: Extract<HostCommand, { readonly command: "trustAcquisition.discard" }> = {
      command: "trustAcquisition.discard",
      id: "discard",
      payload: { acquisitionId: "acquisition-1" },
      version: HOST_PROTOCOL_VERSION,
    };

    await expect(executeTrustAcquisitionCommand(command, "correlation-3", value)).resolves.toEqual({
      command: "trustAcquisition.discard",
      id: "discard",
      ok: true,
      result: { correlationId: "correlation-3" },
      version: HOST_PROTOCOL_VERSION,
    });
    expect(acquisitions.discardCalls).toEqual(["acquisition-1"]);
    expect(activity[0]).toMatchObject({
      operation: "Discard remote trust acquisition",
      outcome: "succeeded",
    });
  });

  it("redacts submitted SSH values from structured failure and Activity", async () => {
    const acquisitions = new FakeAcquisitionService();
    acquisitions.failure = Object.assign(new Error(`Authentication rejected ${target.password}`), {
      code: "SSH_AUTHENTICATION",
      recovery: `Check ${target.username} and ${target.password}.`,
      retryable: false,
      stage: "ssh",
      target: `${target.host}:${String(target.port)}`,
    });
    const { activity, value } = bindings(acquisitions);

    const response = await executeTrustAcquisitionCommand(
      {
        command: "trustAcquisition.material.fetch",
        id: "material-failure",
        version: HOST_PROTOCOL_VERSION,
        payload: { target, kind: "jks", label: "trust", editor: { id: "editor", generation: 1 } },
      },
      "correlation-4",
      value,
    );
    const visible = JSON.stringify({ activity, response });

    expect(response).toMatchObject({
      error: {
        code: "SSH_AUTHENTICATION",
        correlationId: "correlation-4",
        stage: "ssh",
      },
      ok: false,
    });
    expect(activity[0]).toMatchObject({
      operation: "Fetch remote trust material",
      outcome: "failed",
      severity: "error",
    });
    expect(visible).not.toContain(target.password);
    expect(visible).not.toContain(target.username);
    expect(visible).toContain("[REDACTED]");
  });

  it("does not invoke a missing acquisition owner", async () => {
    const recordActivity = vi.fn();
    const response = await executeTrustAcquisitionCommand(passwordCommand(), "correlation-5", {
      acquisitions: undefined,
      available: true,
      recordActivity,
    });

    expect(response).toMatchObject({
      error: {
        code: "BACKEND_UNAVAILABLE",
        stage: "backend",
      },
      ok: false,
    });
    expect(recordActivity).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: "Fetch remote trust password",
        outcome: "failed",
      }),
    );
  });

  it("dispatches through the application facade and clears host-owned values on shutdown", async () => {
    const acquisitions = new FakeAcquisitionService();
    const facade = createFacade(new RecordingConnectionPort(), undefined, undefined, acquisitions);

    await expect(
      facade.execute({
        command: "trustAcquisition.material.fetch",
        id: "material",
        version: HOST_PROTOCOL_VERSION,
        payload: { target, kind: "jks", label: "trust", editor: { id: "editor", generation: 1 } },
      }),
    ).resolves.toMatchObject({ ok: true });
    await facade.shutdown();

    expect(acquisitions.materialCalls).toHaveLength(1);
    expect(acquisitions.clearCalls).toBe(1);
  });
});
