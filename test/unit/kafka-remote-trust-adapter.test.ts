import { describe, expect, it, vi } from "vitest";

import {
  REMOTE_TRUST_ACQUISITION_LIMITS,
  type RemoteSshTargetInput,
} from "../../src/features/kafka/contracts";
import {
  Ssh2KafkaRemoteTrustAdapter,
  type KafkaSshConnector,
  type KafkaSshSession,
} from "../../src/platform/electron/main/ssh2-kafka-remote-trust-adapter";

const target = {
  host: "kafka-lab.example.test",
  hostKeyFingerprint: `SHA256:${"A".repeat(43)}`,
  password: "ssh-password",
  port: 22,
  username: "operator",
} as const satisfies RemoteSshTargetInput;

class FakeSshSession implements KafkaSshSession {
  closeCount = 0;
  executeCalls: Array<{ command: string; maximumBytes: number }> = [];
  executeError: Error | undefined;
  executeResult = "fetched-password\n";
  readCalls: Array<{ maximumBytes: number; remotePath: string }> = [];
  readError: Error | undefined;
  readResult = new Uint8Array([1, 2, 3]);
  removeCalls: string[] = [];
  removeError: Error | undefined;
  removeWaitForAbort = false;
  waitForAbort = false;

  close(): void {
    this.closeCount += 1;
  }

  executeBytes(command: string, maximumBytes: number, signal?: AbortSignal): Promise<Uint8Array> {
    return this.execute(command, maximumBytes, signal).then((output) =>
      new TextEncoder().encode(output),
    );
  }

  execute(command: string, maximumBytes: number, signal?: AbortSignal): Promise<string> {
    this.executeCalls.push({ command, maximumBytes });
    if (this.waitForAbort) {
      return new Promise((_resolve, reject) => {
        const abort = (): void => {
          reject(
            signal?.reason instanceof Error
              ? signal.reason
              : new DOMException("Cancelled.", "AbortError"),
          );
        };
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted === true) {
          abort();
        }
      });
    }
    return this.executeError === undefined
      ? Promise.resolve(this.executeResult)
      : Promise.reject(this.executeError);
  }

  readFile(remotePath: string, maximumBytes: number, signal?: AbortSignal): Promise<Uint8Array> {
    signal?.throwIfAborted();
    this.readCalls.push({ maximumBytes, remotePath });
    return this.readError === undefined
      ? Promise.resolve(this.readResult.slice())
      : Promise.reject(this.readError);
  }

  removeFile(remotePath: string, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    this.removeCalls.push(remotePath);
    if (this.removeWaitForAbort) {
      return new Promise((_resolve, reject) => {
        const abort = (): void => {
          reject(
            signal?.reason instanceof Error
              ? signal.reason
              : new DOMException("Cancelled.", "AbortError"),
          );
        };
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted === true) {
          abort();
        }
      });
    }
    return this.removeError === undefined ? Promise.resolve() : Promise.reject(this.removeError);
  }
}

class FakeSshConnector implements KafkaSshConnector {
  connectCalls: RemoteSshTargetInput[] = [];
  connectError: Error | undefined;

  constructor(readonly session: FakeSshSession) {}

  discoverHostKey(): Promise<string> {
    return Promise.resolve(target.hostKeyFingerprint);
  }

  connect(targetInput: RemoteSshTargetInput, signal?: AbortSignal): Promise<KafkaSshSession> {
    signal?.throwIfAborted();
    this.connectCalls.push(targetInput);
    return this.connectError === undefined
      ? Promise.resolve(this.session)
      : Promise.reject(this.connectError);
  }
}

function remoteFailure(
  code:
    | "REMOTE_COMMAND"
    | "REMOTE_TRANSFER"
    | "SSH_AUTHENTICATION"
    | "SSH_IDENTITY"
    | "SSH_UNREACHABLE",
  stage: "remote-command" | "remote-transfer" | "ssh",
): Error {
  return Object.assign(new Error("unsafe upstream text"), {
    code,
    recovery: "Use the safe recovery.",
    retryable: false,
    stage,
    target: "kafka-lab.example.test:22",
  });
}

describe("SSH2 Kafka remote-trust adapter", () => {
  it("reports only agent configuration, never its path or authenticated availability", () => {
    try {
      vi.stubEnv("SSH_AUTH_SOCK", undefined);
      expect(new Ssh2KafkaRemoteTrustAdapter().agentStatus()).toBe("unavailable");
      vi.stubEnv("SSH_AUTH_SOCK", "/private/agent-sentinel");
      expect(new Ssh2KafkaRemoteTrustAdapter().agentStatus()).toBe("configured");
    } finally {
      vi.unstubAllEnvs();
    }
  });
  it("executes one bounded password command and closes the session", async () => {
    const session = new FakeSshSession();
    const connector = new FakeSshConnector(session);
    const adapter = new Ssh2KafkaRemoteTrustAdapter({ connector });

    await expect(adapter.fetchPassword({ command: "fetch-password", target })).resolves.toBe(
      "fetched-password\n",
    );

    expect(connector.connectCalls).toEqual([target]);
    expect(session.executeCalls).toEqual([
      {
        command: "fetch-password",
        maximumBytes: REMOTE_TRUST_ACQUISITION_LIMITS.commandOutputBytes,
      },
    ]);
    expect(session.closeCount).toBe(1);
  });

  it("executes, reads, removes, and closes one material operation in order", async () => {
    const session = new FakeSshSession();
    const connector = new FakeSshConnector(session);
    const adapter = new Ssh2KafkaRemoteTrustAdapter({ connector });

    await expect(
      adapter.fetchMaterial({
        command: "copy source '/tmp/streamskope-id.trust'",
        maximumBytes: REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
        remotePath: "/tmp/streamskope-id.trust",
        target,
      }),
    ).resolves.toEqual(new Uint8Array([1, 2, 3]));

    expect(session.executeCalls).toEqual([
      {
        command: "copy source '/tmp/streamskope-id.trust'",
        maximumBytes: REMOTE_TRUST_ACQUISITION_LIMITS.diagnosticBytes,
      },
    ]);
    expect(session.readCalls).toEqual([
      {
        maximumBytes: REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
        remotePath: "/tmp/streamskope-id.trust",
      },
    ]);
    expect(session.removeCalls).toEqual(["/tmp/streamskope-id.trust"]);
    expect(session.closeCount).toBe(1);
  });

  it("attempts cleanup after a primary failure and never replaces it with raw cleanup text", async () => {
    const session = new FakeSshSession();
    session.executeError = remoteFailure("REMOTE_COMMAND", "remote-command");
    session.removeError = new Error("unsafe path and server output");
    const adapter = new Ssh2KafkaRemoteTrustAdapter({
      connector: new FakeSshConnector(session),
    });

    let failure: unknown;
    try {
      await adapter.fetchMaterial({
        command: "failing-command",
        maximumBytes: 8_192,
        remotePath: "/tmp/streamskope-id.trust",
        target,
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).toMatchObject({
      code: "REMOTE_COMMAND",
      stage: "remote-command",
    });
    expect(
      failure instanceof Error && "recovery" in failure && typeof failure.recovery === "string"
        ? failure.recovery
        : "",
    ).toContain("cleanup");
    expect(session.removeCalls).toEqual(["/tmp/streamskope-id.trust"]);
    expect(session.closeCount).toBe(1);
  });

  it("reports cleanup failure when the primary material operation succeeded", async () => {
    const session = new FakeSshSession();
    session.removeError = new Error("unsafe cleanup output");
    const adapter = new Ssh2KafkaRemoteTrustAdapter({
      connector: new FakeSshConnector(session),
    });

    await expect(
      adapter.fetchMaterial({
        command: "copy",
        maximumBytes: 8_192,
        remotePath: "/tmp/streamskope-id.trust",
        target,
      }),
    ).rejects.toMatchObject({
      code: "REMOTE_CLEANUP",
      stage: "remote-transfer",
      target: "kafka-lab.example.test:22",
    });
    expect(session.closeCount).toBe(1);
  });

  it(
    "bounds remote cleanup independently and still closes the session",
    { timeout: 250 },
    async () => {
      const session = new FakeSshSession();
      session.removeWaitForAbort = true;
      const adapter = new Ssh2KafkaRemoteTrustAdapter({
        connector: new FakeSshConnector(session),
        operationTimeoutMs: 5,
      });

      await expect(
        adapter.fetchMaterial({
          command: "copy",
          maximumBytes: 8_192,
          remotePath: "/tmp/streamskope-id.trust",
          target,
        }),
      ).rejects.toMatchObject({
        code: "REMOTE_CLEANUP",
        stage: "remote-transfer",
      });
      expect(session.removeCalls).toEqual(["/tmp/streamskope-id.trust"]);
      expect(session.closeCount).toBe(1);
    },
  );

  it("distinguishes operation timeout from caller cancellation and closes late work", async () => {
    const timedSession = new FakeSshSession();
    timedSession.waitForAbort = true;
    const timedAdapter = new Ssh2KafkaRemoteTrustAdapter({
      connector: new FakeSshConnector(timedSession),
      operationTimeoutMs: 5,
    });

    await expect(timedAdapter.fetchPassword({ command: "wait", target })).rejects.toMatchObject({
      code: "TIMEOUT",
      stage: "remote-command",
    });
    expect(timedSession.closeCount).toBe(1);

    const cancelledSession = new FakeSshSession();
    cancelledSession.waitForAbort = true;
    const cancelledAdapter = new Ssh2KafkaRemoteTrustAdapter({
      connector: new FakeSshConnector(cancelledSession),
    });
    const controller = new AbortController();
    const operation = cancelledAdapter.fetchPassword(
      { command: "wait", target },
      controller.signal,
    );
    controller.abort(new DOMException("Cancelled.", "AbortError"));

    await expect(operation).rejects.toMatchObject({ name: "AbortError" });
    expect(cancelledSession.closeCount).toBe(1);
  });

  it("closes nothing when SSH identity or authentication fails before a session exists", async () => {
    for (const failure of [
      remoteFailure("SSH_IDENTITY", "ssh"),
      remoteFailure("SSH_AUTHENTICATION", "ssh"),
      remoteFailure("SSH_UNREACHABLE", "ssh"),
    ]) {
      const session = new FakeSshSession();
      const connector = new FakeSshConnector(session);
      connector.connectError = failure;
      const adapter = new Ssh2KafkaRemoteTrustAdapter({ connector });

      await expect(adapter.fetchPassword({ command: "never-run", target })).rejects.toBe(failure);
      expect(session.executeCalls).toHaveLength(0);
      expect(session.closeCount).toBe(0);
    }
  });
});
