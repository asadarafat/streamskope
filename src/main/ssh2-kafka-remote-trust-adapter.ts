import { REMOTE_TRUST_ACQUISITION_LIMITS, type RemoteSshTargetInput } from "../kafka/contracts";
import type {
  KafkaRemoteMaterialRequest,
  KafkaRemoteHostKeyRequest,
  KafkaRemotePasswordRequest,
  KafkaRemoteTrustPort,
} from "../kafka/application";

import {
  KafkaRemoteTrustTransportError,
  remoteCleanupError,
  remoteTimeoutError,
  withCleanupRisk,
} from "./ssh2-remote-errors";
import { Ssh2KafkaSshConnector } from "./ssh2-kafka-remote-session";

export interface KafkaSshSession {
  close(): void;
  execute(command: string, maximumBytes: number, signal?: AbortSignal): Promise<string>;
  executeBytes(command: string, maximumBytes: number, signal?: AbortSignal): Promise<Uint8Array>;
  readFile(remotePath: string, maximumBytes: number, signal?: AbortSignal): Promise<Uint8Array>;
  removeFile(remotePath: string, signal?: AbortSignal): Promise<void>;
}

export interface KafkaSshConnector {
  discoverHostKey(
    target: KafkaRemoteHostKeyRequest["target"],
    signal?: AbortSignal,
  ): Promise<string>;
  connect(target: RemoteSshTargetInput, signal?: AbortSignal): Promise<KafkaSshSession>;
}

export interface Ssh2KafkaRemoteTrustAdapterOptions {
  readonly connector?: KafkaSshConnector;
  readonly operationTimeoutMs?: number;
}

interface OperationDeadline {
  readonly dispose: () => void;
  readonly signal: AbortSignal;
}

function safeTarget(target: Pick<RemoteSshTargetInput, "host" | "port">): string {
  return `${target.host}:${String(target.port)}`;
}

function operationDeadline(
  callerSignal: AbortSignal | undefined,
  timeoutMs: number,
  stage: "remote-command" | "remote-transfer" | "ssh",
  target: string,
): OperationDeadline {
  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort(remoteTimeoutError(stage, target));
  }, timeoutMs);
  const cancel = (): void => {
    controller.abort(callerSignal?.reason ?? new DOMException("Cancelled.", "AbortError"));
  };
  callerSignal?.addEventListener("abort", cancel, { once: true });
  if (callerSignal?.aborted === true) {
    cancel();
  }
  return {
    dispose: (): void => {
      clearTimeout(timeout);
      callerSignal?.removeEventListener("abort", cancel);
    },
    signal: controller.signal,
  };
}

function safeOperationError(
  signal: AbortSignal,
  error: unknown,
  stage: "remote-command" | "remote-transfer" | "ssh",
  target: string,
): Error {
  if (signal.aborted && signal.reason instanceof Error) {
    return signal.reason;
  }
  if (error instanceof Error) {
    return error;
  }
  return new KafkaRemoteTrustTransportError(
    "INTERNAL",
    stage,
    "Retry the bounded remote trust operation.",
    false,
    target,
    "The remote trust operation failed without safe error detail.",
  );
}

export class Ssh2KafkaRemoteTrustAdapter implements KafkaRemoteTrustPort {
  agentStatus(): "configured" | "unavailable" {
    return process.env.SSH_AUTH_SOCK ? "configured" : "unavailable";
  }
  private readonly connector;
  private readonly operationTimeoutMs;

  constructor(options: Ssh2KafkaRemoteTrustAdapterOptions = {}) {
    this.connector = options.connector ?? new Ssh2KafkaSshConnector();
    this.operationTimeoutMs =
      options.operationTimeoutMs ?? REMOTE_TRUST_ACQUISITION_LIMITS.operationMs;
  }

  async discoverHostKey(request: KafkaRemoteHostKeyRequest, signal?: AbortSignal): Promise<string> {
    const target = safeTarget(request.target);
    const deadline = operationDeadline(signal, this.operationTimeoutMs, "ssh", target);
    try {
      return await this.connector.discoverHostKey(request.target, deadline.signal);
    } catch (error) {
      throw safeOperationError(deadline.signal, error, "ssh", target);
    } finally {
      deadline.dispose();
    }
  }

  async fetchMaterial(
    request: KafkaRemoteMaterialRequest,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    const target = safeTarget(request.target);
    const deadline = operationDeadline(signal, this.operationTimeoutMs, "remote-transfer", target);
    let session: KafkaSshSession | undefined;
    let primaryError: Error | undefined;
    let result: Uint8Array | undefined;
    try {
      session = await this.connector.connect(request.target, deadline.signal);
      if (request.source === "command") {
        result = await session.executeBytes(request.command, request.maximumBytes, deadline.signal);
      } else {
        if (request.source !== "file") {
          await session.execute(
            request.command,
            REMOTE_TRUST_ACQUISITION_LIMITS.diagnosticBytes,
            deadline.signal,
          );
        }
        result = await session.readFile(request.remotePath, request.maximumBytes, deadline.signal);
      }
    } catch (error) {
      primaryError = safeOperationError(deadline.signal, error, "remote-transfer", target);
    } finally {
      deadline.dispose();
    }
    let cleanupError: unknown;
    if (session !== undefined && request.source !== "file" && request.source !== "command") {
      const cleanupDeadline = operationDeadline(
        undefined,
        Math.min(this.operationTimeoutMs, 5_000),
        "remote-transfer",
        target,
      );
      try {
        await session.removeFile(request.remotePath, cleanupDeadline.signal);
      } catch (error) {
        cleanupError = error;
      } finally {
        cleanupDeadline.dispose();
      }
    }
    session?.close();
    if (primaryError !== undefined) {
      throw cleanupError === undefined ? primaryError : withCleanupRisk(primaryError, target);
    }
    if (cleanupError !== undefined) {
      throw remoteCleanupError(target);
    }
    if (result === undefined) {
      throw new KafkaRemoteTrustTransportError(
        "INTERNAL",
        "remote-transfer",
        "Retry the bounded remote trust operation.",
        false,
        target,
        "The remote trust operation produced no result.",
      );
    }
    return result;
  }

  async fetchPassword(request: KafkaRemotePasswordRequest, signal?: AbortSignal): Promise<string> {
    const target = safeTarget(request.target);
    const deadline = operationDeadline(signal, this.operationTimeoutMs, "remote-command", target);
    let session: KafkaSshSession | undefined;
    try {
      session = await this.connector.connect(request.target, deadline.signal);
      return await session.execute(
        request.command,
        REMOTE_TRUST_ACQUISITION_LIMITS.commandOutputBytes,
        deadline.signal,
      );
    } catch (error) {
      throw safeOperationError(deadline.signal, error, "remote-command", target);
    } finally {
      session?.close();
      deadline.dispose();
    }
  }
}
