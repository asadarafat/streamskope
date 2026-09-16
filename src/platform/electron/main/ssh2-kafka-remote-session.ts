import { createHash, timingSafeEqual } from "node:crypto";

import {
  Client,
  utils,
  type ClientChannel,
  type ConnectConfig,
  type SFTPWrapper,
  type Stats,
} from "ssh2";

import {
  REMOTE_TRUST_ACQUISITION_LIMITS,
  type RemoteSshEndpointInput,
  type RemoteSshTargetInput,
} from "../../../features/kafka/contracts";

import type { KafkaSshConnector, KafkaSshSession } from "./ssh2-kafka-remote-trust-adapter";
import {
  KafkaRemoteTrustTransportError,
  remoteCommandError,
  remoteTransferError,
  remoteTimeoutError,
  sshAuthenticationError,
  sshIdentityError,
  sshUnavailableError,
} from "./ssh2-remote-errors";

type NodeCallback<T> = (error: Error | null | undefined, value: T) => void;

function safeTarget(target: RemoteSshEndpointInput): string {
  return `${target.host}:${String(target.port)}`;
}

function expectedFingerprint(value: string): Buffer {
  return Buffer.from(value.slice("SHA256:".length), "base64");
}

function presentedFingerprint(key: Buffer): Buffer {
  return createHash("sha256").update(key).digest();
}

function canonicalFingerprint(key: Buffer): string {
  return `SHA256:${presentedFingerprint(key).toString("base64").replace(/=+$/u, "")}`;
}

function fingerprintMatches(key: Buffer, expected: Buffer): boolean {
  const presented = presentedFingerprint(key);
  return presented.length === expected.length && timingSafeEqual(presented, expected);
}

function abortReason(signal: AbortSignal | undefined): Error {
  return signal?.reason instanceof Error
    ? signal.reason
    : new DOMException("Cancelled.", "AbortError");
}

function isAuthenticationFailure(error: unknown): boolean {
  return error instanceof Error && "level" in error && error.level === "client-authentication";
}

function callbackOperation<T>(
  start: (callback: NodeCallback<T>) => void,
  signal?: AbortSignal,
  onAbort?: () => void,
): Promise<T> {
  if (signal?.aborted === true) {
    return Promise.reject(abortReason(signal));
  }
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (complete: () => void): void => {
      if (settled) {
        return;
      }
      settled = true;
      signal?.removeEventListener("abort", abort);
      complete();
    };
    const abort = (): void => {
      finish(() => {
        onAbort?.();
        reject(abortReason(signal));
      });
    };
    signal?.addEventListener("abort", abort, { once: true });
    start((error, value) => {
      finish(() => {
        if (error === undefined || error === null) {
          resolve(value);
        } else {
          reject(error);
        }
      });
    });
  });
}

class Ssh2KafkaSshSession implements KafkaSshSession {
  private closed = false;
  private sftp: SFTPWrapper | undefined;

  constructor(
    private readonly client: Client,
    private readonly target: string,
  ) {}

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.sftp?.end();
    this.client.end();
  }

  async execute(command: string, maximumBytes: number, signal?: AbortSignal): Promise<string> {
    const bytes = await this.executeBytes(command, maximumBytes, signal);
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    } catch {
      throw remoteCommandError(this.target);
    }
  }

  async executeBytes(
    command: string,
    maximumBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    const output = await callbackOperation<Promise<Uint8Array> | undefined>(
      (callback) => {
        this.client.exec(command, (error, channel) => {
          if (error !== undefined && error !== null) {
            callback(error, undefined);
            return;
          }
          if (signal?.aborted === true) {
            channel.close();
            callback(abortReason(signal), undefined);
            return;
          }
          callback(undefined, this.readCommandOutput(channel, maximumBytes, signal));
        });
      },
      signal,
      () => {
        this.client.destroy();
      },
    ).catch((error: unknown) => {
      if (error instanceof Error && error.name === "AbortError") {
        throw error;
      }
      throw remoteCommandError(this.target);
    });
    if (output === undefined) throw remoteCommandError(this.target);
    return output;
  }

  private readCommandOutput(
    channel: ClientChannel,
    maximumBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    return new Promise((resolve, reject) => {
      let settled = false;
      let exitCode: number | undefined;
      let exitSignal: string | undefined;
      let stderrBytes = 0;
      let stdoutBytes = 0;
      const stdout: Buffer[] = [];
      const finish = (complete: () => void): void => {
        if (settled) {
          return;
        }
        settled = true;
        signal?.removeEventListener("abort", abort);
        channel.removeAllListeners();
        channel.stderr.removeAllListeners();
        complete();
      };
      const fail = (): void => {
        finish(() => {
          channel.close();
          reject(remoteCommandError(this.target));
        });
      };
      const abort = (): void => {
        finish(() => {
          channel.close();
          reject(abortReason(signal));
        });
      };
      channel.on("data", (value: Buffer | string) => {
        const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
        stdoutBytes += chunk.length;
        if (stdoutBytes > maximumBytes) {
          fail();
          return;
        }
        stdout.push(chunk);
      });
      channel.stderr.on("data", (value: Buffer | string) => {
        const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
        stderrBytes += chunk.length;
        if (stderrBytes > REMOTE_TRUST_ACQUISITION_LIMITS.diagnosticBytes) {
          fail();
        }
      });
      channel.once("error", fail);
      channel.once("exit", (code: number | null, signalName?: string) => {
        if (code === null) {
          exitSignal = signalName ?? "remote-signal";
        } else {
          exitCode = code;
        }
      });
      channel.once("close", () => {
        if (exitCode !== 0 || exitSignal !== undefined) {
          fail();
          return;
        }
        finish(() => {
          resolve(new Uint8Array(Buffer.concat(stdout)));
        });
      });
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted === true) {
        abort();
      }
    });
  }

  async readFile(
    remotePath: string,
    maximumBytes: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    const sftp = await this.getSftp(signal);
    let handle: Buffer | undefined;
    let primaryError: Error | undefined;
    let result: Uint8Array | undefined;
    try {
      const sourceStats = await callbackOperation<Stats>(
        (callback) => {
          sftp.lstat(remotePath, callback);
        },
        signal,
        () => {
          this.client.destroy();
        },
      );
      if (!sourceStats.isFile() || sourceStats.size < 1 || sourceStats.size > maximumBytes) {
        throw remoteTransferError(this.target);
      }
      handle = await callbackOperation<Buffer>(
        (callback) => {
          sftp.open(remotePath, "r", callback);
        },
        signal,
        () => {
          this.client.destroy();
        },
      );
      const stats = await callbackOperation<Stats>(
        (callback) => {
          sftp.fstat(handle!, callback);
        },
        signal,
        () => {
          this.client.destroy();
        },
      );
      if (!stats.isFile() || stats.size < 1 || stats.size > maximumBytes) {
        throw remoteTransferError(this.target);
      }
      const bytes = Buffer.allocUnsafe(stats.size);
      let position = 0;
      while (position < bytes.length) {
        signal?.throwIfAborted();
        const chunk = await callbackOperation<number>(
          (callback) => {
            sftp.read(
              handle!,
              bytes,
              position,
              Math.min(64 * 1_024, bytes.length - position),
              position,
              (error, bytesRead) => {
                callback(error, bytesRead);
              },
            );
          },
          signal,
          () => {
            this.client.destroy();
          },
        );
        if (chunk < 1) {
          throw remoteTransferError(this.target);
        }
        position += chunk;
      }
      signal?.throwIfAborted();
      result = new Uint8Array(bytes);
    } catch (error) {
      primaryError =
        error instanceof KafkaRemoteTrustTransportError ||
        (error instanceof Error && error.name === "AbortError")
          ? error
          : remoteTransferError(this.target);
    }
    let closeError = false;
    if (handle !== undefined) {
      try {
        await callbackOperation<void>(
          (callback) => sftp.close(handle, (error) => callback(error, undefined)),
          signal,
          () => this.client.destroy(),
        );
      } catch {
        closeError = true;
      }
    }
    if (primaryError !== undefined) {
      throw primaryError;
    }
    if (closeError || result === undefined) {
      throw remoteTransferError(this.target);
    }
    return result;
  }

  async removeFile(remotePath: string, signal?: AbortSignal): Promise<void> {
    const sftp = await this.getSftp(signal);
    try {
      await callbackOperation<void>(
        (callback) => {
          sftp.unlink(remotePath, (error) => {
            callback(error, undefined);
          });
        },
        signal,
        () => {
          this.client.destroy();
        },
      );
    } catch (error) {
      if (
        error instanceof Error &&
        "code" in error &&
        (error.code === 2 || error.code === "ENOENT")
      ) {
        return;
      }
      throw error;
    }
  }

  private async getSftp(signal?: AbortSignal): Promise<SFTPWrapper> {
    if (this.sftp !== undefined) {
      return this.sftp;
    }
    try {
      this.sftp = await callbackOperation<SFTPWrapper>(
        (callback) => {
          this.client.sftp(callback);
        },
        signal,
        () => {
          this.client.destroy();
        },
      );
      return this.sftp;
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        throw error;
      }
      throw remoteTransferError(this.target);
    }
  }
}

export class Ssh2KafkaSshConnector implements KafkaSshConnector {
  discoverHostKey(target: RemoteSshEndpointInput, signal?: AbortSignal): Promise<string> {
    const client = new Client();
    const targetName = safeTarget(target);
    if (signal?.aborted === true) {
      return Promise.reject(abortReason(signal));
    }
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (complete: () => void): void => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(readyTimer);
        signal?.removeEventListener("abort", abort);
        client.removeListener("error", error);
        client.removeListener("close", close);
        client.on("error", () => undefined);
        complete();
      };
      const abort = (): void => {
        finish(() => {
          client.destroy();
          reject(abortReason(signal));
        });
      };
      const error = (): void => {
        finish(() => {
          client.destroy();
          reject(sshUnavailableError(targetName));
        });
      };
      const close = (): void => {
        finish(() => {
          reject(sshUnavailableError(targetName));
        });
      };
      const readyTimer = setTimeout(() => {
        finish(() => {
          client.destroy();
          reject(remoteTimeoutError("ssh", targetName));
        });
      }, REMOTE_TRUST_ACQUISITION_LIMITS.readyMs);
      client.once("error", error);
      client.once("close", close);
      signal?.addEventListener("abort", abort, { once: true });
      client.connect({
        authHandler: ["none"],
        host: target.host,
        hostVerifier: (key: Buffer): boolean => {
          const fingerprint = canonicalFingerprint(key);
          finish(() => {
            queueMicrotask(() => {
              client.destroy();
            });
            resolve(fingerprint);
          });
          return false;
        },
        port: target.port,
        readyTimeout: REMOTE_TRUST_ACQUISITION_LIMITS.readyMs,
        strictVendor: true,
        tryKeyboard: false,
        username: "streamskope-host-key-discovery",
      });
    });
  }

  connect(target: RemoteSshTargetInput, signal?: AbortSignal): Promise<KafkaSshSession> {
    const client = new Client();
    const targetName = safeTarget(target);
    const expected = expectedFingerprint(target.hostKeyFingerprint);
    if (signal?.aborted === true) {
      return Promise.reject(abortReason(signal));
    }
    const authentication = target.authentication ?? { mode: "password", password: target.password };
    let auth: Pick<
      ConnectConfig,
      "authHandler" | "password" | "privateKey" | "passphrase" | "agent" | "agentForward"
    >;
    if (authentication.mode === "private-key") {
      const parsed = utils.parseKey(authentication.privateKey, authentication.passphrase);
      if (parsed instanceof Error) return Promise.reject(sshAuthenticationError(targetName));
      auth = {
        authHandler: ["publickey"],
        privateKey: authentication.privateKey,
        ...(authentication.passphrase === undefined
          ? {}
          : { passphrase: authentication.passphrase }),
      };
    } else if (authentication.mode === "agent") {
      const agent = process.env.SSH_AUTH_SOCK;
      if (agent === undefined || agent.length === 0)
        return Promise.reject(sshAuthenticationError(targetName));
      auth = { authHandler: ["agent"], agent, agentForward: false };
    } else {
      auth = { authHandler: ["password"], password: authentication.password };
    }
    return new Promise((resolve, reject) => {
      let identityRejected = false;
      let settled = false;
      const finish = (complete: () => void): void => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(readyTimer);
        signal?.removeEventListener("abort", abort);
        client.removeListener("error", error);
        client.removeListener("close", close);
        complete();
      };
      const abort = (): void => {
        finish(() => {
          client.destroy();
          reject(abortReason(signal));
        });
      };
      const error = (cause: Error): void => {
        finish(() => {
          client.destroy();
          reject(
            identityRejected
              ? sshIdentityError(targetName)
              : isAuthenticationFailure(cause)
                ? sshAuthenticationError(targetName)
                : sshUnavailableError(targetName),
          );
        });
      };
      const close = (): void => {
        finish(() => {
          reject(identityRejected ? sshIdentityError(targetName) : sshUnavailableError(targetName));
        });
      };
      const readyTimer = setTimeout(() => {
        finish(() => {
          client.destroy();
          reject(remoteTimeoutError("ssh", targetName));
        });
      }, REMOTE_TRUST_ACQUISITION_LIMITS.readyMs);
      client.once("ready", () => {
        finish(() => {
          client.on("error", () => undefined);
          resolve(new Ssh2KafkaSshSession(client, targetName));
        });
      });
      client.once("error", error);
      client.once("close", close);
      signal?.addEventListener("abort", abort, { once: true });
      const config: ConnectConfig = {
        ...auth,
        host: target.host,
        hostVerifier: (key: Buffer): boolean => {
          const matches = fingerprintMatches(key, expected);
          identityRejected = !matches;
          return matches;
        },
        port: target.port,
        readyTimeout: REMOTE_TRUST_ACQUISITION_LIMITS.readyMs,
        strictVendor: true,
        tryKeyboard: false,
        username: target.username,
      };
      client.connect(config);
    });
  }
}
