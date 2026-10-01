import { createHash, timingSafeEqual } from "node:crypto";
import { constants as fileConstants } from "node:fs";

import { Server, utils, type Connection, type SFTPWrapper } from "ssh2";

const USERNAME = Buffer.from("operator");
const PASSWORD = Buffer.from("ssh-password");
const HOST_KEY_GENERATION_ATTEMPTS = 3;
const SFTP_OPEN_READ = 1;
const SFTP_STATUS = {
  eof: 1,
  failure: 4,
  noSuchFile: 2,
  ok: 0,
} as const;

function secureEqual(value: string, expected: Buffer): boolean {
  const actual = Buffer.from(value);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export interface ControlledSshServerOptions {
  readonly port?: number;
  readonly onConnection?: () => void;
  readonly onConnectionClosed?: () => void;
  readonly onAuthentication?: () => void;
  readonly hangAuthentication?: boolean;
  readonly onCommand?: () => void;
  readonly onSftpRead?: () => void;
  readonly hangSftpRead?: boolean;
  readonly failRemove?: boolean;
  readonly commandExitCode?: number;
  readonly commandOutput?: string | Uint8Array;
  readonly fileMode?: number;
  readonly authorizedPublicKey?: string;
  readonly hangCommands?: boolean;
  readonly onSftpClose?: () => void;
  readonly hangSftpClose?: boolean;
  readonly materialBytes?: Uint8Array;
}

export interface ControlledSshServer {
  readonly commands: readonly string[];
  readonly events: readonly string[];
  readonly fingerprint: string;
  readonly host: "127.0.0.1";
  readonly port: number;
  readonly removedPaths: readonly string[];
  close(): Promise<void>;
  putFile(path: string, bytes: Uint8Array): void;
}

export async function startControlledSshServer(
  options: ControlledSshServerOptions = {},
): Promise<ControlledSshServer> {
  let hostKey: string | undefined;
  let publicKey: Buffer | undefined;
  let parsingError: Error | undefined;
  for (let attempt = 0; attempt < HOST_KEY_GENERATION_ATTEMPTS; attempt += 1) {
    const candidate = utils.generateKeyPairSync("ed25519");
    const parsed = utils.parseKey(candidate.private);
    if (parsed instanceof Error) {
      parsingError = parsed;
      continue;
    }
    hostKey = candidate.private;
    publicKey = parsed.getPublicSSH();
    break;
  }
  if (hostKey === undefined || publicKey === undefined) {
    throw new Error(
      `Controlled SSH server could not generate a parseable host key after ${String(
        HOST_KEY_GENERATION_ATTEMPTS,
      )} attempts.`,
      { cause: parsingError },
    );
  }
  const fingerprint = `SHA256:${createHash("sha256")
    .update(publicKey)
    .digest("base64")
    .replace(/=+$/u, "")}`;
  const commands: string[] = [];
  const events: string[] = [];
  const removedPaths: string[] = [];
  const files = new Map<string, Buffer>();
  const connections = new Set<Connection>();
  const server = new Server({ hostKeys: [hostKey] }, (connection) => {
    connections.add(connection);
    options.onConnection?.();
    connection.on("error", () => undefined);
    connection.on("close", () => {
      connections.delete(connection);
      options.onConnectionClosed?.();
    });
    connection.on("authentication", (context) => {
      events.push(`AUTHENTICATION:${context.method}`);
      options.onAuthentication?.();
      if (options.hangAuthentication === true) return;
      if (context.method === "publickey" && options.authorizedPublicKey !== undefined) {
        const key = utils.parseKey(options.authorizedPublicKey);
        if (
          !(key instanceof Error) &&
          secureEqual(context.username, USERNAME) &&
          context.key.data.equals(key.getPublicSSH()) &&
          (context.signature === undefined ||
            (context.blob !== undefined &&
              key.verify(context.blob, context.signature, context.hashAlgo) === true))
        ) {
          context.accept();
        } else {
          context.reject();
        }
        return;
      }
      if (
        context.method === "password" &&
        secureEqual(context.username, USERNAME) &&
        secureEqual(context.password, PASSWORD)
      ) {
        context.accept();
      } else {
        context.reject();
      }
    });
    connection.on("ready", () => {
      connection.on("session", (accept) => {
        const session = accept();
        session.on("auth-agent", (_accept, reject) => {
          events.push("AGENT_FORWARD");
          reject();
        });
        session.on("exec", (acceptExec, _rejectExec, info) => {
          commands.push(info.command);
          options.onCommand?.();
          const remoteMaterialPath = info.command.match(
            /\/tmp\/streamskope-[A-Za-z0-9-]+\.trust/u,
          )?.[0];
          if (remoteMaterialPath !== undefined && options.materialBytes !== undefined) {
            files.set(remoteMaterialPath, Buffer.from(options.materialBytes));
          }
          const channel = acceptExec();
          if (options.hangCommands === true) {
            return;
          }
          const output = options.commandOutput ?? "remote-password\n";
          if (output.length > 0) {
            channel.write(typeof output === "string" ? output : Buffer.from(output));
          }
          channel.exit(options.commandExitCode ?? 0);
          channel.end();
        });
        session.on("sftp", (acceptSftp) => {
          events.push("SFTP");
          configureSftp(acceptSftp(), files, removedPaths, events, options.fileMode, options);
        });
      });
    });
  });
  server.on("error", () => undefined);
  await new Promise<void>((resolve, reject) => {
    const error = (cause: Error): void => {
      server.removeListener("listening", listening);
      reject(cause);
    };
    const listening = (): void => {
      server.removeListener("error", error);
      resolve();
    };
    server.once("error", error);
    server.once("listening", listening);
    server.listen(options.port ?? 0, "127.0.0.1");
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Controlled SSH server did not expose a TCP address.");
  }
  return {
    close: async (): Promise<void> => {
      for (const connection of connections) {
        connection.end();
      }
      await new Promise<void>((resolve) => {
        server.close(() => {
          resolve();
        });
      });
    },
    commands,
    events,
    fingerprint,
    host: "127.0.0.1",
    port: address.port,
    putFile(path: string, bytes: Uint8Array): void {
      files.set(path, Buffer.from(bytes));
    },
    removedPaths,
  };
}

function configureSftp(
  channel: SFTPWrapper,
  files: Map<string, Buffer>,
  removedPaths: string[],
  events: string[],
  fileMode = fileConstants.S_IFREG | 0o600,
  options: Pick<
    ControlledSshServerOptions,
    "onSftpClose" | "hangSftpClose" | "onSftpRead" | "hangSftpRead" | "failRemove"
  > = {},
): void {
  const handles = new Map<number, { readonly path: string; readonly value: Buffer }>();
  let nextHandle = 0;
  channel.on("error", () => {
    events.push("SFTP_ERROR");
  });

  channel.on("OPEN", (requestId, path, flags) => {
    events.push(`OPEN:${path}:${String(flags)}:${String(files.has(path))}`);
    const value = files.get(path);
    if (value === undefined || (flags & SFTP_OPEN_READ) === 0) {
      channel.status(requestId, SFTP_STATUS.noSuchFile);
      return;
    }
    const handle = Buffer.alloc(4);
    handle.writeUInt32BE(nextHandle);
    handles.set(nextHandle, { path, value });
    nextHandle += 1;
    channel.handle(requestId, handle);
    events.push("OPEN_HANDLE");
  });
  channel.on("LSTAT", (requestId, path) => {
    events.push(`LSTAT:${path}`);
    const value = files.get(path);
    if (value === undefined) {
      channel.status(requestId, SFTP_STATUS.noSuchFile);
      return;
    }
    channel.attrs(requestId, {
      atime: 0,
      gid: 1000,
      mode: fileMode,
      mtime: 0,
      size: value.length,
      uid: 1000,
    });
  });
  channel.on("FSTAT", (requestId, handle) => {
    events.push("FSTAT");
    const opened = handle.length === 4 ? handles.get(handle.readUInt32BE(0)) : undefined;
    if (opened === undefined) {
      channel.status(requestId, SFTP_STATUS.failure);
      return;
    }
    channel.attrs(requestId, {
      atime: Math.floor(Date.now() / 1_000),
      gid: 1_000,
      mode: fileMode,
      mtime: Math.floor(Date.now() / 1_000),
      size: opened.value.length,
      uid: 1_000,
    });
  });
  channel.on("READ", (requestId, handle, offset, length) => {
    events.push(`READ:${String(offset)}:${String(length)}`);
    options.onSftpRead?.();
    if (options.hangSftpRead === true) return;
    const opened = handle.length === 4 ? handles.get(handle.readUInt32BE(0)) : undefined;
    if (opened === undefined) {
      channel.status(requestId, SFTP_STATUS.failure);
      return;
    }
    if (offset >= opened.value.length) {
      channel.status(requestId, SFTP_STATUS.eof);
      return;
    }
    channel.data(
      requestId,
      opened.value.subarray(offset, Math.min(offset + length, opened.value.length)),
    );
  });
  channel.on("CLOSE", (requestId, handle) => {
    events.push("CLOSE");
    options.onSftpClose?.();
    if (options.hangSftpClose === true) return;
    if (handle.length !== 4 || !handles.delete(handle.readUInt32BE(0))) {
      channel.status(requestId, SFTP_STATUS.failure);
      return;
    }
    channel.status(requestId, SFTP_STATUS.ok);
  });
  channel.on("REMOVE", (requestId, path) => {
    events.push(`REMOVE:${path}`);
    if (options.failRemove === true) {
      channel.status(requestId, SFTP_STATUS.failure);
      return;
    }
    if (!files.delete(path)) {
      channel.status(requestId, SFTP_STATUS.noSuchFile);
      return;
    }
    removedPaths.push(path);
    channel.status(requestId, SFTP_STATUS.ok);
  });
}
