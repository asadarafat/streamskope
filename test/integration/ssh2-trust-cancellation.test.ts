import { execFile } from "node:child_process";
import { promisify } from "node:util";

import { describe, expect, it } from "vitest";

import { Ssh2KafkaRemoteTrustAdapter } from "../../src/platform/node/ssh2-kafka-remote-trust-adapter";
import { startControlledSshServer } from "../support/ssh-fixture";

describe("Real SSH acquisition termination", () => {
  it("preserves cancellation and observes late errors before a real SSH handshake", async () => {
    // A subprocess makes an unobserved terminal error fail without tainting Vitest's runner.
    const script = String.raw`
      import assert from 'node:assert/strict';
      import { once } from 'node:events';
      import { createServer } from 'node:net';
      import { Ssh2KafkaSshConnector } from './src/platform/node/ssh2-kafka-remote-session.ts';
      const controller = new AbortController();
      const reason = new DOMException('Cancelled before handshake.', 'AbortError');
      let connections = 0, clientIdentification = false, peerClosed = false;
      let completeClose;
      const connectionClosed = new Promise((resolve) => { completeClose = resolve; });
      const server = createServer((socket) => {
        connections += 1;
        let identification = '';
        socket.on('data', (bytes) => {
          identification += bytes.toString('ascii');
          if (!identification.includes('\r\n')) return;
          assert.match(identification, /^SSH-2\.0-/);
          clientIdentification = true;
          // The peer supplies no header: abort only after the client has connected and written its own.
          controller.abort(reason);
        });
        socket.once('close', () => {
          peerClosed = true;
          completeClose();
        });
      });
      server.listen(0, '127.0.0.1');
      await once(server, 'listening');
      try {
        const address = server.address();
        assert(address !== null && typeof address !== 'string');
        const operation = new Ssh2KafkaSshConnector().connect({
          host: '127.0.0.1', port: address.port, username: 'fixture',
          password: 'fixture-password', hostKeyFingerprint: 'SHA256:' + 'A'.repeat(43),
        }, controller.signal);
        await assert.rejects(operation, (error) => error === reason);
        await connectionClosed;
        process.stdout.write(JSON.stringify({ connections, clientIdentification, peerClosed, cancellation: reason.name }));
      } finally {
        await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
      }
    `;
    const { stdout, stderr } = await promisify(execFile)(
      process.execPath,
      ["--unhandled-rejections=throw", "--import", "tsx", "--input-type=module", "-e", script],
      { cwd: process.cwd(), encoding: "utf8", timeout: 10_000 },
    );
    expect(stderr).toBe("");
    expect(JSON.parse(stdout)).toEqual({
      connections: 1,
      clientIdentification: true,
      peerClosed: true,
      cancellation: "AbortError",
    });
  });

  it.each(["discovery", "authentication", "password", "transfer"] as const)(
    "closes the real connection when cancelled at %s",
    async (stage) => {
      const controller = new AbortController();
      const cancel = (): void => controller.abort(new DOMException("Cancelled", "AbortError"));
      let closed!: () => void;
      const connectionClosed = new Promise<void>((resolve) => {
        closed = resolve;
      });
      const server = await startControlledSshServer({
        onConnectionClosed: () => closed(),
        ...(stage === "discovery" ? { onConnection: cancel } : {}),
        ...(stage === "authentication"
          ? { onAuthentication: cancel, hangAuthentication: true }
          : {}),
        ...(stage === "password" ? { onCommand: cancel, hangCommands: true } : {}),
        ...(stage === "transfer" ? { onSftpRead: cancel, hangSftpRead: true } : {}),
      });
      const target = {
        host: server.host,
        port: server.port,
        hostKeyFingerprint: server.fingerprint,
        username: "operator",
        password: "ssh-password",
      };
      const adapter = new Ssh2KafkaRemoteTrustAdapter();
      server.putFile("/cert.pem", new Uint8Array([1, 2, 3]));
      try {
        const operation =
          stage === "discovery"
            ? adapter.discoverHostKey({ target }, controller.signal)
            : stage === "transfer"
              ? adapter.fetchMaterial(
                  { source: "file", remotePath: "/cert.pem", maximumBytes: 10, target },
                  controller.signal,
                )
              : adapter.fetchPassword({ command: "read-password", target }, controller.signal);
        await expect(operation).rejects.toMatchObject({ name: "AbortError" });
        await connectionClosed;
        expect(server.removedPaths).toEqual([]);
        if (stage === "discovery")
          expect(server.events.filter((event) => event.startsWith("AUTHENTICATION:"))).toEqual([]);
        if (stage !== "password") expect(server.commands).toEqual([]);
      } finally {
        await server.close();
      }
    },
  );

  it.each([0, 1])("reports cleanup failure separately from command exit %s", async (exit) => {
    const server = await startControlledSshServer({
      commandExitCode: exit,
      commandOutput: "",
      materialBytes: new Uint8Array([1, 2, 3]),
      failRemove: true,
    });
    try {
      const result = new Ssh2KafkaRemoteTrustAdapter().fetchMaterial({
        command: "write /tmp/streamskope-owned.trust",
        remotePath: "/tmp/streamskope-owned.trust",
        maximumBytes: 10,
        target: {
          host: server.host,
          port: server.port,
          hostKeyFingerprint: server.fingerprint,
          username: "operator",
          password: "ssh-password",
        },
      });
      const error: unknown = await result.then(
        () => null,
        (failure: unknown) => failure,
      );
      expect(error).toBeInstanceOf(Error);
      expect(error).toMatchObject({ code: exit === 0 ? "REMOTE_CLEANUP" : "REMOTE_COMMAND" });
      expect(String(error)).not.toContain("/tmp/streamskope-owned.trust");
      expect(server.removedPaths).toEqual([]);
    } finally {
      await server.close();
    }
  });
});
