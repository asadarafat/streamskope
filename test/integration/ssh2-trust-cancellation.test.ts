import { describe, expect, it } from "vitest";

import { Ssh2KafkaRemoteTrustAdapter } from "../../src/main/ssh2-kafka-remote-trust-adapter";
import { startControlledSshServer } from "../support/ssh-fixture";

describe("Real SSH acquisition termination", () => {
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
