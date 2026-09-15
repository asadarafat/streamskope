import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { createServer } from "node:https";
import type { Duplex } from "node:stream";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

export async function createHttpsTrustFixture(
  handler: (request: IncomingMessage, response: ServerResponse) => void,
  certificate: "valid" | "expired" | "hostname-mismatch" = "valid",
): Promise<{
  readonly origin: string;
  readonly caPem: string;
  readonly pkcs12: Uint8Array;
  readonly requests: readonly {
    readonly url: string;
    readonly authorization: string | undefined;
  }[];
  readonly sockets: ReadonlySet<Duplex>;
  close(): Promise<void>;
}> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-https-test-"));
  try {
    const keyPath = join(directory, "key.pem");
    const certPath = join(directory, "ca.pem");
    await promisify(execFile)("openssl", [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      keyPath,
      "-out",
      certPath,
      "-days",
      "1",
      "-subj",
      "/CN=localhost",
      "-addext",
      certificate === "hostname-mismatch"
        ? "subjectAltName=DNS:unrelated.invalid"
        : "subjectAltName=DNS:localhost,IP:127.0.0.1",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
    ]);
    if (certificate === "expired") {
      await promisify(execFile)("openssl", [
        "x509",
        "-in",
        certPath,
        "-signkey",
        keyPath,
        "-days",
        "-1",
        "-out",
        certPath,
      ]);
    }
    const caPem = await readFile(certPath, "utf8");
    const { stdout: pkcs12 } = await promisify(execFile)(
      "openssl",
      ["pkcs12", "-export", "-nokeys", "-in", certPath, "-passout", "pass:password"],
      { encoding: "buffer" },
    );
    const sockets = new Set<Duplex>();
    const requests: { url: string; authorization: string | undefined }[] = [];
    const server = createServer(
      { key: await readFile(keyPath), cert: caPem },
      (request, response) => {
        requests.push({ url: request.url ?? "", authorization: request.headers.authorization });
        handler(request, response);
      },
    );
    server.on("connection", (socket) => {
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (address === null || typeof address === "string")
      throw new Error("HTTPS fixture has no port");
    return {
      origin: `https://127.0.0.1:${address.port}`,
      caPem,
      pkcs12,
      requests,
      sockets,
      async close(): Promise<void> {
        for (const socket of sockets) socket.destroy();
        await new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        );
        await rm(directory, { recursive: true, force: true });
      },
    };
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}
