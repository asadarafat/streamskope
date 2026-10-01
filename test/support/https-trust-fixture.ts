import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
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
  readonly privateKeyPem: string;
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
      const requestPath = join(directory, "request.csr");
      const configPath = join(directory, "ca.cnf");
      await mkdir(join(directory, "newcerts"));
      await Promise.all([
        writeFile(join(directory, "index.txt"), ""),
        writeFile(join(directory, "serial"), "1000\n"),
        writeFile(
          configPath,
          [
            "[ ca ]",
            "default_ca = CA_default",
            "[ CA_default ]",
            `dir = ${directory}`,
            "database = $dir/index.txt",
            "new_certs_dir = $dir/newcerts",
            "serial = $dir/serial",
            "default_md = sha256",
            "policy = policy_any",
            "x509_extensions = server_cert",
            "[ policy_any ]",
            "commonName = supplied",
            "[ server_cert ]",
            "basicConstraints = critical,CA:TRUE",
            "subjectAltName = DNS:localhost,IP:127.0.0.1",
            "",
          ].join("\n"),
        ),
      ]);
      await promisify(execFile)("openssl", [
        "req",
        "-new",
        "-key",
        keyPath,
        "-out",
        requestPath,
        "-subj",
        "/CN=localhost",
      ]);
      await promisify(execFile)("openssl", [
        "ca",
        "-batch",
        "-selfsign",
        "-notext",
        "-config",
        configPath,
        "-keyfile",
        keyPath,
        "-in",
        requestPath,
        "-startdate",
        "20200101000000Z",
        "-enddate",
        "20200102000000Z",
        "-out",
        certPath,
      ]);
    }
    const [caPem, privateKeyPem] = await Promise.all([
      readFile(certPath, "utf8"),
      readFile(keyPath, "utf8"),
    ]);
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
      privateKeyPem,
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
