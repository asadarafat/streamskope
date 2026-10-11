import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer, request, type Server } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { connect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { afterAll, beforeAll, expect, it } from "vitest";

const execute = promisify(execFile);
const content = Buffer.from("verified packaging artifact\n");
const checksum = createHash("sha256").update(content).digest("hex");
const sockets = new Set<Socket>();
const servers: Server[] = [];
let directory: string;
let origin: string;
let secureOrigin: string;
let proxy: string;
let proxyRequests = 0;
let tunnelRequests = 0;
let originRequests = 0;

// Proxy bootstrap replaces Node's global HTTP agents. Keep it in a child process
// and resolve through the actual builder consumer, not a convenient root copy.
const probe = String.raw`
  const {createRequire} = require('node:module');
  const {join} = require('node:path');
  const {readFile} = require('node:fs/promises');
  const root = createRequire(join(process.cwd(), 'package.json'));
  const builder = createRequire(root.resolve('app-builder-lib'));
  const get = builder('@electron/get');
  const args = JSON.parse(process.argv[1]);
  const config = {
    version: '9.9.9', artifactName: 'fixture.bin', isGeneric: true,
    cacheRoot: join(args.directory, 'cache'), checksums: {'fixture.bin': args.checksum},
    mirrorOptions: {resolveAssetURL: async () => args.url},
    downloadOptions: {quiet: true, retry: {limit: 0}, timeout: {request: args.mode === 'timeout' ? 500 : 5000}}
  };
  (async () => {
    if (args.mode === 'builder') {
      const {download} = root('app-builder-lib/out/binDownload.js');
      const file = join(args.directory, 'builder-output.bin');
      await download(args.url, file, args.checksum);
      await download(args.url, file, args.checksum);
      console.log(JSON.stringify({content: await readFile(file, 'utf8')}));
    } else {
      let progress = 0;
      config.downloadOptions.getProgressCallback = async () => {progress++;};
      const file = await get.downloadArtifact(config);
      const cached = await get.downloadArtifact(config);
      console.log(JSON.stringify({content: await readFile(file, 'utf8'), cached: file === cached, progress}));
    }
  })().catch(error => {
    console.log(JSON.stringify({error: error.code || error.message, detail: error.message}));
    process.exitCode = 1;
  });
`;

async function listen(server: Server): Promise<string> {
  servers.push(server);
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing fixture address.");
  return `127.0.0.1:${address.port}`;
}

async function download(
  name: string,
  url: string,
  options: { mode?: string; checksum?: string; bypass?: boolean; trusted?: boolean } = {},
): Promise<string> {
  const result = await execute(
    process.execPath,
    [
      "-e",
      probe,
      JSON.stringify({
        directory: join(directory, name),
        url,
        checksum: options.checksum ?? checksum,
        mode: options.mode,
      }),
    ],
    {
      timeout: 10_000,
      env: {
        ...process.env,
        ELECTRON_GET_USE_PROXY: "true",
        ELECTRON_GET_NO_PROGRESS: "1",
        GLOBAL_AGENT_HTTP_PROXY: proxy,
        GLOBAL_AGENT_HTTPS_PROXY: proxy,
        GLOBAL_AGENT_NO_PROXY: options.bypass ? "127.0.0.1" : "",
        GLOBAL_AGENT_ENVIRONMENT_VARIABLE_NAMESPACE: "GLOBAL_AGENT_",
        GLOBAL_AGENT_FORCE_GLOBAL_AGENT: "true",
        ELECTRON_BUILDER_CACHE: join(directory, name, "builder-cache"),
        NODE_EXTRA_CA_CERTS: options.trusted ? join(directory, "cert.pem") : "",
        NODE_TLS_REJECT_UNAUTHORIZED: "1",
      },
    },
  );
  return result.stdout;
}

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "streamskope-download-security-"));
  origin = `http://${await listen(
    createServer((req, res) => {
      originRequests++;
      if (req.url === "/stalled") return;
      res.writeHead(200, { "content-length": content.length });
      res.end(content);
    }),
  )}`;
  await execute("openssl", [
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-days",
    "1",
    "-subj",
    "/CN=download-fixture",
    "-addext",
    "subjectAltName=IP:127.0.0.1,DNS:localhost",
    "-keyout",
    join(directory, "key.pem"),
    "-out",
    join(directory, "cert.pem"),
  ]);
  secureOrigin = `https://${await listen(
    createHttpsServer(
      {
        key: await readFile(join(directory, "key.pem")),
        cert: await readFile(join(directory, "cert.pem")),
      },
      (_req, res) => res.end(content),
    ),
  )}`;
  const proxyServer = createServer((req, res) => {
    proxyRequests++;
    const upstream = request(req.url!, { headers: req.headers }, (response) => {
      res.writeHead(response.statusCode!, response.headers);
      response.pipe(res);
    });
    upstream.on("error", () => res.destroy());
    req.pipe(upstream);
  });
  proxyServer.on("connect", (req, client, head) => {
    tunnelRequests++;
    const address = new URL(`http://${req.url}`);
    // Route only our deliberately mismatched fixture names to the owned server.
    const host = ["127.0.0.2", "mismatch.fixture.test"].includes(address.hostname)
      ? "127.0.0.1"
      : address.hostname;
    const upstream = connect({ port: Number(address.port), host, family: 4 });
    sockets.add(upstream);
    upstream.once("close", () => sockets.delete(upstream));
    upstream.on("error", () => client.destroy());
    client.on("error", () => upstream.destroy());
    client.on("close", () => upstream.destroy());
    upstream.once("connect", () => {
      client.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      upstream.write(head);
      upstream.pipe(client);
      client.pipe(upstream);
    });
  });
  proxy = `http://${await listen(proxyServer)}`;
});

afterAll(async () => {
  for (const socket of sockets) socket.destroy();
  await Promise.all(
    servers.map((server) => new Promise<void>((resolve) => server.close(() => resolve()))),
  );
  if (directory) await rm(directory, { recursive: true, force: true });
});

it("uses the reviewed builder graph without the vulnerable formatter", async () => {
  const lock = JSON.parse(await readFile("package-lock.json", "utf8")) as {
    packages: Record<string, { version: string; dev?: boolean }>;
  };
  expect(
    Object.keys(lock.packages).some((path) => /node_modules\/(?:sprintf-js|roarr)$/u.test(path)),
  ).toBe(false);
  expect(lock.packages["node_modules/global-agent"]).toMatchObject({ version: "4.1.3", dev: true });
  expect(lock.packages["node_modules/http-cache-semantics"]).toMatchObject({
    version: "4.3.0",
    dev: true,
  });
});

it("preserves checksummed builder downloads, cache reuse, proxy routing and bypass", async () => {
  const beforeProxy = proxyRequests;
  const beforeOrigin = originRequests;
  expect(
    JSON.parse(await download("builder", `${origin}/fixture.bin`, { mode: "builder" })),
  ).toEqual({ content: content.toString() });
  expect(proxyRequests).toBeGreaterThan(beforeProxy);
  expect(originRequests - beforeOrigin).toBe(1);
  const afterProxy = proxyRequests;
  const bypassed = JSON.parse(
    await download("bypass", `${origin}/fixture.bin`, { bypass: true }),
  ) as { content: string; cached: boolean; progress: number };
  expect(bypassed).toMatchObject({ content: content.toString(), cached: true });
  expect(bypassed.progress).toBeGreaterThan(0);
  expect(proxyRequests).toBe(afterProxy);
  await expect(
    download("corrupt", `${origin}/fixture.bin`, { checksum: "0".repeat(64) }),
  ).rejects.toMatchObject({ stdout: expect.stringMatching(/checksum/i) as unknown });
  await expect(download("timeout", `${origin}/stalled`, { mode: "timeout" })).rejects.toMatchObject(
    {
      stdout: expect.stringContaining("ETIMEDOUT") as unknown,
    },
  );
});

it("rejects an untrusted certificate through the proxy and accepts an explicitly trusted CA", async () => {
  const before = tunnelRequests;
  await expect(download("untrusted", `${secureOrigin}/fixture.bin`)).rejects.toMatchObject({
    stdout: expect.stringMatching(/CERT|SELF_SIGNED/u) as unknown,
  });
  const trusted = await download("trusted", `${secureOrigin}/fixture.bin`, { trusted: true }).catch(
    (error: { stdout: string; stderr: string }) => {
      throw new Error(`Trusted fixture download failed: ${error.stdout} ${error.stderr}`);
    },
  );
  expect(JSON.parse(trusted)).toMatchObject({ content: content.toString(), cached: true });
  expect(tunnelRequests - before).toBeGreaterThanOrEqual(2);
  expect(
    JSON.parse(
      await download(
        "trusted-dns",
        `${secureOrigin.replace("127.0.0.1", "localhost")}/fixture.bin`,
        { trusted: true },
      ),
    ),
  ).toMatchObject({ content: content.toString(), cached: true });
  for (const host of ["127.0.0.2", "mismatch.fixture.test"]) {
    await expect(
      download(`wrong-host-${host}`, `${secureOrigin.replace("127.0.0.1", host)}/fixture.bin`, {
        trusted: true,
      }),
    ).rejects.toMatchObject({
      stdout: expect.stringContaining("ERR_TLS_CERT_ALTNAME_INVALID") as unknown,
    });
  }
});
