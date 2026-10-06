import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer as createHttpServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { connect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { expect, test, type ElectronApplication } from "@playwright/test";
import { build } from "vite";

import type { PluginNetworkTransportConfiguration } from "../../src/platform/node/plugins/network-transport";

interface NetworkFixture {
  configure(input: PluginNetworkTransportConfiguration): Promise<void>;
  close(): Promise<void>;
  fetch(
    url: string,
    cancelAfterResponse?: boolean,
  ): Promise<{
    status?: number;
    body?: string;
    location?: string | null;
    error?: string;
    name?: string;
  }>;
  defaultProxy(url: string): Promise<string>;
}

test("native plugin transport authenticates an isolated proxy, clears old auth and cancels downloads", async ({
  playwright,
}, info) => {
  test.setTimeout(120_000);
  const directory = await mkdtemp(join(tmpdir(), "streamskope-plugin-proxy-"));
  const certificatePath = join(directory, "certificate.pem");
  const keyPath = join(directory, "private.pem");
  const sockets = new Set<Socket>();
  const credentials = { username: `fixture-${randomUUID()}`, password: randomUUID() };
  const expectedAuth = `Basic ${Buffer.from(`${credentials.username}:${credentials.password}`).toString("base64")}`;
  let accepted = 0;
  let rejected = 0;
  let originReceivedAuth = false;
  let application: ElectronApplication | undefined;
  let origin: ReturnType<typeof createHttpsServer> | undefined;
  let proxy: ReturnType<typeof createHttpServer> | undefined;
  const output: string[] = [];
  try {
    await promisify(execFile)(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        keyPath,
        "-out",
        certificatePath,
        "-days",
        "1",
        "-subj",
        "/CN=127.0.0.1",
        "-addext",
        "subjectAltName=IP:127.0.0.1",
      ],
      { timeout: 15_000 },
    );
    origin = createHttpsServer(
      { cert: await readFile(certificatePath), key: await readFile(keyPath) },
      (request, response) => {
        originReceivedAuth ||=
          request.headers.authorization !== undefined ||
          request.headers["proxy-authorization"] !== undefined;
        if (request.url === "/origin-auth")
          response
            .writeHead(401, { "WWW-Authenticate": 'Basic realm="origin"' })
            .end("origin denied");
        else if (request.url === "/slow") {
          response.writeHead(200, {
            "Content-Type": "application/octet-stream",
            "X-Content-Type-Options": "nosniff",
          });
          response.flushHeaders();
          response.write(Buffer.alloc(4096, 1));
        } else if (request.url === "/redirect")
          response.writeHead(302, { Location: "https://untrusted.example/fixture" }).end();
        else response.end("native plugin package fixture");
      },
    );
    const rememberSocket = (socket: Socket): void => {
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
    };
    origin.on("connection", rememberSocket);
    await new Promise<void>((done) => origin!.listen(0, "127.0.0.1", done));
    const originAddress = origin.address();
    if (originAddress === null || typeof originAddress === "string")
      throw new Error("Origin fixture has no port.");
    const originUrl = `https://127.0.0.1:${originAddress.port}`;
    proxy = createHttpServer();
    proxy.on("connection", rememberSocket);
    proxy.on("connect", (request, client, head) => {
      if (request.headers["proxy-authorization"] !== expectedAuth) {
        rejected += 1;
        client.end(
          'HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm="plugin-fixture"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n',
        );
        return;
      }
      accepted += 1;
      const upstream = connect(originAddress.port, "127.0.0.1", () => {
        client.write("HTTP/1.1 200 Connection Established\r\n\r\n");
        if (head.length) upstream.write(head);
        client.pipe(upstream);
        upstream.pipe(client);
      });
      rememberSocket(upstream);
      upstream.on("error", () => client.destroy());
      client.on("error", () => upstream.destroy());
      client.on("close", () => upstream.destroy());
    });
    await new Promise<void>((done) => proxy!.listen(0, "127.0.0.1", done));
    const proxyAddress = proxy.address();
    if (proxyAddress === null || typeof proxyAddress === "string")
      throw new Error("Proxy fixture has no port.");
    const proxyUrl = `http://127.0.0.1:${proxyAddress.port}`;
    await build({
      configFile: false,
      root: process.cwd(),
      logLevel: "silent",
      build: {
        ssr: true,
        outDir: directory,
        emptyOutDir: false,
        minify: false,
        rollupOptions: {
          input: resolve("test/electron/plugin-network-main.ts"),
          external: ["electron"],
          output: { format: "cjs", entryFileNames: "main.cjs" },
        },
      },
    });
    const environment = Object.fromEntries(
      Object.entries(process.env).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      ),
    );
    delete environment.ELECTRON_RUN_AS_NODE;
    environment.STREAMSKOPE_PLUGIN_PROXY_TEST_CERTIFICATE = certificatePath;
    const require = createRequire(resolve("package.json"));
    application = await playwright._electron.launch({
      executablePath: require("electron") as string,
      args: [...(process.getuid?.() === 0 ? ["--no-sandbox"] : []), join(directory, "main.cjs")],
      env: environment,
    });
    application
      .process()
      .stderr?.on("data", (bytes: Buffer) => output.push(bytes.toString("utf8")));
    const app = application;
    await expect
      .poll(() =>
        app.evaluate(() =>
          Boolean(
            (globalThis as unknown as { pluginNetworkFixture?: NetworkFixture })
              .pluginNetworkFixture,
          ),
        ),
      )
      .toBe(true);
    async function configure(input: PluginNetworkTransportConfiguration): Promise<void> {
      await app.evaluate(
        async (_electron, config) =>
          (
            globalThis as unknown as { pluginNetworkFixture: NetworkFixture }
          ).pluginNetworkFixture.configure(config),
        input,
      );
    }
    async function fetchNative(path: string, cancel = false): ReturnType<NetworkFixture["fetch"]> {
      process.stdout.write(`native proxy fixture: ${path}\n`);
      const result = await app.evaluate(
        async (_electron, input) =>
          (
            globalThis as unknown as { pluginNetworkFixture: NetworkFixture }
          ).pluginNetworkFixture.fetch(input.url, input.cancel),
        { url: `${originUrl}${path}`, cancel },
      );
      process.stdout.write(`native proxy fixture result: ${result.status ?? result.name}\n`);
      return result;
    }
    const defaultProxyBefore = await app.evaluate(
      async (_electron, url) =>
        (
          globalThis as unknown as { pluginNetworkFixture: NetworkFixture }
        ).pluginNetworkFixture.defaultProxy(url),
      originUrl,
    );
    await configure({ mode: "custom", proxyUrl, credentials });
    expect(await fetchNative("/package")).toMatchObject({
      status: 200,
      body: "native plugin package fixture",
    });
    expect(accepted).toBeGreaterThan(0);
    expect(rejected).toBeGreaterThan(0);
    const acceptedBeforeWrongPassword = accepted;
    await configure({
      mode: "custom",
      proxyUrl,
      credentials: { ...credentials, password: "wrong-fixture-password" },
    });
    expect((await fetchNative("/package")).error).toMatch(/proxy.*authentication/iu);
    expect(accepted).toBe(acceptedBeforeWrongPassword);
    await configure({ mode: "custom", proxyUrl, credentials });
    const denied = await fetchNative("/origin-auth");
    expect(denied.status === 401 || denied.error !== undefined).toBe(true);
    expect(originReceivedAuth).toBe(false);
    expect(await fetchNative("/redirect")).toMatchObject({
      status: 302,
      location: "https://untrusted.example/fixture",
    });
    expect(await fetchNative("/slow", true)).toMatchObject({ name: "AbortError" });
    expect(
      await app.evaluate(
        async (_electron, url) =>
          (
            globalThis as unknown as { pluginNetworkFixture: NetworkFixture }
          ).pluginNetworkFixture.defaultProxy(url),
        originUrl,
      ),
    ).toBe(defaultProxyBefore);
    await configure({ mode: "system" });
    const acceptedBeforeSystem = accepted;
    expect(await fetchNative("/package")).toMatchObject({
      status: 200,
      body: "native plugin package fixture",
    });
    expect(accepted).toBe(acceptedBeforeSystem);
    await app.evaluate(async () =>
      (
        globalThis as unknown as { pluginNetworkFixture: NetworkFixture }
      ).pluginNetworkFixture.close(),
    );
    await info.attach("native-proxy-evidence", {
      contentType: "application/json",
      body: JSON.stringify({
        authenticatedConnects: accepted,
        rejectedChallenges: rejected,
        oldCredentialsCleared: true,
        originReceivedAuth,
        cancellation: true,
        manualRedirects: true,
        defaultSessionUnchanged: true,
        systemMode: true,
      }),
    });
  } finally {
    await info.attach("native-proxy-host", {
      contentType: "text/plain",
      body: output
        .join("")
        .replaceAll(credentials.username, "[redacted]")
        .replaceAll(credentials.password, "[redacted]"),
    });
    await info.attach("native-proxy-counts", {
      contentType: "application/json",
      body: JSON.stringify({ accepted, rejected, originReceivedAuth }),
    });
    await application?.close();
    for (const socket of sockets) socket.destroy();
    await Promise.all(
      [origin, proxy]
        .filter((server) => server !== undefined)
        .map((server) => new Promise<void>((done) => server.close(() => done()))),
    );
    await rm(directory, { recursive: true, force: true });
  }
});
