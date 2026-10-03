import { spawn, execFile, type ChildProcess } from "node:child_process";
import { createHash, generateKeyPairSync, randomBytes, sign } from "node:crypto";
import { createReadStream, createWriteStream, type WriteStream } from "node:fs";
import { mkdir, mkdtemp, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import {
  createServer as createHttpServer,
  type IncomingMessage,
  type ServerResponse,
  type Server,
} from "node:http";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve, win32 } from "node:path";
import { Readable } from "node:stream";
import { finished, pipeline } from "node:stream/promises";
import type { ReadableStream } from "node:stream/web";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { Admin } from "@platformatic/kafka";

import { loadFixtureConfig } from "./kafka-fixture";
import { NativeKafkaFixtureError, type NativeFixturePhase } from "./native-fixture-error";

const run = promisify(execFile);
const KAFKA_VERSION = "4.3.1";
const ARCHIVE = `kafka_2.13-${KAFKA_VERSION}.tgz`;
// Apache release checksum: https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz.sha512
const ARCHIVE_SHA512 =
  "c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c";

async function digest(path: string): Promise<string> {
  const hash = createHash("sha512");
  for await (const chunk of createReadStream(path)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}

export function nativeArchiveExtractor(
  platform = process.platform,
  systemRoot = process.env.SystemRoot,
): string {
  if (platform !== "win32") return "tar";
  if (systemRoot === undefined || !win32.isAbsolute(systemRoot))
    throw new Error("Windows native fixture requires an absolute SystemRoot.");
  // The PATH can contain Git/MSYS GNU tar, which treats native drive-letter
  // archive arguments as remote paths. Windows ships a native bsdtar binary.
  return win32.join(systemRoot, "System32", "tar.exe");
}

async function kafkaDistribution(): Promise<string> {
  const cache = resolve(".cache/native-kafka");
  await mkdir(cache, { recursive: true });
  const archive = join(cache, ARCHIVE);
  if (!(await stat(archive).catch(() => undefined))) {
    const response = await fetch(
      `https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${ARCHIVE}`,
      {
        signal: AbortSignal.timeout(180_000),
      },
    );
    if (!response.ok || !response.body)
      throw new Error("Unable to download the pinned Kafka fixture.");
    const temporary = `${archive}.${process.pid}.partial`;
    try {
      await pipeline(
        Readable.fromWeb(response.body as ReadableStream<Uint8Array>),
        createWriteStream(temporary, { flags: "wx" }),
      );
      if ((await digest(temporary)) !== ARCHIVE_SHA512)
        throw new Error("Kafka fixture checksum mismatch.");
      await rename(temporary, archive);
    } finally {
      await rm(temporary, { force: true });
    }
  }
  if ((await digest(archive)) !== ARCHIVE_SHA512)
    throw new Error("Cached Kafka fixture checksum mismatch.");
  // Extract into a new owned directory; do not trust a previously extracted executable tree.
  const extracted = await mkdtemp(join(cache, "distribution-"));
  try {
    await run(nativeArchiveExtractor(), ["-xzf", archive, "-C", extracted], { timeout: 60_000 });
    return extracted;
  } catch (error) {
    let cleanup: "passed" | "failed" = "passed";
    try {
      await rm(extracted, { recursive: true, force: true });
    } catch {
      cleanup = "failed";
    }
    throw new NativeKafkaFixtureError("extract Kafka distribution", error, "", cleanup);
  }
}

async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  if (!address || typeof address === "string") throw new Error("Fixture port allocation failed.");
  return address.port;
}

async function stop(child: ChildProcess | undefined): Promise<void> {
  if (!child || child.pid === undefined || child.exitCode !== null || child.signalCode !== null)
    return;
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  child.kill("SIGTERM");
  await Promise.race([exited, delay(10_000, undefined, { ref: false })]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await exited;
  }
}

export async function disposeNativeFixtureResources(
  actions: readonly (() => Promise<unknown>)[],
): Promise<void> {
  const failures: unknown[] = [];
  for (const action of actions) {
    try {
      await action();
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length > 0) throw new AggregateError(failures, "Native fixture cleanup failed.");
}

/** A real JVM broker for native OS qualification, with no Docker or local-lab dependency. */
export async function startNativeKafkaFixture(
  runtime = {
    distribution: kafkaDistribution,
    directory: () => mkdtemp(join(tmpdir(), "streamskope-native-kafka-")),
    configuration: loadFixtureConfig,
  },
): Promise<{
  readonly environment: Record<string, string>;
  readonly metadata: { kafkaVersion: string; archiveSha512: string; transport: string };
  dispose(): Promise<void>;
}> {
  let extracted: string;
  try {
    extracted = await runtime.distribution();
  } catch (error) {
    if (error instanceof NativeKafkaFixtureError) throw error;
    throw new NativeKafkaFixtureError("prepare Kafka distribution", error);
  }
  let directory: string | undefined;
  let oauth: Server | undefined;
  let broker: ChildProcess | undefined;
  let admin: Admin | undefined;
  let log: WriteStream | undefined;
  let logFailure: Error | undefined;
  let disposed = false;
  let phase: NativeFixturePhase = "initialize fixture";
  const dispose = async (): Promise<void> => {
    if (disposed) return;
    await disposeNativeFixtureResources([
      (): Promise<void> => admin?.close() ?? Promise.resolve(),
      (): Promise<void> => stop(broker),
      (): Promise<void> => {
        const server = oauth;
        if (server === undefined) return Promise.resolve();
        server.closeAllConnections();
        return new Promise<void>((resolve) => server.close(() => resolve()));
      },
      (): Promise<void> => {
        const stream = log;
        if (stream === undefined) return Promise.resolve();
        stream.end();
        return finished(stream);
      },
      (): Promise<void> =>
        directory === undefined
          ? Promise.resolve()
          : rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 500 }),
      (): Promise<void> =>
        rm(extracted, { recursive: true, force: true, maxRetries: 5, retryDelay: 500 }),
    ]);
    disposed = true;
  };
  try {
    const distribution = join(extracted, `kafka_2.13-${KAFKA_VERSION}`);
    directory = await runtime.directory();
    const config = await runtime.configuration();
    const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    const jwk = {
      ...publicKey.export({ format: "jwk" }),
      kid: "native-fixture",
      alg: "RS256",
      use: "sig",
    };
    const issuer = "streamskope-native-fixture";
    const handleOAuth = async (
      request: IncomingMessage,
      response: ServerResponse,
    ): Promise<void> => {
      response.setHeader("content-type", "application/json");
      if (request.method === "GET" && request.url === "/.well-known/jwks.json") {
        response.end(JSON.stringify({ keys: [jwk] }));
        return;
      }
      if (request.method !== "POST" || request.url !== "/rest-gateway/rest/api/v1/auth/token") {
        response.writeHead(404).end("{}");
        return;
      }
      let body = "";
      for await (const chunk of request) {
        body += String(chunk);
        if (body.length > 4096) {
          response.writeHead(413).end("{}");
          return;
        }
      }
      const form = new URLSearchParams(body);
      const basic = `Basic ${Buffer.from(`${config.oauthClientId}:${config.oauthClientSecret}`).toString("base64")}`;
      const credentialsMatch =
        request.headers.authorization === basic ||
        (form.get("client_id") === config.oauthClientId &&
          form.get("client_secret") === config.oauthClientSecret);
      if (!credentialsMatch || form.get("grant_type") !== "client_credentials") {
        response.writeHead(401).end(JSON.stringify({ error: "invalid_client" }));
        return;
      }
      const now = Math.floor(Date.now() / 1000);
      const encode = (value: unknown): string =>
        Buffer.from(JSON.stringify(value)).toString("base64url");
      const unsigned = `${encode({ alg: "RS256", typ: "JWT", kid: jwk.kid })}.${encode({
        sub: config.oauthClientId,
        iss: issuer,
        aud: "streamskope-native",
        scope: config.oauthScope,
        iat: now,
        exp: now + 600,
      })}`;
      const token = `${unsigned}.${sign("RSA-SHA256", Buffer.from(unsigned), privateKey).toString("base64url")}`;
      response.end(JSON.stringify({ access_token: token, expires_in: 600, token_type: "bearer" }));
    };
    oauth = createHttpServer((request, response) => {
      void handleOAuth(request, response).catch(() => response.destroy());
    });
    log = createWriteStream(join(directory, "broker.log"), { mode: 0o600 });
    log.on("error", (error: Error) => {
      logFailure = error;
    });
    phase = "initialize OAuth";
    const oauthServer = oauth;
    await new Promise<void>((resolve, reject) => {
      oauthServer.once("error", reject);
      oauthServer.listen(0, "127.0.0.1", resolve);
    });
    const address = oauthServer.address();
    if (!address || typeof address === "string")
      throw new Error("OAuth fixture port allocation failed.");
    const oauthRoot = `http://127.0.0.1:${address.port}`;
    const ports = new Set<number>();
    while (ports.size < 3) ports.add(await availablePort());
    const [clientPort, internalPort, controllerPort] = [...ports];
    const ca = join(directory, "ca.pem");
    const keystore = join(directory, "server.p12");
    const password = randomBytes(24).toString("hex");
    phase = "generate fixture TLS key";
    await run(
      "keytool",
      [
        "-genkeypair",
        "-alias",
        "server",
        "-keyalg",
        "RSA",
        "-keysize",
        "2048",
        "-dname",
        "CN=localhost",
        "-ext",
        "SAN=DNS:localhost,IP:127.0.0.1",
        "-validity",
        "2",
        "-storetype",
        "PKCS12",
        "-keystore",
        keystore,
        "-storepass",
        password,
        "-noprompt",
      ],
      { timeout: 30_000 },
    );
    await run(
      "keytool",
      [
        "-exportcert",
        "-rfc",
        "-alias",
        "server",
        "-keystore",
        keystore,
        "-storepass",
        password,
        "-file",
        ca,
      ],
      { timeout: 30_000 },
    );
    const pathValue = (value: string): string => value.replaceAll("\\", "/");
    const properties = {
      "process.roles": "broker,controller",
      "node.id": "1",
      "controller.listener.names": "CONTROLLER",
      "controller.quorum.bootstrap.servers": `127.0.0.1:${controllerPort}`,
      "log.dirs": pathValue(join(directory, "data")),
      listeners: `CLIENT://127.0.0.1:${clientPort},INTERNAL://127.0.0.1:${internalPort},CONTROLLER://127.0.0.1:${controllerPort}`,
      "advertised.listeners": `CLIENT://127.0.0.1:${clientPort},INTERNAL://127.0.0.1:${internalPort}`,
      "listener.security.protocol.map": "CLIENT:SASL_SSL,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
      "inter.broker.listener.name": "INTERNAL",
      "num.network.threads": "2",
      "num.io.threads": "2",
      "num.partitions": "1",
      "offsets.topic.replication.factor": "1",
      "offsets.topic.num.partitions": "1",
      "transaction.state.log.replication.factor": "1",
      "transaction.state.log.min.isr": "1",
      "share.coordinator.state.topic.replication.factor": "1",
      "group.initial.rebalance.delay.ms": "0",
      "auto.create.topics.enable": "false",
      "log.segment.bytes": "16777216",
      "ssl.keystore.type": "PKCS12",
      "ssl.keystore.location": pathValue(keystore),
      "ssl.keystore.password": password,
      "ssl.key.password": password,
      "sasl.enabled.mechanisms": "OAUTHBEARER",
      "listener.name.client.oauthbearer.sasl.jaas.config":
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
      "listener.name.client.oauthbearer.sasl.server.callback.handler.class":
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler",
      "sasl.oauthbearer.jwks.endpoint.url": `${oauthRoot}/.well-known/jwks.json`,
      "sasl.oauthbearer.expected.issuer": issuer,
      "sasl.oauthbearer.expected.audience": "streamskope-native",
    };
    const settings = join(directory, "server.properties");
    await writeFile(
      settings,
      Object.entries(properties)
        .map(([key, value]) => `${key}=${value}`)
        .join("\n") + "\n",
      { mode: 0o600 },
    );
    const java = [
      "-Xms128m",
      "-Xmx384m",
      `-Dkafka.logs.dir=${join(directory, "logs")}`,
      `-Dlog4j2.configurationFile=${join(distribution, "config/log4j2.yaml")}`,
      `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=${oauthRoot}/.well-known/jwks.json`,
      "-cp",
      join(distribution, "libs/*"),
    ];
    phase = "format broker storage";
    await run(
      "java",
      [
        ...java,
        "kafka.tools.StorageTool",
        "format",
        "--standalone",
        "--cluster-id",
        randomBytes(16).toString("base64url"),
        "--config",
        settings,
      ],
      { cwd: directory, timeout: 30_000 },
    );
    phase = "start broker";
    broker = spawn("java", [...java, "kafka.Kafka", settings], {
      cwd: directory,
      stdio: ["ignore", "pipe", "pipe"],
    });
    broker.stdout?.pipe(log, { end: false });
    broker.stderr?.pipe(log, { end: false });
    let spawnError = false;
    broker.once("error", () => {
      spawnError = true;
    });
    const token = (await fetch(`${oauthRoot}/rest-gateway/rest/api/v1/auth/token`, {
      method: "POST",
      headers: {
        authorization: `Basic ${Buffer.from(`${config.oauthClientId}:${config.oauthClientSecret}`).toString("base64")}`,
      },
      body: new URLSearchParams({ grant_type: "client_credentials" }),
      signal: AbortSignal.timeout(5000),
    }).then((response) => response.json())) as { access_token: string };
    admin = new Admin({
      bootstrapBrokers: [`127.0.0.1:${clientPort}`],
      clientId: "native-recovery-fixture",
      tls: { ca: await readFile(ca), rejectUnauthorized: true },
      sasl: { mechanism: "OAUTHBEARER", token: token.access_token },
      retries: 0,
      connectTimeout: 1000,
      requestTimeout: 2000,
    });
    phase = "authenticate with TLS and OAuth";
    let ready = false;
    for (let attempt = 0; attempt < 60; attempt += 1) {
      if (logFailure !== undefined) throw logFailure;
      if (spawnError || broker.exitCode !== null || broker.signalCode !== null) break;
      try {
        await admin.listTopics();
        ready = true;
        break;
      } catch {
        await delay(1000);
      }
    }
    if (!ready) throw new Error("Native Kafka fixture did not become ready.");
    return {
      environment: {
        STREAMSKOPE_TEST_KAFKA_ENDPOINT: `127.0.0.1:${clientPort}`,
        STREAMSKOPE_TEST_OAUTH_ENDPOINT: `${oauthRoot}/rest-gateway/rest/api/v1/auth/token`,
        STREAMSKOPE_TEST_CA_PATH: ca,
      },
      metadata: {
        kafkaVersion: KAFKA_VERSION,
        archiveSha512: ARCHIVE_SHA512,
        transport: "TLS / RS256 OAUTHBEARER; loopback only",
      },
      dispose,
    };
  } catch (error: unknown) {
    // Keep no test credentials, tokens, certificates or raw broker logs in public evidence.
    const formatStderr =
      error !== null &&
      typeof error === "object" &&
      "stderr" in error &&
      typeof error.stderr === "string"
        ? error.stderr
        : "";
    const logs =
      formatStderr +
      (directory === undefined
        ? ""
        : await readFile(join(directory, "broker.log"), "utf8").catch(() => ""));
    let cleanup: "passed" | "failed" = "passed";
    try {
      await dispose();
    } catch {
      cleanup = "failed";
    }
    throw new NativeKafkaFixtureError(phase, error, logs, cleanup);
  }
}
