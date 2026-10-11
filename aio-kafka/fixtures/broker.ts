import { generateKeyPairSync, randomBytes, sign } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import {
  createServer as createHttpServer,
  type IncomingMessage,
  type ServerResponse,
  type Server,
} from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import { Admin } from "@platformatic/kafka";

import { loadFixtureConfig } from "./development";
import { KafkaFixtureError, type KafkaFixturePhase } from "./error";
import {
  createKafkaAuthenticationMaterials,
  type KafkaAuthenticationFixture,
} from "./authentication";
import { createContainerBrokerRuntime } from "./container-runtime";
import { disposeFixtureResources } from "./resources";
import type { BrokerRuntime } from "./runtime";
import { loopbackPorts } from "./loopback";

/** Isolated AIO Docker broker with shared TLS/OAuth configuration. */
export async function startKafkaFixture(
  suppliedRuntime:
    | {
        prepare(): Promise<BrokerRuntime>;
        directory(): Promise<string>;
        configuration: typeof loadFixtureConfig;
      }
    | undefined = undefined,
  options: {
    readonly authenticationMatrix?: boolean;
  } = {},
): Promise<{
  readonly authentication?: KafkaAuthenticationFixture;
  readonly environment: Record<string, string>;
  readonly internalBroker: string;
  readonly metadata: BrokerRuntime["metadata"] & { readonly transport: string };
  dispose(): Promise<void>;
}> {
  const runtime = suppliedRuntime ?? {
    prepare: createContainerBrokerRuntime,
    directory: (): Promise<string> => mkdtemp(join(tmpdir(), "streamskope-aio-kafka-")),
    configuration: loadFixtureConfig,
  };
  let driver: BrokerRuntime;
  try {
    driver = await runtime.prepare();
  } catch (error) {
    if (error instanceof KafkaFixtureError) throw error;
    throw new KafkaFixtureError("prepare broker runtime", error);
  }
  let directory: string | undefined;
  let oauth: Server | undefined;
  let admin: Admin | undefined;
  let disposed = false;
  let brokerRemoved = false;
  let phase: KafkaFixturePhase = "initialize fixture";
  const dispose = async (): Promise<void> => {
    if (disposed) return;
    await disposeFixtureResources([
      (): Promise<void> => admin?.close() ?? Promise.resolve(),
      async (): Promise<void> => {
        await driver.dispose();
        brokerRemoved = true;
      },
      (): Promise<void> => {
        const server = oauth;
        if (server === undefined) return Promise.resolve();
        server.closeAllConnections();
        return new Promise<void>((resolve) => server.close(() => resolve()));
      },
      async (): Promise<void> => {
        if (!brokerRemoved)
          throw new Error("AIO Kafka broker removal is unconfirmed; retain its directory.");
        if (directory !== undefined)
          await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 500 });
      },
    ]);
    disposed = true;
  };
  try {
    directory = await runtime.directory();
    const config = await runtime.configuration();
    const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    const jwk = {
      ...publicKey.export({ format: "jwk" }),
      kid: "aio-fixture",
      alg: "RS256",
      use: "sig",
    };
    const issuer = "streamskope-aio-fixture";
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
        aud: "streamskope-aio",
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
    const ports = await loopbackPorts(options.authenticationMatrix ? 6 : 3);
    const [
      clientPort,
      internalPort,
      controllerPort,
      mutualTlsPort,
      mutualSaslPort,
      saslPlaintextPort,
    ] = ports;
    const ca = join(directory, "ca.pem");
    const keystore = join(directory, "server.p12");
    const password = randomBytes(24).toString("hex");
    phase = "generate fixture TLS key";
    await driver.tool(
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
      { cwd: directory, timeout: 30_000 },
    );
    await driver.tool(
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
      { cwd: directory, timeout: 30_000 },
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
      "sasl.oauthbearer.expected.audience": "streamskope-aio",
    };
    const authentication = options.authenticationMatrix
      ? await createKafkaAuthenticationMaterials({
          directory,
          tlsPort: clientPort!,
          mutualTlsPort: mutualTlsPort!,
          mutualSaslPort: mutualSaslPort!,
          saslPlaintextPort: saslPlaintextPort!,
          brokerCaPath: ca,
          execute: (command, args) =>
            driver.tool(command, args, { cwd: directory!, timeout: 30_000 }),
        })
      : undefined;
    if (authentication !== undefined) {
      Object.assign(properties, authentication.properties, {
        listeners: `${properties.listeners},${authentication.listeners}`,
        "advertised.listeners": `${properties["advertised.listeners"]},${authentication.listeners}`,
        "listener.security.protocol.map": `${properties["listener.security.protocol.map"]},MTLS:SSL,MUTUAL:SASL_SSL,CLEAR:SASL_PLAINTEXT`,
      });
    }
    const settings = join(directory, "server.properties");
    await writeFile(
      settings,
      Object.entries(properties)
        .map(([key, value]) => `${key}=${value}`)
        .join("\n") + "\n",
      { mode: 0o600 },
    );
    phase = "start broker";
    await driver.start({
      directory,
      oauthRoot,
      clusterId: randomBytes(16).toString("base64url"),
      storageArguments: authentication?.storageArguments ?? [],
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
      clientId: "aio-kafka-readiness",
      tls: { ca: await readFile(ca), rejectUnauthorized: true },
      sasl: { mechanism: "OAUTHBEARER", token: token.access_token },
      retries: 0,
      connectTimeout: 1000,
      requestTimeout: 2000,
    });
    phase = "authenticate with TLS and OAuth";
    let ready = false;
    for (let attempt = 0; attempt < 60; attempt += 1) {
      if (await driver.exited()) break;
      try {
        await admin.listTopics();
        ready = true;
        break;
      } catch {
        await delay(1000);
      }
    }
    if (!ready) throw new Error("AIO Kafka fixture did not become ready.");
    return {
      ...(authentication === undefined ? {} : { authentication: authentication.fixture }),
      internalBroker: `127.0.0.1:${internalPort}`,
      environment: {
        STREAMSKOPE_TEST_KAFKA_ENDPOINT: `127.0.0.1:${clientPort}`,
        STREAMSKOPE_TEST_OAUTH_ENDPOINT: `${oauthRoot}/rest-gateway/rest/api/v1/auth/token`,
        STREAMSKOPE_TEST_CA_PATH: ca,
      },
      metadata: {
        ...driver.metadata,
        transport: options.authenticationMatrix
          ? "TLS, mTLS and explicit plaintext / PLAIN, SCRAM and RS256 OAUTHBEARER; loopback only"
          : "TLS / RS256 OAUTHBEARER; loopback only",
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
    const logs = formatStderr + (await driver.logs());
    let cleanup: "passed" | "failed" = "passed";
    try {
      await dispose();
    } catch {
      cleanup = "failed";
    }
    throw new KafkaFixtureError(phase, error, logs, cleanup);
  }
}
