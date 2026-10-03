import { execFile } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { Admin } from "@platformatic/kafka";

import type { SecureConnectionInput } from "../../src/features/kafka/contracts";

const execute = promisify(execFile);
async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error === undefined ? resolve() : reject(error))),
  );
  if (address === null || typeof address === "string")
    throw new Error("No fixture port allocated.");
  return address.port;
}

/** An isolated Linux-only real broker; never changes the developer's running Kafka. */
export async function startAuthorizationFixture(): Promise<{
  readonly admin: Admin;
  readonly connection: SecureConnectionInput;
  dispose(): Promise<void>;
}> {
  if (process.platform !== "linux")
    throw new Error("The local Kafka authorization fixture requires Linux Docker host networking.");
  const [clientPort, adminPort, controllerPort] = await Promise.all([
    availablePort(),
    availablePort(),
    availablePort(),
  ]);
  const name = `streamskope-acl-qualification-${randomUUID()}`;
  const password = randomBytes(24).toString("hex");
  const topology = await readFile("aio-kafka/topology.clab.yml", "utf8");
  const image = /image:\s*(apache\/kafka:[^\s]+)/u.exec(topology)?.[1];
  if (image === undefined || !image.includes("@sha256:"))
    throw new Error("The Kafka fixture image must be pinned.");
  const environment: Record<string, string> = {
    CLUSTER_ID: randomBytes(16).toString("base64url"),
    KAFKA_PROCESS_ROLES: "broker,controller",
    KAFKA_NODE_ID: "1",
    KAFKA_CONTROLLER_QUORUM_VOTERS: `1@127.0.0.1:${controllerPort}`,
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
    KAFKA_LISTENERS: `CLIENT://127.0.0.1:${clientPort},ADMIN://127.0.0.1:${adminPort},CONTROLLER://127.0.0.1:${controllerPort}`,
    KAFKA_ADVERTISED_LISTENERS: `CLIENT://127.0.0.1:${clientPort},ADMIN://127.0.0.1:${adminPort}`,
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
      "CLIENT:PLAINTEXT,ADMIN:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
    KAFKA_INTER_BROKER_LISTENER_NAME: "ADMIN",
    KAFKA_SASL_ENABLED_MECHANISMS: "PLAIN",
    KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: "PLAIN",
    KAFKA_LISTENER_NAME_ADMIN_PLAIN_SASL_JAAS_CONFIG: `org.apache.kafka.common.security.plain.PlainLoginModule required username="fixtureadmin" password="${password}" user_fixtureadmin="${password}";`,
    KAFKA_AUTHORIZER_CLASS_NAME: "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
    KAFKA_SUPER_USERS: "User:fixtureadmin",
    KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "true",
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false",
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
    KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m",
    KAFKA_NUM_NETWORK_THREADS: "2",
    KAFKA_NUM_IO_THREADS: "2",
  };
  const admin = new Admin({
    bootstrapBrokers: [`127.0.0.1:${adminPort}`],
    clientId: "streamskope-permission-qualification",
    sasl: { mechanism: "PLAIN", username: "fixtureadmin", password },
    retries: 0,
    connectTimeout: 1_000,
    requestTimeout: 2_000,
  });
  const dispose = async (): Promise<void> => {
    try {
      await admin.close();
    } finally {
      await execute("docker", ["rm", "--force", name], { timeout: 30_000 });
    }
  };
  try {
    await execute(
      "docker",
      [
        "run",
        "--detach",
        "--rm",
        "--name",
        name,
        "--network",
        "host",
        ...Object.entries(environment).flatMap(([key, value]) => ["--env", `${key}=${value}`]),
        image,
      ],
      { timeout: 30_000 },
    );
    let ready = false;
    for (let attempt = 0; attempt < 60; attempt += 1) {
      try {
        await admin.listTopics();
        ready = true;
        break;
      } catch {
        await delay(1_000);
      }
    }
    if (!ready) throw new Error("The authorization fixture did not become ready.");
    return {
      admin,
      connection: {
        name: "Isolated authorization fixture",
        brokers: [`127.0.0.1:${clientPort}`],
        tls: { enabled: false },
      },
      dispose,
    };
  } catch {
    await dispose();
    // Docker command errors can include environment arguments, so never expose them.
    throw new Error(
      "Could not start the isolated Kafka authorization fixture; no existing fixture was modified.",
    );
  }
}
