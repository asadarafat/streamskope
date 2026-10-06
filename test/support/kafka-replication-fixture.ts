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
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
  if (address === null || typeof address === "string") throw new Error("No fixture port.");
  return address.port;
}

/** Three disposable KRaft nodes: stopping one leaves a real controller quorum. */
export async function startReplicationFixture(): Promise<{
  readonly admin: Admin;
  readonly connection: SecureConnectionInput;
  stopBroker(id: number): Promise<void>;
  startBroker(id: number): Promise<void>;
  dispose(): Promise<void>;
}> {
  if (process.platform !== "linux")
    throw new Error("The isolated replication fixture requires Linux Docker host networking.");
  const topology = await readFile("aio-kafka/topology.clab.yml", "utf8");
  const image = /image:\s*(apache\/kafka:[^\s]+)/u.exec(topology)?.[1];
  if (!image?.includes("@sha256:")) throw new Error("Kafka fixture image must be pinned.");
  const ports: { client: number; controller: number }[] = [];
  // Sequential reservations avoid accidentally recycling one ephemeral port in this fixture.
  const reserved = new Set<number>();
  while (reserved.size < 6) reserved.add(await availablePort());
  const allocated = [...reserved];
  for (let node = 0; node < 3; node++)
    ports.push({ client: allocated[node * 2]!, controller: allocated[node * 2 + 1]! });
  const prefix = `streamskope-replication-${randomUUID()}`;
  const names = ports.map((_, index) => `${prefix}-${index + 1}`);
  const clusterId = randomBytes(16).toString("base64url");
  const quorum = ports.map((p, index) => `${index + 1}@127.0.0.1:${p.controller}`).join(",");
  const admin = new Admin({
    bootstrapBrokers: ports.map((p) => `127.0.0.1:${p.client}`),
    clientId: "streamskope-replication-qualification",
    retries: 0,
    connectTimeout: 1_000,
    requestTimeout: 5_000,
  });
  const created = new Set<string>();
  let disposed = false;
  const dispose = async (): Promise<void> => {
    if (disposed) return;
    disposed = true;
    const results = await Promise.allSettled([
      admin.close(),
      ...[...created].map((name) =>
        execute("docker", ["rm", "--force", name], { timeout: 30_000 }),
      ),
    ]);
    if (results.some((result) => result.status === "rejected"))
      throw new Error("The isolated replication fixture cleanup did not complete.");
  };
  const brokerName = (id: number): string => {
    const name = names[id - 1];
    if (!name) throw new Error("Unknown replication fixture broker.");
    return name;
  };
  try {
    for (const [index, p] of ports.entries()) {
      const environment: Record<string, string> = {
        CLUSTER_ID: clusterId,
        KAFKA_PROCESS_ROLES: "broker,controller",
        KAFKA_NODE_ID: String(index + 1),
        KAFKA_CONTROLLER_QUORUM_VOTERS: quorum,
        KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
        KAFKA_LISTENERS: `CLIENT://127.0.0.1:${p.client},CONTROLLER://127.0.0.1:${p.controller}`,
        KAFKA_ADVERTISED_LISTENERS: `CLIENT://127.0.0.1:${p.client}`,
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CLIENT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        KAFKA_INTER_BROKER_LISTENER_NAME: "CLIENT",
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3",
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3",
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "2",
        KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false",
        KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
        KAFKA_REPLICA_LAG_TIME_MAX_MS: "3000",
        KAFKA_BROKER_SESSION_TIMEOUT_MS: "6000",
        KAFKA_HEAP_OPTS: "-Xms128m -Xmx256m",
        // These few test records do not need Kafka's default 128 MiB cleaner map.
        // Bound it explicitly so three nodes can start within their fixture heaps.
        KAFKA_LOG_CLEANER_DEDUPE_BUFFER_SIZE: String(8 * 1024 * 1024),
        KAFKA_NUM_NETWORK_THREADS: "2",
        KAFKA_NUM_IO_THREADS: "2",
      };
      // No --rm: restart must retain this node's log and identity until final cleanup.
      created.add(names[index]!);
      await execute(
        "docker",
        [
          "run",
          "--detach",
          "--name",
          names[index]!,
          "--network",
          "host",
          "--memory",
          "768m",
          ...Object.entries(environment).flatMap(([key, value]) => ["--env", `${key}=${value}`]),
          image,
        ],
        { timeout: 30_000 },
      );
    }
    let ready = false;
    for (let attempt = 0; attempt < 60; attempt++) {
      if (attempt % 5 === 0) {
        for (const name of created) {
          const { stdout } = await execute(
            "docker",
            ["inspect", "--format", "{{.State.Running}}", name],
            { timeout: 5_000 },
          );
          if (stdout.trim() !== "true") {
            const logs = await execute("docker", ["logs", "--tail", "60", name], {
              timeout: 5_000,
            });
            throw new Error(
              `An isolated replication node exited during startup. ${logs.stdout}${logs.stderr}`,
            );
          }
        }
      }
      try {
        const metadata = await admin.metadata({ forceUpdate: true });
        if (metadata.brokers.size === 3) {
          ready = true;
          break;
        }
      } catch {
        // Startup includes election and broker registration; retry within a fixed bound.
      }
      await delay(1_000);
    }
    if (!ready) throw new Error("The three replication fixture brokers did not become ready.");
    return {
      admin,
      connection: {
        name: "Isolated replication fixture",
        brokers: ports.map((p) => `127.0.0.1:${p.client}`),
        tls: { enabled: false },
      },
      async stopBroker(id): Promise<void> {
        await execute("docker", ["stop", "--time", "10", brokerName(id)], { timeout: 30_000 });
      },
      async startBroker(id): Promise<void> {
        await execute("docker", ["start", brokerName(id)], { timeout: 30_000 });
      },
      dispose,
    };
  } catch (error) {
    await dispose();
    throw error;
  }
}
