import { execFile } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { Admin } from "@platformatic/kafka";

import type { SecureConnectionInput } from "../../src/features/kafka/contracts";

import { FixtureContainerOwner } from "./container-owner";
import { kafkaPin } from "./pins";
import { loopbackPorts } from "./loopback";

const execute = promisify(execFile);

/** Three disposable KRaft nodes: stopping one leaves a real controller quorum. */
export async function startReplicationFixture(jmx?: {
  readonly agent: string;
  readonly configuration: string;
}): Promise<{
  readonly metricsInstances: readonly string[];
  readonly admin: Admin;
  readonly connection: SecureConnectionInput;
  stopBroker(id: number): Promise<void>;
  startBroker(id: number): Promise<void>;
  dispose(): Promise<void>;
}> {
  if (process.platform !== "linux")
    throw new Error("The isolated replication fixture requires Linux Docker host networking.");
  const { image } = await kafkaPin();
  const owner = new FixtureContainerOwner();
  const identities = new Map<string, string>();
  const ports: { client: number; controller: number }[] = [];
  const allocated = await loopbackPorts(jmx ? 9 : 6);
  for (let node = 0; node < 3; node++)
    ports.push({ client: allocated[node * 2]!, controller: allocated[node * 2 + 1]! });
  const jmxPorts = allocated.slice(6);
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
  const dispose = async (): Promise<void> => {
    const results = await Promise.allSettled([admin.close(), owner.dispose()]);
    if (results.some((result) => result.status === "rejected"))
      throw new Error("The isolated replication fixture cleanup did not complete.");
  };
  const brokerName = (id: number): string => {
    const name = names[id - 1];
    if (!name) throw new Error("Unknown replication fixture broker.");
    return name;
  };
  try {
    await owner.prepare(image);
    for (const [index, p] of ports.entries()) {
      const environment: Record<string, string> = {
        ...(jmx
          ? {
              KAFKA_OPTS: `-javaagent:/opt/metrics/agent.jar=127.0.0.1:${jmxPorts[index]}:/opt/metrics/exporter.yml`,
            }
          : {}),
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
      const id = await owner.create(names[index]!, [
        "--network",
        "host",
        "--memory",
        "768m",
        "--cpus",
        "2",
        ...(jmx
          ? [
              "--volume",
              `${jmx.agent}:/opt/metrics/agent.jar:ro`,
              "--volume",
              `${jmx.configuration}:/opt/metrics/exporter.yml:ro`,
            ]
          : []),
        ...Object.entries(environment).flatMap(([key, value]) => ["--env", `${key}=${value}`]),
        image,
      ]);
      identities.set(names[index]!, id);
      await owner.start(id);
    }
    let ready = false;
    for (let attempt = 0; attempt < 60; attempt++) {
      if (attempt % 5 === 0) {
        for (const name of identities.values()) {
          const { stdout } = await execute(
            "docker",
            ["inspect", "--format", "{{.State.Running}}", name],
            { timeout: 5_000 },
          );
          if (stdout.trim() !== "true") {
            const [state, logs] = await Promise.all([
              execute(
                "docker",
                ["inspect", "--format", "exit={{.State.ExitCode}} oom={{.State.OOMKilled}}", name],
                { timeout: 5_000 },
              ),
              execute("docker", ["logs", "--tail", "200", name], { timeout: 5_000 }),
            ]);
            const lines = `${logs.stdout}${logs.stderr}`.split("\n");
            const causes = lines
              .filter((line) => /ERROR|FATAL|Exception|Caused by|OutOfMemory/u.test(line))
              .slice(-30);
            throw new Error(
              `An isolated replication node exited during startup (${state.stdout.trim()}).\n${causes.join("\n")}\n${lines.slice(-20).join("\n")}`,
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
      metricsInstances: jmxPorts.map((port) => `127.0.0.1:${port}`),
      connection: {
        name: "Isolated replication fixture",
        brokers: ports.map((p) => `127.0.0.1:${p.client}`),
        tls: { enabled: false },
      },
      async stopBroker(id): Promise<void> {
        await execute("docker", ["stop", "--time", "10", identities.get(brokerName(id))!], {
          timeout: 30_000,
        });
      },
      async startBroker(id): Promise<void> {
        await owner.start(identities.get(brokerName(id))!);
      },
      dispose,
    };
  } catch (error) {
    await dispose();
    throw error;
  }
}
