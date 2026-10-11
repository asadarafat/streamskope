import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";

import { startReplicationFixture } from "./kafka-replication-fixture";

const execute = promisify(execFile);
const IMAGE =
  "prom/prometheus:v3.15.0@sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e";
const JMX_SHA256 = "0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9";
/** Exact exporter rules qualified with Kafka 4.3.1; arbitrary JMX configurations are not inferred. */
export const KAFKA_METRICS_EXPORTER_CONFIGURATION = `includeObjectNames:
  - "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"
  - "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"
rules:
  - pattern: 'kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value'
    name: kafka_server_replicamanager_underreplicatedpartitions
    type: GAUGE
  - pattern: 'kafka.controller<type=KafkaController, name=OfflinePartitionsCount><>Value'
    name: kafka_controller_kafkacontroller_offlinepartitionscount
    type: GAUGE
`;
async function agent(): Promise<string> {
  const path = resolve(".artifacts/fixtures/jmx_prometheus_javaagent-1.7.0.jar");
  let bytes: Buffer;
  try {
    bytes = await readFile(path);
  } catch (cause) {
    if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
    const response = await fetch(
      "https://github.com/prometheus/jmx_exporter/releases/download/1.7.0/jmx_prometheus_javaagent-1.7.0.jar",
      { signal: AbortSignal.timeout(60_000) },
    );
    if (!response.ok)
      throw new Error("Pinned JMX agent could not be downloaded.", {
        cause,
      });
    bytes = Buffer.from(await response.arrayBuffer());
    if (createHash("sha256").update(bytes).digest("hex") !== JMX_SHA256)
      throw new Error("JMX agent digest differs.", {
        cause,
      });
    await mkdir(join(path, ".."), { recursive: true });
    await writeFile(path, bytes, { mode: 0o644 });
  }
  if (createHash("sha256").update(bytes).digest("hex") !== JMX_SHA256)
    throw new Error("Cached JMX agent digest differs.");
  return path;
}
export async function startPrometheusFixture(): Promise<{
  readonly endpoint: string;
  readonly job: "streamskope-kafka";
  readonly broker: Awaited<ReturnType<typeof startReplicationFixture>>;
  dispose(): Promise<void>;
}> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-prometheus-"));
  let broker: Awaited<ReturnType<typeof startReplicationFixture>> | undefined;
  const name = `streamskope-prometheus-${randomUUID()}`;
  let created = false;
  const dispose = async (): Promise<void> => {
    const results = await Promise.allSettled([
      ...(created
        ? [execute("docker", ["rm", "--force", "--volumes", name], { timeout: 30_000 })]
        : []),
      ...(broker ? [broker.dispose()] : []),
    ]);
    await rm(root, { recursive: true, force: true });
    if (results.some((r) => r.status === "rejected"))
      throw new Error("Metrics fixture cleanup failed.");
  };
  try {
    await chmod(root, 0o755);
    const configuration = join(root, "exporter.yml");
    await writeFile(configuration, KAFKA_METRICS_EXPORTER_CONFIGURATION, { mode: 0o644 });
    broker = await startReplicationFixture({ agent: await agent(), configuration });
    const server = createServer();
    await new Promise<void>((ready) => server.listen(0, "127.0.0.1", ready));
    const address = server.address();
    await new Promise<void>((closed) => server.close(() => closed()));
    if (!address || typeof address === "string")
      throw new Error("Metrics fixture port unavailable.");
    const config = join(root, "prometheus.yml");
    await writeFile(
      config,
      `global:\n  scrape_interval: 1s\nscrape_configs:\n  - job_name: streamskope-kafka\n    static_configs:\n      - targets: ${JSON.stringify(broker.metricsInstances)}\n`,
      { mode: 0o644 },
    );
    created = true;
    await execute(
      "docker",
      [
        "run",
        "--detach",
        "--name",
        name,
        "--network",
        "host",
        "--memory",
        "512m",
        "--tmpfs",
        "/prometheus:uid=65534,gid=65534,size=256m",
        "--volume",
        `${config}:/etc/prometheus/prometheus.yml:ro`,
        IMAGE,
        "--config.file=/etc/prometheus/prometheus.yml",
        `--web.listen-address=127.0.0.1:${address.port}`,
        "--storage.tsdb.retention.time=10m",
      ],
      { timeout: 120_000 },
    );
    const endpoint = `http://127.0.0.1:${address.port}`;
    for (let attempt = 0; attempt < 30; attempt++) {
      try {
        if ((await fetch(`${endpoint}/-/ready`, { signal: AbortSignal.timeout(1000) })).ok)
          return { endpoint, job: "streamskope-kafka", broker, dispose };
      } catch {
        /* bounded startup */
      }
      await delay(500);
    }
    throw new Error("Prometheus fixture did not become ready.");
  } catch (cause) {
    await dispose();
    throw cause;
  }
}
