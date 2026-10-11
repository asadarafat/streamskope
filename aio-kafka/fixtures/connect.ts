import { randomUUID } from "node:crypto";
import { mkdtemp, mkdir, writeFile, chmod, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import type { KafkaClusterServiceContext } from "../../src/features/kafka/application/types";
import { OwnedKafkaResources } from "../../src/features/kafka/engine/owned-kafka-resources";

import { loopbackPorts } from "./loopback";
import { FixtureContainerOwner } from "./container-owner";
import { kafkaPin } from "./pins";
export async function startConnectFixture(broker: string): Promise<{
  readonly url: string;
  readonly context: KafkaClusterServiceContext;
  writeSource(lines: readonly string[]): Promise<void>;
  dispose(): Promise<void>;
}> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-connect-"));
  await chmod(directory, 0o755);
  const sourceData = join(directory, "source-data");
  await mkdir(sourceData, { mode: 0o777 });
  await chmod(sourceData, 0o777);
  const [port] = await loopbackPorts(1);
  const name = `streamskope-connect-${randomUUID()}`;
  const { image, version } = await kafkaPin();
  const containers = new FixtureContainerOwner();
  const config = [
    `bootstrap.servers=${broker}`,
    `listeners=http://127.0.0.1:${port}`,
    `rest.advertised.host.name=127.0.0.1`,
    `rest.advertised.port=${port}`,
    `group.id=${name}`,
    `config.storage.topic=${name}-configs`,
    `offset.storage.topic=${name}-offsets`,
    `status.storage.topic=${name}-status`,
    `config.storage.replication.factor=1`,
    `offset.storage.replication.factor=1`,
    `status.storage.replication.factor=1`,
    `offset.storage.partitions=1`,
    `status.storage.partitions=1`,
    `key.converter=org.apache.kafka.connect.storage.StringConverter`,
    `value.converter=org.apache.kafka.connect.storage.StringConverter`,
    `plugin.path=/opt/kafka/libs/connect-file-${version}.jar`,
    `offset.flush.interval.ms=1000`,
  ].join("\n");
  await writeFile(join(directory, "worker.properties"), config, { mode: 0o644 });
  const url = `http://127.0.0.1:${port}`;
  const lifecycle = new AbortController(),
    owner = new OwnedKafkaResources(lifecycle.signal);
  const context: KafkaClusterServiceContext = {
    baseUrl: url,
    signal: lifecycle.signal,
    requestOwner: owner,
    authorization: (): Promise<undefined> => Promise.resolve(undefined),
  };
  const dispose = async (): Promise<void> => {
    lifecycle.abort();
    const failures: unknown[] = [];
    try {
      await owner.close();
    } catch (error) {
      failures.push(error);
    }
    try {
      await containers.dispose();
    } catch (error) {
      failures.push(error);
    }
    try {
      await rm(directory, { recursive: true, force: true });
    } catch (error) {
      failures.push(error);
    }
    if (failures.length) throw new AggregateError(failures, "Connect fixture cleanup failed.");
  };
  try {
    await containers.prepare(image);
    const id = await containers.create(name, [
      "--network",
      "host",
      "--memory",
      "512m",
      "--cpus",
      "1",
      "--env",
      "KAFKA_HEAP_OPTS=-Xms128m -Xmx256m",
      "--mount",
      `type=bind,source=${directory},target=/fixture,readonly`,
      "--mount",
      `type=bind,source=${sourceData},target=/source-data`,
      "--entrypoint",
      "/opt/kafka/bin/connect-distributed.sh",
      image,
      "/fixture/worker.properties",
    ]);
    await containers.start(id);
    for (let n = 0; n < 90; n++) {
      try {
        const response = await fetch(`${url}/connectors`, { signal: AbortSignal.timeout(1000) });
        if (response.ok)
          return {
            url,
            context,
            dispose,
            writeSource: async (lines: readonly string[]): Promise<void> => {
              await writeFile(join(sourceData, "source.txt"), lines.join("\n") + "\n", {
                mode: 0o666,
              });
              await chmod(join(sourceData, "source.txt"), 0o666);
            },
          };
      } catch {
        /* Worker starts asynchronously. */
      }
      await delay(1000);
    }
    throw new Error("Worker readiness timed out.");
  } catch {
    await dispose();
    throw new Error("Isolated Connect fixture did not become ready.");
  }
}
