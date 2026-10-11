import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import { FixtureContainerOwner, dockerCommand } from "./container-owner";
import { kafkaPin } from "./pins";
import type { BrokerRuntime } from "./runtime";

const context = resolve("aio-kafka/images/qualification-broker");

/** A cached thin image; all executable Kafka/JDK bytes come from the lab's digest pin. */
export async function qualificationBrokerImage(): Promise<{ image: string; version: string }> {
  const pin = await kafkaPin();
  const hash = createHash("sha256").update(pin.image);
  for (const file of ["Dockerfile", "entrypoint.sh"])
    hash.update(await readFile(`${context}/${file}`));
  const image = `streamskope-aio-kafka-qualification:${hash.digest("hex")}`;
  try {
    await dockerCommand(["image", "inspect", image], 10_000);
  } catch {
    const owner = new FixtureContainerOwner();
    await owner.prepare(pin.image);
    await dockerCommand(
      ["build", "--build-arg", `KAFKA_IMAGE=${pin.image}`, "--tag", image, context],
      180_000,
    );
  }
  // Record the immutable local image ID, even though its build inputs also name the cache.
  const immutable = await dockerCommand(["image", "inspect", "--format", "{{.Id}}", image], 10_000);
  if (!/^sha256:[a-f0-9]{64}$/u.test(immutable.stdout.trim()))
    throw new Error("AIO Kafka qualification image identity is unavailable.");
  return { image: immutable.stdout.trim(), version: pin.version };
}

export async function createContainerBrokerRuntime(): Promise<BrokerRuntime> {
  if (process.platform !== "linux")
    throw new Error("AIO Kafka container qualification requires a Linux Docker host.");
  const pin = await qualificationBrokerImage();
  const owner = new FixtureContainerOwner();
  const prefix = `streamskope-kafka-qualification-${owner.token}`;
  let tools: string | undefined;
  let broker: string | undefined;
  let directory: string | undefined;
  const mount = (path: string): string[] => [
    "--network",
    "host",
    "--user",
    `${process.getuid!()}:${process.getgid!()}`,
    "--mount",
    `type=bind,source=${path},target=${path}`,
    "--workdir",
    path,
  ];
  return {
    metadata: { backend: "container", kafkaVersion: pin.version, image: pin.image },
    async tool(command, arguments_, options): Promise<unknown> {
      if (tools === undefined) {
        directory = options.cwd;
        tools = await owner.create(`${prefix}-tools`, [
          ...mount(directory),
          "--memory",
          "256m",
          "--cpus",
          "1",
          "--entrypoint",
          "/bin/sleep",
          pin.image,
          "infinity",
        ]);
        await owner.start(tools);
      }
      if (directory !== options.cwd) throw new Error("AIO Kafka tool directory changed.");
      return dockerCommand(["exec", tools, command, ...arguments_], options.timeout);
    },
    async start(options): Promise<void> {
      await owner.remove(`${prefix}-tools`);
      tools = undefined;
      await writeFile(
        `${options.directory}/storage-arguments`,
        options.storageArguments.join("\n") + (options.storageArguments.length ? "\n" : ""),
        { mode: 0o600 },
      );
      broker = await owner.create(prefix, [
        ...mount(options.directory),
        "--memory",
        "768m",
        "--cpus",
        "2",
        "--env",
        "KAFKA_HEAP_OPTS=-Xms128m -Xmx384m",
        "--env",
        `LOG_DIR=${options.directory}/logs`,
        "--env",
        `KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/opt/kafka/config/log4j2.yaml -Dkafka.logs.dir=${options.directory}/logs`,
        "--env",
        `KAFKA_OPTS=-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=${options.oauthRoot}/.well-known/jwks.json`,
        pin.image,
        options.directory,
        options.clusterId,
      ]);
      await owner.start(broker);
    },
    async exited(): Promise<boolean> {
      if (broker === undefined) return true;
      const state = await dockerCommand(
        ["inspect", broker, "--format", "{{.State.Running}}"],
        5_000,
      );
      return state.stdout.trim() !== "true";
    },
    async logs(): Promise<string> {
      if (broker === undefined) return "";
      return dockerCommand(["logs", "--tail", "100", broker], 10_000)
        .then((result) => result.stdout + (result.stderr ?? ""))
        .catch(() => "");
    },
    dispose: () => owner.dispose(),
  };
}
