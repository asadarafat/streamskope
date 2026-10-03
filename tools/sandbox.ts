import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdir, open, unlink, writeFile, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { Admin, Producer } from "@platformatic/kafka";

import { StreamSkopeKafkaEngine } from "../src/features/kafka/engine";
import type { SecureConnectionInput } from "../src/features/kafka/contracts";

class SandboxError extends Error {}
async function closeClients(clients: readonly { close(): Promise<void> }[]): Promise<void> {
  const closed = await Promise.allSettled(clients.map((client) => client.close()));
  if (closed.some((result) => result.status === "rejected"))
    throw new SandboxError("Sandbox client cleanup could not be confirmed.");
}

const execute = promisify(execFile),
  project = "streamskope-sandbox";
async function waitForTopic(
  client: Admin | Producer<Buffer | null, Buffer, Buffer, Buffer>,
  topic: string,
  signal: AbortSignal,
): Promise<void> {
  const deadline = Date.now() + 15000;
  for (let attempt = 0; attempt < 40 && Date.now() < deadline; attempt++) {
    signal.throwIfAborted();
    try {
      const observed = (
        await client.metadata({ topics: [topic], forceUpdate: true, autocreateTopics: false })
      ).topics.get(topic);
      if (
        observed?.partitions.length &&
        observed.partitions.every((partition) => partition.leader >= 0)
      )
        return;
    } catch {
      /* Topic creation acknowledgement can precede readable broker metadata. Only retry this read. */
    }
    await delay(250, undefined, { signal });
  }
  throw new SandboxError("Topic metadata is not ready. Inspect sandbox status before retrying up.");
}
async function docker(args: readonly string[], signal: AbortSignal): Promise<string> {
  return (await execute("docker", [...args], { timeout: 180000, maxBuffer: 1048576, signal }))
    .stdout;
}
async function owned(instance: string, signal: AbortSignal): Promise<void> {
  for (const kind of ["container", "network", "volume"] as const) {
    const list = kind === "container" ? ["ps", "-aq"] : [kind, "ls", "-q"];
    const ids = (
      await docker([...list, "--filter", `label=com.docker.compose.project=${project}`], signal)
    )
      .trim()
      .split(/\s+/u)
      .filter(Boolean);
    if (kind === "network")
      ids.push(
        ...(await docker(["network", "ls", "-q", "--filter", `name=^${project}_default$`], signal))
          .trim()
          .split(/\s+/u)
          .filter(Boolean),
      );
    for (const id of new Set(ids)) {
      const labels = kind === "container" ? ".Config.Labels" : ".Labels";
      const label = (
        await docker(
          [kind, "inspect", "--format", `{{index ${labels} "org.streamskope.owner"}}`, id],
          signal,
        )
      ).trim();
      const owner = (
        await docker(
          [kind, "inspect", "--format", `{{index ${labels} "org.streamskope.instance"}}`, id],
          signal,
        )
      ).trim();
      if (label !== "sandbox-v1" || owner !== instance)
        throw new SandboxError("Project contains unowned resources.");
    }
  }
}
function port(name: string, fallback: number): number {
  const raw = process.env[name] ?? String(fallback);
  if (!/^\d{4,5}$/u.test(raw) || Number(raw) > 65535 || Number(raw) < 1024)
    throw new SandboxError("Invalid sandbox port.");
  return Number(raw);
}
export async function sandboxMain(
  args: readonly string[],
  signal: AbortSignal = new AbortController().signal,
): Promise<void> {
  const operation = args[0] ?? "help";
  if (operation === "help") {
    process.stdout.write(
      "npm run dev -- sandbox up|status|consume|transform|down\nOwns only the streamskope-sandbox Docker Compose project. Local plaintext endpoints; no production credentials.\n",
    );
    return;
  }
  if (args.length !== 1 || !["up", "status", "consume", "transform", "down"].includes(operation))
    throw new SandboxError("Unsupported sandbox operation.");
  const directory = resolve(".artifacts/sandbox");
  await mkdir(directory, { recursive: true });
  const lock = resolve(directory, "operation.lock");
  const handle = await open(lock, "wx", 0o600);
  try {
    const ownerPath = resolve(directory, "instance");
    let instance: string;
    try {
      instance = (await readFile(ownerPath, "utf8")).trim();
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
      instance = randomBytes(16).toString("hex");
      await writeFile(ownerPath, instance, { mode: 0o600, flag: "wx" });
    }
    if (!/^[a-f0-9]{32}$/u.test(instance))
      throw new SandboxError("Invalid sandbox ownership file.");
    process.env.STREAMSKOPE_SANDBOX_INSTANCE = instance;
    await owned(instance, signal);
    const compose = ["compose", "--project-name", project, "--file", "sandbox/compose.yml"];
    if (operation === "down") {
      await docker([...compose, "down", "--volumes", "--remove-orphans"], signal);
      process.stdout.write("Owned sandbox removed; seed resets on the next up.\n");
      return;
    }
    if (operation === "status") {
      process.stdout.write(await docker([...compose, "ps", "--format", "json"], signal));
      return;
    }
    const broker = `127.0.0.1:${port("STREAMSKOPE_SANDBOX_KAFKA_PORT", 19096)}`,
      connect = `http://127.0.0.1:${port("STREAMSKOPE_SANDBOX_CONNECT_PORT", 18086)}`;
    const connection: SecureConnectionInput = {
      name: "StreamSkope sandbox",
      brokers: [broker],
      tls: { enabled: false },
      services: { connect: { baseUrl: connect, authentication: "none" } },
    };
    if (operation === "up") {
      await docker([...compose, "up", "--detach", "--wait", "--wait-timeout", "120"], signal);
      let ready = false;
      for (let i = 0; i < 60; i++) {
        try {
          const r = await fetch(`${connect}/connectors`, {
            signal: AbortSignal.any([signal, AbortSignal.timeout(1000)]),
          });
          if (r.ok) {
            ready = true;
            break;
          }
        } catch {
          /* bounded readiness */
        }
        await delay(1000, undefined, { signal });
      }
      if (!ready) throw new SandboxError("Connect readiness timed out; inspect sandbox status.");
    }
    const active = await new StreamSkopeKafkaEngine().openConnection(
      connection,
      AbortSignal.any([signal, AbortSignal.timeout(15000)]),
    );
    const admin = new Admin({
        bootstrapBrokers: [broker],
        retries: 0,
        connectTimeout: 2000,
        requestTimeout: 2000,
        clientId: "streamskope-sandbox-admin",
      }),
      producer = new Producer<Buffer | null, Buffer, Buffer, Buffer>({
        bootstrapBrokers: [broker],
        clientId: "streamskope-sandbox-producer",
        retries: 0,
        connectTimeout: 2000,
        requestTimeout: 2000,
        autocreateTopics: false,
        repeatOnStaleMetadata: false,
      });
    try {
      if (operation === "up") {
        const seed = Array.from({ length: 10 }, (_, i) => ({
          topic: "sandbox.events",
          key: Buffer.from(`event-${i + 1}`),
          value: Buffer.from(
            JSON.stringify({ eventId: i + 1, seed: 1, kind: "sandbox", value: (i + 1) * 10 }),
          ),
        }));
        const existing = await active.listTopics();
        if (!existing.includes("sandbox.events")) {
          await admin.createTopics({
            topics: ["sandbox.events"],
            partitions: 1,
            replicas: 1,
            configs: [{ name: "retention.bytes", value: "16777216" }],
          });
          await waitForTopic(producer, "sandbox.events", signal);
          await producer.send({ messages: seed });
        }
        const seeded = await active.openMessageStream(
          { topic: "sandbox.events", mode: "earliest", maxMessages: 10 },
          AbortSignal.any([signal, AbortSignal.timeout(15000)]),
        );
        let index = 0;
        try {
          for await (const record of seeded) {
            const expected = seed[index];
            if (
              !expected ||
              record.offset !== String(index) ||
              record.key !== expected.key.toString() ||
              record.payload !== expected.value.toString()
            )
              throw new SandboxError(
                "Sandbox seed changed or incomplete. Inspect it; down/up resets owned data.",
              );
            index++;
          }
        } finally {
          await seeded.close();
        }
        if (index !== seed.length)
          throw new SandboxError("Sandbox seed incomplete. Inspect it; down/up resets owned data.");
        if (!existing.includes("sandbox.processed"))
          await admin.createTopics({ topics: ["sandbox.processed"], partitions: 1, replicas: 1 });
        await waitForTopic(admin, "sandbox.processed", signal);
        const config = {
          connection,
          protection: { readOnly: true, maskKey: false, maskHeaders: [], valuePaths: [] },
        };
        await writeFile(
          resolve(directory, "connection.json"),
          JSON.stringify(config, null, 2) + "\n",
          { mode: 0o600 },
        );
        await writeFile(
          resolve(directory, "query.json"),
          JSON.stringify({ topic: "sandbox.events", mode: "earliest", maxMessages: 10 }) + "\n",
          { mode: 0o600 },
        );
        process.stdout.write(
          `Sandbox ready: Kafka ${broker}; Connect ${connect}. Configuration: .artifacts/sandbox/connection.json\n`,
        );
        return;
      }
      const stream = await active.openMessageStream(
        { topic: "sandbox.events", mode: "earliest", maxMessages: 10 },
        AbortSignal.any([signal, AbortSignal.timeout(15000)]),
      );
      let count = 0;
      try {
        for await (const record of stream) {
          signal.throwIfAborted();
          count++;
          if (operation === "consume")
            process.stdout.write(
              JSON.stringify({
                topic: record.topic,
                partition: record.partition,
                offset: record.offset,
                key: record.key,
                value: record.payload,
              }) + "\n",
            );
          else
            await producer.send({
              messages: [
                {
                  topic: "sandbox.processed",
                  key: record.key === null ? null : Buffer.from(record.key),
                  value: Buffer.from(
                    JSON.stringify({ sourceOffset: record.offset, value: record.payload }),
                  ),
                },
              ],
            });
        }
      } finally {
        await stream.close();
      }
      process.stdout.write(
        `${operation}: ${count} records; transform is an explicit batch and repeating it creates copies.\n`,
      );
    } finally {
      await closeClients([active, admin, producer]);
    }
  } finally {
    await handle.close();
    await unlink(lock);
  }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const controller = new AbortController();
  const cancel = (): void => controller.abort();
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  void sandboxMain(process.argv.slice(2), controller.signal)
    .catch((error: unknown) => {
      if (error instanceof SandboxError) process.stderr.write(error.message + "\n");
      process.stderr.write(
        "Sandbox operation failed. Check Docker, port availability, resource ownership and .artifacts/sandbox/operation.lock. Existing resources were not automatically deleted.\n",
      );
      process.exitCode = controller.signal.aborted ? 130 : 1;
    })
    .finally(() => {
      process.removeListener("SIGINT", cancel);
      process.removeListener("SIGTERM", cancel);
    });
}
