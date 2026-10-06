import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { chmod, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { connect, headers, type NatsConnection } from "@nats-io/transport-node";

import {
  loadNatsFixtureRecord,
  loadNatsFixtureIntent,
  natsIntentPath,
  natsFixtureConnection,
  natsOwnershipRoot,
  natsRecordPath,
  NATS_FIXTURE_NAME,
  NatsFixtureError,
  type NatsFixtureRecord,
  type NatsFixtureIntent,
  natsContainerlabPaths,
  parseNatsFixtureRecord,
  parseNatsFixtureIntent,
  type NatsContainerlabFixtureIntent,
  type NatsContainerlabFixtureRecord,
} from "./ownership";
import { ContainerlabNatsRuntime } from "./containerlab-runtime";
import type { NatsPersistentRuntime } from "./persistent-runtime";
import { NATS_SERVER_IMAGES } from "./definition";
import { boundedNatsOperation } from "./client";

const execute = promisify(execFile);

export type NatsDockerCommand = (
  arguments_: readonly string[],
  timeout?: number,
) => Promise<string>;

const runDocker: NatsDockerCommand = async (arguments_, timeout = 10_000) => {
  try {
    return (await execute("docker", [...arguments_], { timeout })).stdout;
  } catch {
    throw new NatsFixtureError(
      "Local AIO NATS Docker operation failed. Check Docker and the owned container before retrying.",
    );
  }
};

interface NatsFixtureConfig {
  readonly port: number;
  readonly subject: string;
  readonly payload: Readonly<Record<string, unknown>>;
}

export async function loadNatsFixtureConfig(repositoryRoot: string): Promise<NatsFixtureConfig> {
  const value: unknown = JSON.parse(
    await readFile(join(repositoryRoot, "aio-nats", "fixture.config.json"), "utf8"),
  );
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new NatsFixtureError("Local AIO NATS source configuration is invalid.");
  const candidate = value as Partial<NatsFixtureConfig>;
  if (
    typeof candidate.port !== "number" ||
    !Number.isSafeInteger(candidate.port) ||
    candidate.port < 1 ||
    candidate.port > 65_535 ||
    typeof candidate.subject !== "string" ||
    !/^[a-z0-9_-]+(?:\.[a-z0-9_-]+)+$/u.test(candidate.subject) ||
    typeof candidate.payload !== "object" ||
    candidate.payload === null ||
    Array.isArray(candidate.payload)
  )
    throw new NatsFixtureError("Local AIO NATS source configuration is invalid.");
  return candidate as NatsFixtureConfig;
}

export interface NatsFixtureStatus {
  readonly name: string;
  readonly status: "ready" | "absent";
  readonly server?: string;
  readonly caPath?: string;
  readonly tokenPath?: string;
  readonly subject?: string;
}

/** Persistent lab ownership is separate from disposable qualification ownership. */
export class NatsFixtureLifecycle {
  private readonly runtime: NatsPersistentRuntime;

  constructor(
    private readonly repositoryRoot: string,
    private readonly docker: NatsDockerCommand = runDocker,
    runtime?: NatsPersistentRuntime,
  ) {
    this.runtime = runtime ?? new ContainerlabNatsRuntime(repositoryRoot, docker);
  }

  async ensure(signal?: AbortSignal): Promise<NatsFixtureStatus> {
    return this.exclusive(async () => {
      signal?.throwIfAborted();
      await this.recoverInterruptedStart();
      const config = await loadNatsFixtureConfig(this.repositoryRoot);
      const existing = await loadNatsFixtureRecord(this.repositoryRoot);
      if (existing !== undefined) {
        if (existing.port !== config.port)
          throw new NatsFixtureError(
            "Local AIO NATS settings changed. Stop the owned fixture before starting it again.",
          );
        const status = await this.verifyOwnership(existing);
        if (status === "absent") {
          if (existing.format === 2) await this.runtime.stop(existing);
          await this.removeLocalRecord(existing);
        } else {
          if (status === "exited" || status === "created") {
            if (existing.format === 2) await this.runtime.resume(existing, signal);
            else await this.docker(["start", existing.container], 30_000);
          } else if (status !== "running")
            throw new NatsFixtureError(
              "Local AIO NATS is not resumable; explicit recovery is required.",
            );
          await this.verifyReady(existing, signal);
          return this.summary(existing, config.subject);
        }
      }

      const stdout = await this.docker([
        "ps",
        "--all",
        "--filter",
        `name=^/${NATS_FIXTURE_NAME}$`,
        "--format",
        "{{.ID}}",
      ]);
      if (stdout.trim().length > 0)
        throw new NatsFixtureError(
          "The Local AIO NATS name belongs to an unrecorded container; no resource was changed.",
        );
      const instances = join(natsOwnershipRoot(this.repositoryRoot), "instances");
      await mkdir(instances, { recursive: true, mode: 0o700 });
      await chmod(instances, 0o700);
      const directory = await mkdtemp(join(instances, "server-"));
      if (process.platform !== "linux" || (process.arch !== "arm64" && process.arch !== "x64")) {
        await rm(directory, { recursive: true, force: true });
        throw new NatsFixtureError(
          "Local AIO NATS requires Linux arm64 or amd64 Docker and Containerlab.",
        );
      }
      const identity = randomUUID();
      const intent: NatsContainerlabFixtureIntent = {
        format: 2,
        identity,
        directory,
        image: NATS_SERVER_IMAGES[process.arch],
        port: config.port,
        ...natsContainerlabPaths(identity, directory),
        creationStarted: false,
        mutationsSettled: true,
      };
      await this.saveStartIntent(intent);
      let progressIntent = intent;
      let record: NatsContainerlabFixtureRecord | undefined;
      try {
        const candidate = await this.runtime.start(
          intent,
          async (progress) => {
            const parsed = parseNatsFixtureIntent(progress, this.repositoryRoot);
            if (
              parsed.format !== 2 ||
              parsed.identity !== intent.identity ||
              parsed.directory !== intent.directory ||
              parsed.image !== intent.image ||
              parsed.port !== intent.port ||
              (progressIntent.daemon !== undefined && parsed.daemon !== progressIntent.daemon)
            )
              throw new NatsFixtureError(
                "Local AIO NATS deployment changed its ownership identity.",
              );
            await this.saveStartIntent(parsed);
            progressIntent = parsed;
          },
          signal,
        );
        const validated = parseNatsFixtureRecord(candidate, this.repositoryRoot);
        if (
          validated.format !== 2 ||
          validated.identity !== intent.identity ||
          validated.directory !== intent.directory ||
          validated.image !== intent.image ||
          validated.port !== intent.port ||
          progressIntent.daemon !== validated.daemon ||
          progressIntent.container !== validated.container ||
          progressIntent.network !== validated.network ||
          progressIntent.volume !== validated.volume ||
          !progressIntent.mutationsSettled
        )
          throw new NatsFixtureError(
            "Local AIO NATS deployment did not confirm its complete ownership journal.",
          );
        record = validated;
        await this.verifyReady(record, signal);
        await this.saveRecord(record);
        await rm(natsIntentPath(this.repositoryRoot), { force: true });
        return this.summary(record, config.subject);
      } catch {
        // Preserve all private evidence until the daemon-resource owner confirms cleanup.
        try {
          if (record !== undefined) await this.runtime.stop(record);
          else await this.runtime.recover(progressIntent);
        } catch {
          throw new NatsFixtureError(
            "Local AIO NATS startup completion or cleanup is unconfirmed. Private ownership evidence was retained; follow the recovery instructions before retrying.",
          );
        }
        const committed = await loadNatsFixtureRecord(this.repositoryRoot);
        if (committed !== undefined && committed.identity === intent.identity)
          await this.removeLocalRecord(committed);
        await rm(directory, { recursive: true, force: true });
        await rm(natsIntentPath(this.repositoryRoot), { force: true });
        throw new NatsFixtureError(
          "Local AIO NATS startup qualification failed; only the new owned resources were removed.",
        );
      }
    });
  }

  async status(): Promise<NatsFixtureStatus> {
    const record = await loadNatsFixtureRecord(this.repositoryRoot);
    if (record === undefined) return { name: NATS_FIXTURE_NAME, status: "absent" };
    if ((await this.verifyOwnership(record)) !== "running")
      throw new NatsFixtureError(
        "Local AIO NATS is stopped. Run npm run dev -- nats start to resume it.",
      );
    await this.verifyReady(record);
    return this.summary(record, (await loadNatsFixtureConfig(this.repositoryRoot)).subject);
  }

  async stop(): Promise<void> {
    await this.exclusive(async () => {
      await this.recoverInterruptedStart();
      const record = await loadNatsFixtureRecord(this.repositoryRoot);
      if (record === undefined) return;
      if (record.format === 2) await this.runtime.stop(record);
      else if ((await this.verifyOwnership(record)) !== "absent")
        await this.docker(["rm", "--force", "--volumes", record.container], 30_000);
      await this.removeLocalRecord(record);
    });
  }

  /** Finite, low-rate live samples. Start the workbench subscription before publishing. */
  async publish(
    seconds = 60,
    rate = 2,
    signal?: AbortSignal,
  ): Promise<{ readonly published: number; readonly subject: string }> {
    if (
      !Number.isInteger(seconds) ||
      seconds < 1 ||
      seconds > 600 ||
      !Number.isInteger(rate) ||
      rate < 1 ||
      rate > 100
    )
      throw new NatsFixtureError(
        "Publish duration must be 1–600 seconds and rate must be 1–100 messages/second.",
      );
    const record = await loadNatsFixtureRecord(this.repositoryRoot);
    if (record === undefined) throw new NatsFixtureError("Start Local AIO NATS before publishing.");
    if ((await this.verifyOwnership(record)) !== "running")
      throw new NatsFixtureError("Local AIO NATS is not running.");
    const config = await loadNatsFixtureConfig(this.repositoryRoot);
    const client = await this.open(record);
    let published = 0;
    const deadline = Date.now() + seconds * 1_000 + 2_000;
    try {
      for (let index = 0; index < seconds * rate; index += 1) {
        signal?.throwIfAborted();
        if (Date.now() >= deadline)
          throw new NatsFixtureError("Local AIO NATS sample publication exceeded its duration.");
        if (client.isClosed())
          throw new NatsFixtureError("Local AIO NATS sample publisher disconnected.");
        const metadata = headers();
        metadata.set("content-type", "application/json");
        metadata.set("x-fixture", "aio-nats");
        metadata.set("x-correlation-id", `aio-nats-${index + 1}`);
        client.publish(
          config.subject,
          JSON.stringify({
            ...config.payload,
            sequence: index + 1,
            timestamp: new Date().toISOString(),
          }),
          { headers: metadata },
        );
        await boundedNatsOperation(client.flush(), Math.min(2_000, deadline - Date.now()));
        published += 1;
        if (index + 1 < seconds * rate) await delay(1_000 / rate);
      }
      return { published, subject: config.subject };
    } finally {
      await client.close();
      await client.closed();
    }
  }

  private summary(record: NatsFixtureRecord, subject: string): NatsFixtureStatus {
    return {
      name: record.name,
      status: "ready",
      server: `nats://127.0.0.1:${record.port}`,
      caPath: join(record.directory, "ca.pem"),
      tokenPath: join(record.directory, "token"),
      subject,
    };
  }

  private async exclusive<T>(operation: () => Promise<T>): Promise<T> {
    const root = natsOwnershipRoot(this.repositoryRoot);
    await mkdir(root, { recursive: true, mode: 0o700 });
    await chmod(root, 0o700);
    const lock = join(root, ".operation.lock");
    try {
      await mkdir(lock, { mode: 0o700 });
    } catch {
      throw new NatsFixtureError(
        "Another Local AIO NATS lifecycle operation holds the lock. Wait for it before retrying.",
      );
    }
    try {
      return await operation();
    } finally {
      await rm(lock, { recursive: true });
    }
  }

  private async verifyOwnership(record: NatsFixtureRecord): Promise<string> {
    if (record.format === 2) return this.runtime.status(record);
    const found = await this.docker([
      "ps",
      "--all",
      "--no-trunc",
      "--filter",
      `id=${record.container}`,
      "--format",
      "{{.ID}}",
    ]);
    if (found.trim() === "") {
      const named = await this.docker([
        "ps",
        "--all",
        "--no-trunc",
        "--filter",
        `name=^/${record.name}$`,
        "--format",
        "{{.ID}}",
      ]);
      if (named.trim() !== "")
        throw new NatsFixtureError(
          "Local AIO NATS name is occupied by another container; no resource was changed.",
        );
      return "absent";
    }
    if (found.trim() !== record.container)
      throw new NatsFixtureError(
        "Local AIO NATS container identity could not be verified; no resource was changed.",
      );
    return this.inspectOwnership(record);
  }

  private async inspectOwnership(record: NatsFixtureRecord): Promise<string> {
    const stdout = await this.docker([
      "inspect",
      "--format",
      '{{.Id}}|{{.Name}}|{{.State.Status}}|{{index .Config.Labels "io.streamskope.fixture.id"}}|{{index .Config.Labels "io.streamskope.fixture"}}|{{.Config.Image}}',
      record.container,
    ]);
    const [container, name, status, identity, fixture, image] = stdout.trim().split("|");
    if (
      container !== record.container ||
      name !== `/${record.name}` ||
      identity !== record.identity ||
      fixture !== "nats" ||
      image !== record.image ||
      status === undefined
    )
      throw new NatsFixtureError(
        "Local AIO NATS container ownership could not be verified; no resource was changed.",
      );
    return status;
  }

  private async removeLocalRecord(record: NatsFixtureRecord): Promise<void> {
    await rm(record.directory, { recursive: true, force: true });
    await rm(natsRecordPath(this.repositoryRoot), { force: true });
  }

  private async saveRecord(record: NatsFixtureRecord): Promise<void> {
    const staging = join(natsOwnershipRoot(this.repositoryRoot), `.record-${randomUUID()}.json`);
    try {
      await writeFile(staging, `${JSON.stringify(record)}\n`, { mode: 0o600, flag: "wx" });
      await rename(staging, natsRecordPath(this.repositoryRoot));
    } finally {
      await rm(staging, { force: true });
    }
  }

  private async saveStartIntent(intent: NatsFixtureIntent): Promise<void> {
    const staging = join(natsOwnershipRoot(this.repositoryRoot), `.intent-${randomUUID()}.json`);
    try {
      await writeFile(staging, `${JSON.stringify(intent)}\n`, { mode: 0o600, flag: "wx" });
      await rename(staging, natsIntentPath(this.repositoryRoot));
    } finally {
      await rm(staging, { force: true });
    }
  }

  private async recoverInterruptedStart(): Promise<void> {
    const intent = await loadNatsFixtureIntent(this.repositoryRoot);
    if (intent === undefined) return;
    const record = await loadNatsFixtureRecord(this.repositoryRoot);
    if (intent.format === 2) {
      if (record !== undefined) {
        if (
          record.format !== 2 ||
          record.identity !== intent.identity ||
          record.directory !== intent.directory ||
          record.image !== intent.image ||
          record.port !== intent.port ||
          !intent.mutationsSettled ||
          record.daemon !== intent.daemon ||
          (intent.container !== undefined && record.container !== intent.container) ||
          (intent.network !== undefined && record.network !== intent.network) ||
          (intent.volume !== undefined && record.volume !== intent.volume)
        )
          throw new NatsFixtureError(
            "Local AIO NATS start intent conflicts with committed ownership; no resource was changed.",
          );
        await rm(natsIntentPath(this.repositoryRoot), { force: true });
        return;
      }
      await this.runtime.recover(intent);
      await rm(intent.directory, { recursive: true, force: true });
      await rm(natsIntentPath(this.repositoryRoot), { force: true });
      return;
    }
    if (record !== undefined) {
      if (
        record.format !== 1 ||
        record.identity !== intent.identity ||
        record.directory !== intent.directory ||
        record.image !== intent.image ||
        record.port !== intent.port ||
        (intent.container !== undefined && record.container !== intent.container)
      )
        throw new NatsFixtureError(
          "Local AIO NATS start intent conflicts with committed ownership; no resource was changed.",
        );
      await rm(natsIntentPath(this.repositoryRoot), { force: true });
      return;
    }
    if (intent.container !== undefined) {
      if ((await this.verifyOwnership({ ...intent, container: intent.container })) !== "absent")
        await this.docker(["rm", "--force", "--volumes", intent.container], 30_000);
      await rm(intent.directory, { recursive: true, force: true });
      await rm(natsIntentPath(this.repositoryRoot), { force: true });
      return;
    }
    const named = await this.docker([
      "ps",
      "--all",
      "--no-trunc",
      "--filter",
      `name=^/${intent.name}$`,
      "--format",
      "{{.ID}}",
    ]);
    const container = named.trim();
    if (container === "" && intent.creationStarted)
      throw new NatsFixtureError(
        "Local AIO NATS daemon-side creation completion is unconfirmed. Private ownership evidence was retained; manual recovery requires establishing that the request has quiesced.",
      );
    if (container !== "") {
      if (!/^[a-f0-9]{64}$/u.test(container))
        throw new NatsFixtureError(
          "Local AIO NATS pending container identity could not be verified.",
        );
      await this.inspectOwnership({ ...intent, container });
      await this.docker(["rm", "--force", "--volumes", container], 30_000);
    }
    await rm(intent.directory, { recursive: true, force: true });
    await rm(natsIntentPath(this.repositoryRoot), { force: true });
  }

  private async open(record: NatsFixtureRecord): Promise<NatsConnection> {
    const input = await natsFixtureConnection(record);
    const client = await connect({
      servers: [...input.servers],
      ...(input.authentication.mode === "token" ? { token: input.authentication.token } : {}),
      tls: {
        rejectUnauthorized: true,
        ...(input.tls.mode === "tls" && input.tls.caPem !== undefined
          ? { ca: input.tls.caPem }
          : {}),
      },
      timeout: 1_000,
      reconnect: false,
      waitOnFirstConnect: false,
      ignoreClusterUpdates: true,
      noRandomize: true,
      debug: false,
    });
    if (client.isClosed()) throw new NatsFixtureError("Local AIO NATS client closed during setup.");
    return client;
  }

  private async verifyReady(record: NatsFixtureRecord, signal?: AbortSignal): Promise<void> {
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      signal?.throwIfAborted();
      let client: NatsConnection | undefined;
      try {
        client = await this.open(record);
        const subject = `streamskope.readiness.${randomUUID()}`;
        const subscription = client.subscribe(subject, { max: 1 });
        await boundedNatsOperation(client.flush(), Math.min(2_000, deadline - Date.now()));
        const message = subscription[Symbol.asyncIterator]().next();
        client.publish(subject, "ready");
        await boundedNatsOperation(client.flush(), Math.min(2_000, deadline - Date.now()));
        const received = await boundedNatsOperation(
          message,
          Math.min(1_000, deadline - Date.now()),
        );
        if (received.done || received.value.string() !== "ready")
          throw new NatsFixtureError("Local AIO NATS round-trip qualification failed.");
        return;
      } catch {
        await delay(100);
      } finally {
        if (client !== undefined) {
          await client.close();
          await client.closed();
        }
      }
    }
    throw new NatsFixtureError("Local AIO NATS TLS/token round-trip readiness timed out.");
  }
}
