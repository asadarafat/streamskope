import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { promisify } from "node:util";

import {
  ensureContainerImage,
  containerCommandFailureReason,
} from "../../tools/dev/container-image";

const execute = promisify(execFile);
export type DockerCommand = (
  arguments_: readonly string[],
  timeoutMs: number,
) => Promise<{ stdout: string; stderr?: string }>;
export const dockerCommand: DockerCommand = (arguments_, timeout) =>
  execute("docker", [...arguments_], { timeout });

export class FixtureCleanupError extends Error {
  constructor(readonly reason: ReturnType<typeof containerCommandFailureReason>) {
    super(`AIO Kafka fixture cleanup could not be confirmed (${reason}).`);
    this.name = "FixtureCleanupError";
  }
}

/** Ownership is checked by label and immutable ID, never by a reusable name alone. */
export class FixtureContainerOwner {
  readonly token = randomUUID();
  private readonly containers = new Map<string, string | undefined>();
  private readonly admissions = new Set<Promise<string>>();
  private revoked = false;

  constructor(private readonly command: DockerCommand = dockerCommand) {}

  async prepare(image: string): Promise<void> {
    if (this.revoked) throw new Error("AIO Kafka fixture ownership was revoked.");
    await ensureContainerImage(image, undefined, this.command);
  }

  async create(name: string, arguments_: readonly string[]): Promise<string> {
    if (this.revoked) throw new Error("AIO Kafka fixture ownership was revoked.");
    if (this.containers.has(name)) throw new Error("AIO Kafka fixture name was already admitted.");
    // Admit before create: a CLI timeout may happen after Docker creates the resource.
    this.containers.set(name, undefined);
    const operation = (async (): Promise<string> => {
      const { stdout } = await this.command(
        [
          "create",
          "--name",
          name,
          "--label",
          `streamskope.fixture=${this.token}`,
          "--label",
          "streamskope.fixture.system=aio-kafka",
          ...arguments_,
        ],
        30_000,
      );
      const id = stdout.trim();
      if (!/^[a-f0-9]{64}$/u.test(id))
        throw new Error("AIO Kafka fixture identity was not returned.");
      this.containers.set(name, id);
      return id;
    })();
    this.admissions.add(operation);
    try {
      return await operation;
    } finally {
      this.admissions.delete(operation);
    }
  }

  async start(id: string): Promise<void> {
    if (this.revoked || ![...this.containers.values()].includes(id))
      throw new Error("AIO Kafka fixture start has no active owner.");
    await this.command(["start", id], 30_000);
  }

  async remove(name: string): Promise<void> {
    if (!this.containers.has(name)) return;
    let id = this.containers.get(name);
    if (id === undefined) {
      const found = await this.command(
        [
          "ps",
          "--all",
          "--no-trunc",
          "--filter",
          `name=^/${name}$`,
          "--filter",
          `label=streamskope.fixture=${this.token}`,
          "--format",
          "{{.ID}}",
        ],
        15_000,
      );
      id = found.stdout.trim();
      if (id === "") {
        this.containers.delete(name);
        return;
      }
      if (!/^[a-f0-9]{64}$/u.test(id)) throw new Error("AIO Kafka fixture identity is ambiguous.");
      this.containers.set(name, id);
    }
    const remaining = (): Promise<{ stdout: string }> =>
      this.command(
        ["ps", "--all", "--no-trunc", "--filter", `id=${id}`, "--format", "{{.ID}}"],
        15_000,
      );
    // A repeated cleanup succeeds if the original ID is already gone.
    if ((await remaining()).stdout.trim() !== "") {
      const inspected = await this.command(
        ["inspect", id, "--format", '{{.Id}} {{index .Config.Labels "streamskope.fixture"}}'],
        15_000,
      );
      if (inspected.stdout.trim() !== `${id} ${this.token}`)
        throw new Error("AIO Kafka fixture cleanup ownership differs.");
      await this.command(["rm", "--force", "--volumes", id], 30_000);
      if ((await remaining()).stdout.trim() !== "")
        throw new Error("AIO Kafka fixture removal is unconfirmed.");
    }
    this.containers.delete(name);
  }

  async dispose(): Promise<void> {
    this.revoked = true;
    // A timed-out/cancelled caller does not prove that Docker failed to create it.
    await Promise.allSettled([...this.admissions]);
    const results = await Promise.allSettled(
      [...this.containers.keys()].map((name) => this.remove(name)),
    );
    const failure = results.find((result) => result.status === "rejected");
    if (failure?.status === "rejected")
      throw new FixtureCleanupError(containerCommandFailureReason(failure.reason));
  }
}
