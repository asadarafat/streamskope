import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi, type Mock } from "vitest";

import { NATS_SERVER_IMAGES } from "../../tools/dev/nats-fixture/definition";
import { NatsFixtureLifecycle } from "../../tools/dev/nats-fixture/lifecycle";
import {
  natsContainerlabPaths,
  natsIntentPath,
  natsOwnershipRoot,
  natsRecordPath,
  parseNatsFixtureIntent,
  parseNatsFixtureRecord,
  type NatsContainerlabFixtureIntent,
  type NatsContainerlabFixtureRecord,
} from "../../tools/dev/nats-fixture/ownership";
import type { NatsPersistentRuntime } from "../../tools/dev/nats-fixture/persistent-runtime";

const directories: string[] = [];

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});

async function repository(
  recorded = true,
): Promise<{ root: string; record: NatsContainerlabFixtureRecord }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-clab-lifecycle-"));
  directories.push(root);
  const directory = join(natsOwnershipRoot(root), "instances", "server-ABC123");
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await writeFile(
    join(root, "aio-nats", "fixture.config.json"),
    JSON.stringify({
      port: 14222,
      subject: "streamskope.fixture.events",
      payload: { source: "aio-nats" },
    }),
  );
  const identity = "11111111-1111-4111-8111-111111111111";
  const record: NatsContainerlabFixtureRecord = {
    format: 2,
    daemon: "fixture-daemon-identity",
    identity,
    directory,
    image: NATS_SERVER_IMAGES.arm64,
    port: 14222,
    ...natsContainerlabPaths(identity, directory),
    container: "b".repeat(64),
    network: "c".repeat(64),
    volume: "d".repeat(64),
  };
  await writeFile(join(directory, "token"), "private-token", { mode: 0o600 });
  if (recorded) await writeFile(natsRecordPath(root), JSON.stringify(record), { mode: 0o600 });
  return { root, record };
}

type OwnerMock = { [Key in keyof NatsPersistentRuntime]: Mock<NatsPersistentRuntime[Key]> };

function owner(): OwnerMock {
  return {
    start: vi
      .fn<NatsPersistentRuntime["start"]>()
      .mockRejectedValue(new Error("unexpected deploy")),
    status: vi.fn<NatsPersistentRuntime["status"]>().mockResolvedValue("absent"),
    resume: vi
      .fn<NatsPersistentRuntime["resume"]>()
      .mockRejectedValue(new Error("unexpected resume")),
    stop: vi.fn<NatsPersistentRuntime["stop"]>().mockResolvedValue(undefined),
    recover: vi.fn<NatsPersistentRuntime["recover"]>().mockResolvedValue(undefined),
  };
}

async function journal(root: string, intent: NatsContainerlabFixtureIntent): Promise<void> {
  await writeFile(natsIntentPath(root), JSON.stringify(intent), { mode: 0o600 });
}

const docker = (): Promise<string> =>
  Promise.reject(new Error("unexpected legacy Docker operation"));

describe("persistent Containerlab cleanup boundary", () => {
  it("keeps credentials until the resource owner confirms all three resources are absent", async () => {
    const { root, record } = await repository();
    const runtime = owner();
    let complete: (() => void) | undefined;
    runtime.stop = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          complete = resolve;
        }),
    );
    const lifecycle = new NatsFixtureLifecycle(root, docker, runtime);
    const stopping = lifecycle.stop();
    await vi.waitFor(() => expect(runtime.stop).toHaveBeenCalledWith(record));
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe("private-token");
    expect(JSON.parse(await readFile(natsRecordPath(root), "utf8"))).toEqual(record);
    complete?.();
    await stopping;
    await lifecycle.stop();
    expect(runtime.stop).toHaveBeenCalledTimes(1);
    await expect(readFile(natsRecordPath(root))).rejects.toMatchObject({ code: "ENOENT" });
    await expect(readFile(join(record.directory, "token"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("retains the complete ownership record and credentials when cleanup fails", async () => {
    const { root, record } = await repository();
    const runtime = owner();
    runtime.stop = vi.fn().mockRejectedValue(new Error("network ownership unconfirmed"));
    await expect(new NatsFixtureLifecycle(root, docker, runtime).stop()).rejects.toThrow(
      "unconfirmed",
    );
    expect(JSON.parse(await readFile(natsRecordPath(root), "utf8"))).toEqual(record);
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe("private-token");
    await expect(readFile(join(natsOwnershipRoot(root), ".operation.lock"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("does not redeploy after an absent server until remaining resources are cleaned", async () => {
    const { root, record } = await repository();
    const runtime = owner();
    runtime.stop = vi.fn().mockRejectedValue(new Error("private volume still attached"));
    await expect(new NatsFixtureLifecycle(root, docker, runtime).ensure()).rejects.toThrow(
      "still attached",
    );
    expect(runtime.status).toHaveBeenCalledWith(record);
    expect(runtime.stop).toHaveBeenCalledWith(record);
    expect(runtime.start).not.toHaveBeenCalled();
    expect(JSON.parse(await readFile(natsRecordPath(root), "utf8"))).toEqual(record);
  });

  it("retains interrupted creation evidence when the runtime cannot establish quiescence", async () => {
    const { root, record } = await repository(false);
    const base = {
      format: record.format,
      daemon: record.daemon,
      identity: record.identity,
      directory: record.directory,
      image: record.image,
      port: record.port,
      ...natsContainerlabPaths(record.identity, record.directory),
    };
    const intent: NatsContainerlabFixtureIntent = {
      ...base,
      creationStarted: true,
      mutationsSettled: false,
    };
    await journal(root, intent);
    const runtime = owner();
    runtime.recover = vi.fn().mockRejectedValue(new Error("creation completion unconfirmed"));
    const lifecycle = new NatsFixtureLifecycle(root, docker, runtime);
    await expect(lifecycle.stop()).rejects.toThrow("unconfirmed");
    expect(runtime.recover).toHaveBeenCalledWith(intent);
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe("private-token");
    expect(JSON.parse(await readFile(natsIntentPath(root), "utf8"))).toEqual(intent);
    runtime.recover = vi.fn().mockResolvedValue(undefined);
    await lifecycle.stop();
    await expect(readFile(natsIntentPath(root))).rejects.toMatchObject({ code: "ENOENT" });
    expect(runtime.stop).not.toHaveBeenCalled();
  });

  it("rejects conflicting committed resource IDs before asking the runtime to mutate", async () => {
    const { root, record } = await repository();
    const intent = {
      ...record,
      volume: "e".repeat(64),
      creationStarted: true,
      mutationsSettled: true,
    };
    await journal(root, intent);
    const runtime = owner();
    await expect(new NatsFixtureLifecycle(root, docker, runtime).stop()).rejects.toThrow(
      "conflicts",
    );
    expect(runtime.stop).not.toHaveBeenCalled();
    expect(runtime.recover).not.toHaveBeenCalled();
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe("private-token");
  });

  it("requires a settled full-ID journal before committing a deployment result", async () => {
    const { root } = await repository(false);
    const runtime = owner();
    runtime.start = vi.fn<NatsPersistentRuntime["start"]>(async (intent, saveProgress) => {
      const resourceIds = {
        daemon: "fixture-daemon-identity",
        container: "b".repeat(64),
        network: "c".repeat(64),
        volume: "d".repeat(64),
      };
      await saveProgress({
        ...intent,
        ...resourceIds,
        creationStarted: true,
        mutationsSettled: false,
      });
      return { ...intent, ...resourceIds };
    });
    runtime.recover = vi.fn().mockRejectedValue(new Error("not quiescent"));
    await expect(
      new NatsFixtureLifecycle(root, () => Promise.resolve(""), runtime).ensure(),
    ).rejects.toThrow("retained");
    expect(runtime.stop).not.toHaveBeenCalled();
    expect(runtime.recover).toHaveBeenCalledWith(
      expect.objectContaining({ mutationsSettled: false }),
    );
    await expect(readFile(natsRecordPath(root))).rejects.toMatchObject({ code: "ENOENT" });
    expect(JSON.parse(await readFile(natsIntentPath(root), "utf8"))).toMatchObject({
      format: 2,
      container: "b".repeat(64),
      network: "c".repeat(64),
      volume: "d".repeat(64),
      mutationsSettled: false,
    });
  });

  it("preserves settled ownership after cancellation when compensation fails", async () => {
    const { root } = await repository(false);
    const runtime = owner();
    const controller = new AbortController();
    runtime.start = vi.fn<NatsPersistentRuntime["start"]>(async (intent, saveProgress) => {
      const resourceIds = {
        daemon: "fixture-daemon-identity",
        container: "b".repeat(64),
        network: "c".repeat(64),
        volume: "d".repeat(64),
      };
      await saveProgress({
        ...intent,
        ...resourceIds,
        creationStarted: true,
        mutationsSettled: true,
      });
      controller.abort();
      return { ...intent, ...resourceIds };
    });
    runtime.stop = vi.fn().mockRejectedValue(new Error("cleanup unavailable"));
    await expect(
      new NatsFixtureLifecycle(root, () => Promise.resolve(""), runtime).ensure(controller.signal),
    ).rejects.toThrow("retained");
    expect(runtime.stop).toHaveBeenCalledOnce();
    expect(runtime.recover).not.toHaveBeenCalled();
    await expect(readFile(natsRecordPath(root))).rejects.toMatchObject({ code: "ENOENT" });
    expect(JSON.parse(await readFile(natsIntentPath(root), "utf8"))).toMatchObject({
      mutationsSettled: true,
    });
  });
});

describe("Containerlab ownership input validation", () => {
  it.each([
    { directory: "/tmp/foreign-private-material" },
    { topologyPath: "/tmp/foreign.clab.yml" },
    { runtimeDirectory: "/tmp/foreign-lab" },
    { lab: "some-other-lab" },
    { name: "some-other-server" },
    { networkName: "some-other-network" },
    { image: "nats:latest" },
    { daemon: "" },
    { daemon: "unbounded/not-a-daemon" },
    { container: "short-id" },
    { network: "short-id" },
    { volume: "a-named-foreign-volume" },
  ])("refuses foreign or ambiguous ownership: %j", async (patch) => {
    const { root, record } = await repository();
    expect(() => parseNatsFixtureRecord({ ...record, ...patch }, root)).toThrow("invalid");
  });

  it("does not treat a preparation journal with daemon IDs as a never-started deployment", async () => {
    const { root, record } = await repository();
    expect(() =>
      parseNatsFixtureIntent({ ...record, creationStarted: false, mutationsSettled: true }, root),
    ).toThrow("invalid");
    expect(() =>
      parseNatsFixtureIntent({ ...record, creationStarted: true, mutationsSettled: "yes" }, root),
    ).toThrow("invalid");
  });
});
