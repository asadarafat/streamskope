import { execFile } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { afterEach, beforeAll, afterAll, describe, expect, it } from "vitest";

import { InMemoryNatsProfileStore, NatsProfileService } from "../../src/features/nats/application";
import { NATS_SERVER_IMAGES } from "../../tools/dev/nats-fixture/definition";
import { prepareLocalNatsDevelopmentProfile } from "../../tools/dev/nats-fixture/development-profile";
import { NatsFixtureLifecycle } from "../../tools/dev/nats-fixture/lifecycle";
import {
  natsOwnershipRoot,
  natsIntentPath,
  natsRecordPath,
  readPrivateFixtureFile,
  type NatsFixtureRecord,
} from "../../tools/dev/nats-fixture/ownership";

const directories: string[] = [];
const token = "a".repeat(64);
let caDirectory: string;
let caPem: string;

beforeAll(async () => {
  caDirectory = await mkdtemp(join(tmpdir(), "streamskope-nats-unit-ca-"));
  await promisify(execFile)(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      join(caDirectory, "ca-key.pem"),
      "-out",
      join(caDirectory, "ca.pem"),
      "-days",
      "1",
      "-subj",
      "/CN=Ephemeral lifecycle regression CA",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
    ],
    { timeout: 10_000 },
  );
  caPem = await readFile(join(caDirectory, "ca.pem"), "utf8");
});

afterAll(async () => {
  await rm(caDirectory, { recursive: true, force: true });
});

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

async function repository(recorded = true): Promise<{ root: string; record: NatsFixtureRecord }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-nats-lifecycle-"));
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
  const record: NatsFixtureRecord = {
    format: 1,
    name: "streamskope-nats",
    identity: "11111111-1111-4111-8111-111111111111",
    container: "b".repeat(64),
    directory,
    image: NATS_SERVER_IMAGES.arm64,
    port: 14222,
  };
  await writeFile(join(directory, "ca.pem"), caPem, { mode: 0o600 });
  await writeFile(join(directory, "token"), token, { mode: 0o600 });
  if (recorded) await writeFile(natsRecordPath(root), JSON.stringify(record), { mode: 0o600 });
  return { root, record };
}

function inspection(record: NatsFixtureRecord, identity = record.identity): string {
  return `${record.container}|/${record.name}|running|${identity}|nats|${record.image}\n`;
}

describe("persistent Local AIO NATS ownership", () => {
  it("does not claim or mutate an unrecorded container with the fixture name", async () => {
    const { root } = await repository(false);
    const calls: string[][] = [];
    const lifecycle = new NatsFixtureLifecycle(root, (arguments_) => {
      calls.push([...arguments_]);
      return Promise.resolve("foreign-container\n");
    });
    await expect(lifecycle.ensure()).rejects.toThrow("unrecorded container");
    expect(calls.map((call) => call[0])).toEqual(["ps"]);
    await expect(readFile(natsRecordPath(root))).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("refuses destructive cleanup when the daemon ownership UUID differs", async () => {
    const { root, record } = await repository();
    const calls: string[][] = [];
    const lifecycle = new NatsFixtureLifecycle(root, (arguments_) => {
      calls.push([...arguments_]);
      return Promise.resolve(
        arguments_[0] === "ps"
          ? record.container
          : inspection(record, "22222222-2222-4222-8222-222222222222"),
      );
    });
    await expect(lifecycle.stop()).rejects.toThrow("ownership could not be verified");
    expect(calls.map((call) => call[0])).toEqual(["ps", "inspect"]);
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe(token);
    expect(JSON.parse(await readFile(natsRecordPath(root), "utf8"))).toEqual(record);
  });

  it("removes only a verified owned server and its material, and repeat stop is harmless", async () => {
    const { root, record } = await repository();
    const unrelated = join(natsOwnershipRoot(root), "unrelated.txt");
    await writeFile(unrelated, "keep");
    const calls: string[][] = [];
    const lifecycle = new NatsFixtureLifecycle(root, (arguments_) => {
      calls.push([...arguments_]);
      return Promise.resolve(
        arguments_[0] === "ps"
          ? record.container
          : arguments_[0] === "inspect"
            ? inspection(record)
            : "",
      );
    });
    await lifecycle.stop();
    await lifecycle.stop();
    expect(calls.map((call) => call[0])).toEqual(["ps", "inspect", "rm"]);
    expect(calls[2]).toEqual(["rm", "--force", "--volumes", record.container]);
    expect(await readFile(unrelated, "utf8")).toBe("keep");
    await expect(readFile(join(record.directory, "token"))).rejects.toMatchObject({
      code: "ENOENT",
    });
    await expect(readFile(natsRecordPath(root))).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("finishes interrupted stop after successful daemon confirmation that the full ID and name are absent", async () => {
    const { root, record } = await repository();
    const calls: string[][] = [];
    const lifecycle = new NatsFixtureLifecycle(root, (arguments_) => {
      calls.push([...arguments_]);
      return Promise.resolve("");
    });
    await lifecycle.stop();
    expect(calls.map((call) => call[0])).toEqual(["ps", "ps"]);
    expect(calls[0]).toContain(`id=${record.container}`);
    expect(calls[1]).toContain(`name=^/${record.name}$`);
    await expect(readFile(natsRecordPath(root))).rejects.toMatchObject({ code: "ENOENT" });
    await expect(readFile(join(record.directory, "token"))).rejects.toMatchObject({
      code: "ENOENT",
    });
    await lifecycle.stop();
    expect(calls).toHaveLength(2);
  });

  it("does not mistake a failed daemon query for an absent owned container", async () => {
    const { root, record } = await repository();
    await expect(
      new NatsFixtureLifecycle(root, () => Promise.reject(new Error("daemon unavailable"))).stop(),
    ).rejects.toThrow("daemon unavailable");
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe(token);
    expect(JSON.parse(await readFile(natsRecordPath(root), "utf8"))).toEqual(record);
  });

  it("keeps residual ownership when an absent full ID has a foreign container occupying its name", async () => {
    const { root, record } = await repository();
    const calls: string[][] = [];
    await expect(
      new NatsFixtureLifecycle(root, (arguments_) => {
        calls.push([...arguments_]);
        return Promise.resolve(arguments_.includes(`id=${record.container}`) ? "" : "c".repeat(64));
      }).stop(),
    ).rejects.toThrow("occupied by another container");
    expect(calls.map((call) => call[0])).toEqual(["ps", "ps"]);
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe(token);
  });

  it("recovers an interrupted start using its persisted UUID and cleans only its matching container", async () => {
    const { root, record } = await repository(false);
    const { container, ...intent } = record;
    await writeFile(natsIntentPath(root), JSON.stringify(intent), { mode: 0o600 });
    const calls: string[][] = [];
    const lifecycle = new NatsFixtureLifecycle(root, (arguments_) => {
      calls.push([...arguments_]);
      return Promise.resolve(
        arguments_[0] === "ps" ? container : arguments_[0] === "inspect" ? inspection(record) : "",
      );
    });
    await lifecycle.stop();
    expect(calls.map((call) => call[0])).toEqual(["ps", "inspect", "rm"]);
    expect(calls[2]).toEqual(["rm", "--force", "--volumes", container]);
    await expect(readFile(natsIntentPath(root))).rejects.toMatchObject({ code: "ENOENT" });
    await expect(readFile(join(record.directory, "token"))).rejects.toMatchObject({
      code: "ENOENT",
    });
    await lifecycle.stop();
    expect(calls).toHaveLength(3);
  });

  it("preserves a pending journal when a same-name container has a different ownership UUID", async () => {
    const { root, record } = await repository(false);
    const { container, ...intent } = record;
    await writeFile(natsIntentPath(root), JSON.stringify(intent), { mode: 0o600 });
    const calls: string[][] = [];
    await expect(
      new NatsFixtureLifecycle(root, (arguments_) => {
        calls.push([...arguments_]);
        return Promise.resolve(
          arguments_[0] === "ps"
            ? container
            : inspection(record, "22222222-2222-4222-8222-222222222222"),
        );
      }).stop(),
    ).rejects.toThrow("ownership could not be verified");
    expect(calls.map((call) => call[0])).toEqual(["ps", "inspect"]);
    expect(JSON.parse(await readFile(natsIntentPath(root), "utf8"))).toEqual(intent);
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe(token);
  });

  it("retains uncertain creation evidence across an empty daemon query until the matching late container appears", async () => {
    const { root, record } = await repository(false);
    const { container, ...prepared } = record;
    const intent = { ...prepared, creationStarted: true };
    await writeFile(natsIntentPath(root), JSON.stringify(intent), { mode: 0o600 });
    let appeared = false;
    const calls: string[][] = [];
    const lifecycle = new NatsFixtureLifecycle(root, (arguments_) => {
      calls.push([...arguments_]);
      return Promise.resolve(
        !appeared
          ? ""
          : arguments_[0] === "ps"
            ? container
            : arguments_[0] === "inspect"
              ? inspection(record)
              : "",
      );
    });
    await expect(lifecycle.stop()).rejects.toThrow("completion is unconfirmed");
    expect(calls.map((call) => call[0])).toEqual(["ps"]);
    expect(JSON.parse(await readFile(natsIntentPath(root), "utf8"))).toEqual(intent);
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe(token);
    appeared = true;
    await lifecycle.stop();
    expect(calls.map((call) => call[0])).toEqual(["ps", "ps", "inspect", "rm"]);
    await expect(readFile(natsIntentPath(root))).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("cleans private preparation only when the journal confirms Docker creation never started", async () => {
    const { root, record } = await repository(false);
    const { container, ...prepared } = record;
    await writeFile(natsIntentPath(root), JSON.stringify({ ...prepared, creationStarted: false }), {
      mode: 0o600,
    });
    const calls: string[][] = [];
    await new NatsFixtureLifecycle(root, (arguments_) => {
      calls.push([...arguments_]);
      return Promise.resolve("");
    }).stop();
    expect(calls.map((call) => call[0])).toEqual(["ps"]);
    expect(calls[0]).not.toContain(`id=${container}`);
    await expect(readFile(natsIntentPath(root))).rejects.toMatchObject({ code: "ENOENT" });
    await expect(readFile(join(record.directory, "token"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("finishes interrupted startup cleanup when a successfully returned full container ID is confirmed absent", async () => {
    const { root, record } = await repository(false);
    await writeFile(natsIntentPath(root), JSON.stringify({ ...record, creationStarted: true }), {
      mode: 0o600,
    });
    const calls: string[][] = [];
    await new NatsFixtureLifecycle(root, (arguments_) => {
      calls.push([...arguments_]);
      return Promise.resolve("");
    }).stop();
    expect(calls.map((call) => call[0])).toEqual(["ps", "ps"]);
    expect(calls[0]).toContain(`id=${record.container}`);
    await expect(readFile(natsIntentPath(root))).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("rejects a cleanup directory outside this checkout before contacting Docker", async () => {
    const { root, record } = await repository();
    const foreign = join(root, "foreign-private-material");
    await mkdir(foreign);
    await writeFile(join(foreign, "keep.txt"), "keep");
    await writeFile(natsRecordPath(root), JSON.stringify({ ...record, directory: foreign }));
    let contacted = false;
    const lifecycle = new NatsFixtureLifecycle(root, () => {
      contacted = true;
      return Promise.resolve("");
    });
    await expect(lifecycle.stop()).rejects.toThrow("ownership record is invalid");
    expect(contacted).toBe(false);
    expect(await readFile(join(foreign, "keep.txt"), "utf8")).toBe("keep");
  });

  it("requires explicit recovery for changed source settings while preserving owned material", async () => {
    const { root, record } = await repository();
    await writeFile(
      join(root, "aio-nats", "fixture.config.json"),
      JSON.stringify({
        port: 14223,
        subject: "streamskope.fixture.events",
        payload: {},
      }),
    );
    let contacted = false;
    await expect(
      new NatsFixtureLifecycle(root, () => {
        contacted = true;
        return Promise.resolve("");
      }).ensure(),
    ).rejects.toThrow("settings changed");
    expect(contacted).toBe(false);
    expect(await readFile(join(record.directory, "token"), "utf8")).toBe(token);
  });

  it("rejects public, symbolic-link and oversized credential files", async () => {
    const { record } = await repository();
    const path = join(record.directory, "token");
    await chmod(path, 0o644);
    await expect(readPrivateFixtureFile(path, 128)).rejects.toThrow("not private");
    await chmod(path, 0o600);
    const link = join(record.directory, "linked-token");
    await symlink(path, link);
    await expect(readPrivateFixtureFile(link, 128)).rejects.toThrow("not private");
    await expect(readPrivateFixtureFile(path, 32)).rejects.toThrow("not private");
  });
});

describe("Local AIO NATS session-only development profile", () => {
  it("seeds host-owned credentials once and exposes only protected summaries", async () => {
    const { root } = await repository();
    const store = new InMemoryNatsProfileStore();
    expect(await prepareLocalNatsDevelopmentProfile(store, root)).toBe("seeded");
    const records = await store.load();
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      id: "local-aio-nats",
      name: "Local AIO NATS",
      revision: 1,
      servers: ["nats://127.0.0.1:14222"],
      authentication: { mode: "token", token },
      tls: { mode: "tls", caPem },
    });
    const summary = JSON.stringify(await new NatsProfileService(store).list());
    expect(summary).not.toContain(token);
    expect(summary).not.toContain("BEGIN CERTIFICATE");
    expect(await prepareLocalNatsDevelopmentProfile(store, root)).toBe("unchanged");
    expect(await store.load()).toEqual(records);
  });

  it("leaves a missing fixture unconfigured and preserves existing user profiles", async () => {
    const { root } = await repository(false);
    const store = new InMemoryNatsProfileStore();
    expect(await prepareLocalNatsDevelopmentProfile(store, root)).toBe("unavailable");
    expect(await store.load()).toEqual([]);
    await store.save([
      {
        id: "user-profile",
        revision: 1,
        name: "Existing user profile",
        servers: ["nats://localhost:4222"],
        authentication: { mode: "none" },
        tls: { mode: "plaintext" },
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      },
    ]);
    const before = await store.load();
    expect(await prepareLocalNatsDevelopmentProfile(store, root)).toBe("unchanged");
    expect(await store.load()).toEqual(before);
  });
});
