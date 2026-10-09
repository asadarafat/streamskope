import { spawn } from "node:child_process";
import { once } from "node:events";
import { access, chmod, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { openPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { AtomicKafkaProfileFileStore } from "../../src/platform/node/kafka-profile-file-store";
import { AtomicKafkaQueryFileStore } from "../../src/platform/node/kafka-query-file-store";
import { browserInstallerFixture } from "../support/browser-installer-fixture";

type Fixture = Awaited<ReturnType<typeof browserInstallerFixture>>;
const fixtures: Fixture[] = [];

afterEach(async () => {
  for (const fixture of fixtures.splice(0)) await fixture.cleanup();
});

async function fixture(): Promise<Fixture> {
  const value = await browserInstallerFixture();
  fixtures.push(value);
  return value;
}

function hostMutations(calls: Awaited<ReturnType<Fixture["calls"]>>): unknown[] {
  return calls.filter(
    ({ command, args }) =>
      ["apt-get", "useradd", "systemctl", "dpkg-query"].includes(command) ||
      (["clab", "containerlab"].includes(command) && args.includes("deploy")) ||
      (command === "docker" && args.some((arg) => ["stop", "rm", "start"].includes(arg))),
  );
}

describe.skipIf(process.platform !== "linux")("browser maintenance installer admission", () => {
  it("reports its own fixture watchdog instead of presenting a timeout as an installer refusal", async () => {
    const host = await fixture();
    const script = join(host.root, "wait-for-watchdog.sh");
    await writeFile(script, "exec sleep 30\n");
    const result = host.run(script, [], { timeoutMs: 100 });
    await expect(result).rejects.toThrow("Installer fixture watchdog terminated");
    await expect(result).rejects.toMatchObject({
      cause: { code: null, killed: true, signal: "SIGTERM" },
    });
  });

  it.each(["check", "upgrade", "rollback", "recover"])(
    "refuses %s without an existing installation and does not bootstrap or create data",
    async (operation) => {
      const host = await fixture();
      const installer = await host.installer();
      const result = await host.run(installer.file, [operation]);
      expect(result.exitCode).not.toBe(0);
      expect(hostMutations(await host.calls())).toEqual([]);
      await expect(access(host.state)).rejects.toThrow();
      await expect(access(host.data)).rejects.toThrow();
    },
  );

  it.each([["--upgrade"], ["check", "upgrade"], ["install"], ["check", "--local"]])(
    "rejects unsupported operation arguments %j before changing host state",
    async (...args) => {
      const host = await fixture();
      const installer = await host.installer();
      const result = await host.run(installer.file, args);
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain("one operation");
      expect(await host.calls()).toEqual([]);
      await expect(access(host.state)).rejects.toThrow();
    },
  );

  it("does not install missing prerequisites during explicit maintenance", async () => {
    const host = await fixture();
    const installer = await host.installer();
    await rm(join(host.root, "bin/docker"));
    const result = await host.run(installer.file, ["check"]);
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain("no packages were installed");
    expect(hostMutations(await host.calls())).toEqual([]);
    await expect(access(host.state)).rejects.toThrow();
  });

  it("refuses ordinary resume with a pending journal before prerequisite installation", async () => {
    const host = await fixture();
    const installer = await host.installer();
    await mkdir(host.state, { mode: 0o700 });
    const journal = "private retained transaction bytes\n";
    await writeFile(join(host.state, "maintenance.json"), journal, { mode: 0o600 });
    await rm(join(host.root, "bin/docker"));
    const result = await host.run(installer.file);
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain("with recover");
    expect(await readFile(join(host.state, "maintenance.json"), "utf8")).toBe(journal);
    expect(hostMutations(await host.calls())).toEqual([]);
    await expect(access(host.data)).rejects.toThrow();
  });

  it("uses the existing installer flock without truncating or replacing its inode", async () => {
    const host = await fixture();
    const installer = await host.installer();
    const installed = await host.run(installer.file);
    expect(installed.exitCode, installed.stderr).toBe(0);
    const lock = join(host.state, "installer.lock");
    const marker = "existing installer lock bytes\n";
    await writeFile(lock, marker);
    await chmod(lock, 0o600);
    const before = await stat(lock);
    const calls = (await host.calls()).length;
    const holder = spawn(
      "python3",
      [
        "-c",
        "import fcntl,sys,time; f=open(sys.argv[1],'r+'); fcntl.flock(f,fcntl.LOCK_EX); print('ready',flush=True); time.sleep(30)",
        lock,
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    try {
      const [ready] = (await once(holder.stdout, "data")) as [Buffer];
      expect(ready.toString()).toContain("ready");
      const blocked = await host.run(installer.file, ["check"]);
      expect(blocked.exitCode).not.toBe(0);
      expect(blocked.stderr).toContain("Another StreamSkope installer");
      expect(hostMutations((await host.calls()).slice(calls))).toEqual([]);
      expect(await readFile(lock, "utf8")).toBe(marker);
      expect((await stat(lock)).ino).toBe(before.ino);
    } finally {
      holder.kill("SIGTERM");
      await once(holder, "exit");
    }
  });

  it("refuses a missing maintenance lock without creating a replacement", async () => {
    const host = await fixture();
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).toBe(0);
    const record = await readFile(join(host.state, "installation.json"));
    await rm(join(host.state, "installer.lock"));
    const calls = (await host.calls()).length;
    const blocked = await host.run(installer.file, ["upgrade"]);
    expect(blocked.exitCode).not.toBe(0);
    await expect(access(join(host.state, "installer.lock"))).rejects.toThrow();
    expect(await readFile(join(host.state, "installation.json"))).toEqual(record);
    expect(hostMutations((await host.calls()).slice(calls))).toEqual([]);
  });

  it("resumes schema2 current identity without selecting previous or a newer installer", async () => {
    const host = await fixture();
    const original = await host.installer("0.10.1");
    expect((await host.run(original.file)).exitCode).toBe(0);
    const recordPath = join(host.state, "installation.json");
    const old = JSON.parse(await readFile(recordPath, "utf8")) as Record<string, unknown>;
    const record = `${JSON.stringify({
      schemaVersion: 2,
      current: {
        version: old.version,
        sourceRevision: old.sourceRevision,
        topologySha256: old.topologySha256,
        manifestSha256: old.manifestSha256,
      },
      previous: null,
      uid: old.uid,
      gid: old.gid,
      home: old.home,
      operatorUid: old.operatorUid,
      port: old.port,
    })}\n`;
    await writeFile(recordPath, record);
    const newer = await host.installer("0.10.2");
    const before = (await host.calls()).length;
    const resumed = await host.run(newer.file);
    expect(resumed.exitCode, resumed.stderr).toBe(0);
    expect(`${resumed.stdout}${resumed.stderr}`).toContain("0.10.1");
    expect(await readFile(recordPath, "utf8")).toBe(record);
    expect(hostMutations((await host.calls()).slice(before))).toEqual([]);
    expect(
      (await host.calls())
        .slice(before)
        .some(({ args }) => args.some((arg) => arg.includes("/v0.10.2/"))),
    ).toBe(false);
  });
  it("finishes a stale committed journal without pulling images or rechecking changed live data", async () => {
    const { host, next } = await initialized();
    const upgraded = await host.run(next.file, ["upgrade"]);
    expect(upgraded.exitCode, upgraded.stderr).toBe(0);
    const [generation] = await generations(host);
    const archived = await readFile(
      join(host.state, "backups", generation ?? "", "attempt-1", "transaction.json"),
    );
    await writeFile(join(host.state, "maintenance.json"), archived, { mode: 0o600 });
    await profiles(host, "unlock", true);
    const record = await readFile(join(host.state, "installation.json"));
    const changed = await dataBytes(host);
    for (const version of ["0.10.1", "0.10.2"]) {
      await rm(join(host.state, `streamskope-${version}-container.json`));
    }
    await host.control({ failedDownload: "SHA256SUMS" });
    const calls = (await host.calls()).length;
    const recovered = await host.run(next.file, ["recover"]);
    expect(recovered.exitCode, recovered.stderr).toBe(0);
    expect(recovered.stdout).toContain("Maintenance recovery completed");
    expect(await readFile(join(host.state, "installation.json"))).toEqual(record);
    expect(await dataBytes(host)).toEqual(changed);
    expect(
      (await host.calls())
        .slice(calls)
        .filter(({ command }) => ["docker", "curl", "containerlab", "clab"].includes(command)),
    ).toEqual([]);
    await expect(access(join(host.state, "maintenance.json"))).rejects.toThrow();
    assertPrivate(recovered);
  }, 20_000);

  it("preserves an unrecorded candidate after a deploy crash instead of adopting it by name", async () => {
    const { host, next } = await initialized();
    const record = await readFile(join(host.state, "installation.json"));
    await host.control({ crashAfter: "deploy" });
    const interrupted = await host.run(next.file, ["upgrade"], { timeoutMs: 30_000 });
    expect(interrupted.exitCode, interrupted.stderr).toBe(137);
    expect(
      JSON.parse(await readFile(join(host.state, "maintenance.json"), "utf8")),
      JSON.stringify(interrupted),
    ).toMatchObject({ phase: "retired", candidateId: null });
    const owner = (await control(host)).container;
    const calls = (await host.calls()).length;
    const recovered = await host.run(next.file, ["recover"]);
    expect(recovered.exitCode).not.toBe(0);
    expect(hostMutations((await host.calls()).slice(calls))).toEqual([]);
    expect((await control(host)).container).toEqual(owner);
    expect(await readFile(join(host.state, "installation.json"))).toEqual(record);
    await expect(access(join(host.state, "maintenance.json"))).resolves.toBeUndefined();
    expect(await generations(host)).toHaveLength(1);
  }, 45_000);
});

const secret = "private-maintenance-passphrase";

async function profiles(host: Fixture, mode: "create" | "unlock", managed = false): Promise<void> {
  const vault = await openPassphraseVault({ dataRoot: host.data, passphrase: secret, mode });
  try {
    await new AtomicKafkaProfileFileStore(
      vault.paths.kafkaProfiles,
      vault.protector,
      vault.capability,
    ).commit([
      {
        id: "private-profile",
        name: "Private maintenance profile",
        brokers: ["broker.private.invalid:9092"],
        transport: "plaintext",
        createdAt: "2026-10-08T12:00:00.000Z",
        updatedAt: mode === "create" ? "2026-10-08T12:00:00.000Z" : "2026-10-08T13:00:00.000Z",
        ...(managed
          ? {
              source: {
                kind: "plugin" as const,
                pluginId: "streamskope.eda",
                version: 1,
                data: {},
              },
            }
          : {}),
      },
    ]);
  } finally {
    await vault.lock();
  }
  await rm(join(host.data, "setup-code"), { force: true });
}

async function initialized(): Promise<{
  host: Fixture;
  original: Awaited<ReturnType<Fixture["installer"]>>;
  next: Awaited<ReturnType<Fixture["installer"]>>;
}> {
  const host = await fixture();
  const original = await host.installer("0.10.1");
  const result = await host.run(original.file);
  expect(result.exitCode, result.stderr).toBe(0);
  await profiles(host, "create");
  return { host, original, next: await host.installer("0.10.2") };
}

async function dataBytes(host: Fixture): Promise<ReadonlyMap<string, Buffer>> {
  const values = await Promise.all(
    ["vault.json", "vault.lock", "kafka-profiles.json"].map(
      async (file) => [file, await readFile(join(host.data, file))] as const,
    ),
  );
  return new Map(values);
}

async function generations(host: Fixture): Promise<string[]> {
  return (await readdir(join(host.state, "backups"))).sort();
}

async function control(host: Fixture): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(join(host.root, "control.json"), "utf8")) as Record<
    string,
    unknown
  >;
}

function assertPrivate(result: Awaited<ReturnType<Fixture["run"]>>): void {
  for (const value of [
    secret,
    "private-profile",
    "broker.private.invalid",
    "Private maintenance profile",
  ])
    expect(`${result.stdout}${result.stderr}`).not.toContain(value);
}

describe.skipIf(process.platform !== "linux")(
  "browser maintenance transactions through the installer",
  () => {
    it("checks a real initialized data tree without stopping the host or changing authoritative bytes", async () => {
      const { host, next } = await initialized();
      const state = await readFile(join(host.state, "installation.json"));
      const data = await dataBytes(host);
      const calls = (await host.calls()).length;
      const result = await host.run(next.file, ["check"]);
      expect(result.exitCode, result.stderr).toBe(0);
      expect(result.stdout).toContain("v0.10.2 compatibility check completed");
      expect(result.stdout).toContain("running v0.10.1 workbench was not changed");
      expect(result.stdout).toContain("http://127.0.0.1:");
      expect(result.stdout).not.toContain('"schemaVersion"');
      expect(await readFile(join(host.state, "installation.json"))).toEqual(state);
      expect(await dataBytes(host)).toEqual(data);
      expect(hostMutations((await host.calls()).slice(calls))).toEqual([]);
      await expect(access(join(host.state, "maintenance.json"))).rejects.toThrow();
      await expect(access(join(host.state, "backups"))).rejects.toThrow();
      assertPrivate(result);
    });

    it("upgrades, repeats and rolls back with complete retained backups and the same held data lease", async () => {
      const { host, next } = await initialized();
      const data = await dataBytes(host);
      const lease = await stat(join(host.data, "vault.lock"));
      await host.control({ verifyLeaseDuringDeploy: true });
      const result = await host.run(next.file, ["upgrade"]);
      expect(result.exitCode, result.stderr).toBe(0);
      expect(result.stdout).toContain("Upgrade completed. StreamSkope v0.10.2");
      expect(result.stdout).toContain("unlock your existing vault");
      expect((await control(host)).leaseContenderBlocked).toBe(true);
      expect((await stat(join(host.data, "vault.lock"))).ino).toBe(lease.ino);
      expect(await dataBytes(host)).toEqual(data);
      const record = JSON.parse(
        await readFile(join(host.state, "installation.json"), "utf8"),
      ) as unknown;
      expect(record).toMatchObject({
        schemaVersion: 2,
        current: { version: "0.10.2" },
        previous: { version: "0.10.1" },
      });
      const [generation] = await generations(host);
      expect(generation).toMatch(/^[a-f0-9-]{36}$/);
      const backup = join(host.state, "backups", generation ?? "", "attempt-1");
      for (const [file, content] of data) {
        expect(await readFile(join(backup, "data", file))).toEqual(content);
        expect((await stat(join(backup, "data", file))).mode & 0o777).toBe(
          (await stat(join(host.data, file))).mode & 0o777,
        );
      }
      await expect(access(join(backup, "inventory.json"))).resolves.toBeUndefined();
      await expect(access(join(backup, "transaction.json"))).resolves.toBeUndefined();
      await expect(access(join(host.state, "maintenance.json"))).rejects.toThrow();
      const calls = (await host.calls()).length;
      const repeated = await host.run(next.file, ["upgrade"]);
      expect(repeated.exitCode, repeated.stderr).toBe(0);
      expect(repeated.stdout).toContain("already selected");
      expect(hostMutations((await host.calls()).slice(calls))).toEqual([]);
      expect(await generations(host)).toEqual([generation]);
      const rolledBack = await host.run(next.file, ["rollback"]);
      expect(rolledBack.exitCode, rolledBack.stderr).toBe(0);
      expect(rolledBack.stdout).toContain("Rollback completed. StreamSkope v0.10.1");
      expect(await dataBytes(host)).toEqual(data);
      expect(await generations(host)).toHaveLength(2);
      expect((await stat(join(host.data, "vault.lock"))).ino).toBe(lease.ino);
      const resumed = await host.run(next.file);
      expect(resumed.exitCode, resumed.stderr).toBe(0);
      expect(`${resumed.stdout}${resumed.stderr}`).toContain("0.10.1");
      assertPrivate(result);
      assertPrivate(rolledBack);
    }, 30_000);

    it("retains all four investigation-library generations and protected siblings in the complete ownership-held backup", async () => {
      const { host, next } = await initialized();
      const queries = join(host.data, "queries");
      await mkdir(queries, { mode: 0o700 });
      const source = `${JSON.stringify(
        {
          schemaVersion: 1,
          queries: [
            {
              id: "incident",
              name: "Old investigation",
              configuration: {
                schemaVersion: 1,
                request: { topic: "orders", mode: "earliest", maxMessages: 10 },
              },
            },
          ],
        },
        null,
        2,
      )}\n`;
      const path = join(queries, "kafka-queries.json");
      await writeFile(`${path}.pre-views-v1`, source, { mode: 0o600 });
      // A genuine v2 disk document may compact the exact default presentation.
      const versionTwo = source.replace('"schemaVersion": 1,', '"schemaVersion": 2,');
      await writeFile(`${path}.pre-records-v2`, versionTwo, { mode: 0o600 });
      const versionThree = source.replace('"schemaVersion": 1,', '"schemaVersion": 3,');
      await writeFile(path, versionThree, { mode: 0o600 });
      const store = new AtomicKafkaQueryFileStore(path);
      const {
        queries: [legacy],
      } = await store.load();
      expect(legacy).toBeDefined();
      await store.commit({
        queries: [
          {
            ...legacy!,
            name: "Updated investigation",
            view: {
              schemaVersion: 1,
              destination: { kind: "topic", workspace: "monitor" },
              messages: {
                visibleColumns: ["key", "preview"],
                columnWidths: [],
                inspectorWidth: 400,
                filtersOpen: true,
              },
            },
          },
        ],
        topics: [],
      });
      const lease = await stat(join(host.data, "vault.lock"));
      const expected = await dataBytes(host);
      for (const file of [
        "queries/kafka-queries.json",
        "queries/kafka-queries.json.pre-views-v1",
        "queries/kafka-queries.json.pre-records-v2",
        "queries/kafka-queries.json.pre-catalog-v3",
      ])
        (expected as Map<string, Buffer>).set(file, await readFile(join(host.data, file)));
      expect(expected.get("queries/kafka-queries.json.pre-views-v1")?.toString()).toBe(source);
      expect(expected.get("queries/kafka-queries.json.pre-records-v2")?.toString()).toBe(
        versionTwo,
      );
      expect(expected.get("queries/kafka-queries.json.pre-catalog-v3")?.toString()).toBe(
        versionThree,
      );
      await host.control({ verifyLeaseDuringDeploy: true });
      const result = await host.run(next.file, ["upgrade"]);
      expect(result.exitCode, result.stderr).toBe(0);
      expect((await control(host)).leaseContenderBlocked).toBe(true);
      expect((await stat(join(host.data, "vault.lock"))).ino).toBe(lease.ino);
      const [generation] = await generations(host);
      const backup = join(host.state, "backups", generation ?? "", "attempt-1", "data");
      for (const [file, content] of expected) {
        expect(await readFile(join(backup, file))).toEqual(content);
        expect(await readFile(join(host.data, file))).toEqual(content);
        expect((await stat(join(backup, file))).mode & 0o777).toBe(
          (await stat(join(host.data, file))).mode & 0o777,
        );
      }
      assertPrivate(result);
    }, 30_000);

    it("refuses a lease file without an initialized vault before any downtime", async () => {
      const host = await fixture();
      const original = await host.installer("0.10.1");
      expect((await host.run(original.file)).exitCode).toBe(0);
      await writeFile(join(host.data, "vault.lock"), "", { mode: 0o600 });
      const next = await host.installer("0.10.2");
      const calls = (await host.calls()).length;
      const result = await host.run(next.file, ["upgrade"]);
      expect(result.exitCode).not.toBe(0);
      expect(hostMutations((await host.calls()).slice(calls))).toEqual([]);
      await expect(access(join(host.state, "maintenance.json"))).rejects.toThrow();
      await expect(access(join(host.data, "vault.json"))).rejects.toThrow();
    });

    it.each([{ stopExitCode: 137 }, { stopExitCode: 1 }, { stopOomKilled: true }])(
      "never removes the old container or advances its record after unconfirmed stop %j",
      async (failure) => {
        const { host, next } = await initialized();
        const record = await readFile(join(host.state, "installation.json"));
        const data = await dataBytes(host);
        await host.control(failure);
        const calls = (await host.calls()).length;
        const result = await host.run(next.file, ["upgrade"]);
        expect(result.exitCode).not.toBe(0);
        expect(result.stderr).toContain("clean stop");
        expect(await readFile(join(host.state, "installation.json"))).toEqual(record);
        expect(await dataBytes(host)).toEqual(data);
        expect(
          (await host.calls())
            .slice(calls)
            .some(({ command, args }) => command === "docker" && args.includes("rm")),
        ).toBe(false);
        const journal = JSON.parse(
          await readFile(join(host.state, "maintenance.json"), "utf8"),
        ) as unknown;
        expect(journal).toMatchObject({ phase: "intent", backup: null });
        assertPrivate(result);
      },
    );

    it("does not bypass an independently held vault lease after the original host stops", async () => {
      const { host, next } = await initialized();
      const record = await readFile(join(host.state, "installation.json"));
      const vault = await openPassphraseVault({
        dataRoot: host.data,
        passphrase: secret,
        mode: "unlock",
      });
      try {
        const result = await host.run(next.file, ["upgrade"]);
        expect(result.exitCode).not.toBe(0);
        expect(result.stderr).toContain("vault is still in use");
        expect(await readFile(join(host.state, "installation.json"))).toEqual(record);
        expect(
          (await host.calls()).some(
            ({ command, args }) => command === "docker" && args.includes("rm"),
          ),
        ).toBe(false);
      } finally {
        await vault.lock();
      }
    });

    it.each(["stop", "remove"] as const)(
      "recovers the exact journal target after the helper is killed following %s",
      async (crashAfter) => {
        const { host, next } = await initialized();
        const data = await dataBytes(host);
        await host.control({ crashAfter });
        const interrupted = await host.run(next.file, ["upgrade"]);
        expect(interrupted.exitCode).not.toBe(0);
        const journal = JSON.parse(
          await readFile(join(host.state, "maintenance.json"), "utf8"),
        ) as unknown;
        expect(journal).toMatchObject({ phase: crashAfter === "stop" ? "intent" : "retiring" });
        const later = await host.installer("0.10.3");
        const calls = (await host.calls()).length;
        const recovered = await host.run(later.file, ["recover"]);
        expect(recovered.exitCode, recovered.stderr).toBe(0);
        expect(recovered.stdout).toContain("StreamSkope v0.10.2 is selected");
        expect(await dataBytes(host)).toEqual(data);
        expect(
          (await host.calls())
            .slice(calls)
            .some(({ args }) => args.some((arg) => arg.includes("/v0.10.3/"))),
        ).toBe(false);
        await expect(access(join(host.state, "maintenance.json"))).rejects.toThrow();
        assertPrivate(recovered);
      },
      20_000,
    );

    it("rechecks newly managed data before stopping a still-running host during intent recovery", async () => {
      const { host, next } = await initialized();
      await host.control({ crashAfter: "before-stop" });
      expect((await host.run(next.file, ["upgrade"])).exitCode).not.toBe(0);
      await profiles(host, "unlock", true);
      const data = await dataBytes(host);
      const calls = (await host.calls()).length;
      const recovered = await host.run(next.file, ["recover"]);
      expect(recovered.exitCode).not.toBe(0);
      expect(hostMutations((await host.calls()).slice(calls))).toEqual([]);
      expect(await dataBytes(host)).toEqual(data);
      expect(await control(host)).toMatchObject({ container: { State: { Running: true } } });
      assertPrivate(recovered);
    });

    it("does not commit or restore old bytes after a post-deploy crash permits legitimate data changes", async () => {
      const { host, next } = await initialized();
      const record = await readFile(join(host.state, "installation.json"));
      await host.control({ crashAfter: "status" });
      const interrupted = await host.run(next.file, ["upgrade"]);
      expect(interrupted.exitCode).not.toBe(0);
      expect(
        JSON.parse(await readFile(join(host.state, "maintenance.json"), "utf8")),
      ).toMatchObject({ phase: "deployed" });
      await profiles(host, "unlock");
      const changed = await dataBytes(host);
      const recovered = await host.run(next.file, ["recover"]);
      expect(recovered.exitCode).not.toBe(0);
      expect(await readFile(join(host.state, "installation.json"))).toEqual(record);
      expect(await dataBytes(host)).toEqual(changed);
      await expect(access(join(host.state, "maintenance.json"))).resolves.toBeUndefined();
      assertPrivate(recovered);
    });
  },
);
