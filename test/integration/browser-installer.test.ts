import { access, chmod, mkdir, readFile, stat, symlink, writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:net";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { renderBrowserWorkbenchInstaller } from "../../tools/package/browser-installer";
import {
  browserInstallerFixture,
  INSTALL_SETUP_CODE,
  INSTALL_SOURCE,
  type InstallerControl,
} from "../support/browser-installer-fixture";

type Fixture = Awaited<ReturnType<typeof browserInstallerFixture>>;
const fixtures: Fixture[] = [];
const listeners: Server[] = [];

afterEach(async () => {
  for (const listener of listeners.splice(0))
    await new Promise<void>((accept) => listener.close(() => accept()));
  for (const fixture of fixtures.splice(0)) await fixture.cleanup();
});

async function fixture(control?: InstallerControl): Promise<Fixture> {
  const value = await browserInstallerFixture(control);
  fixtures.push(value);
  return value;
}

const deployments = (
  calls: Awaited<ReturnType<Fixture["calls"]>>,
): Awaited<ReturnType<Fixture["calls"]>> =>
  calls.filter(
    (call) => ["containerlab", "clab"].includes(call.command) && call.args.includes("deploy"),
  );

async function occupiedPort(port = 0): Promise<number> {
  const listener = createServer();
  await new Promise<void>((accept, reject) => {
    listener.once("error", reject);
    listener.listen(port, "127.0.0.1", () => accept());
  });
  listeners.push(listener);
  return (listener.address() as { port: number }).port;
}

describe.skipIf(process.platform !== "linux")("released browser installer", () => {
  it("rejects missing Docker on an unsupported OS before package or deployment changes", async () => {
    const host = await fixture({ missingDocker: true });
    await writeFile(join(host.root, "os-release"), 'ID=alpine\nVERSION_ID="3.20"\n');
    const installer = await host.installer();
    const result = await host.run(installer.file);
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toMatch(/Ubuntu|Debian|unsupported/i);
    expect(
      await access(host.state).then(
        () => true,
        () => false,
      ),
    ).toBe(false);
    expect(
      await access(join(host.root, "etc")).then(
        () => true,
        () => false,
      ),
    ).toBe(false);
    expect(
      (await host.calls()).some((call) =>
        ["apt-get", "systemctl", "useradd"].includes(call.command),
      ),
    ).toBe(false);
    expect(deployments(await host.calls())).toHaveLength(0);
  });

  it("preserves an existing container runtime when Docker prerequisite installation conflicts", async () => {
    const host = await fixture({ missingDocker: true, installedPackages: ["containerd", "runc"] });
    const installer = await host.installer();
    const result = await host.run(installer.file);
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toMatch(/containerd|runc|conflict/i);
    expect((await host.calls()).some((call) => call.command === "dpkg-query")).toBe(true);
    expect(
      await access(host.state).then(
        () => true,
        () => false,
      ),
    ).toBe(false);
    expect(
      await access(join(host.root, "etc")).then(
        () => true,
        () => false,
      ),
    ).toBe(false);
    expect(
      (await host.calls()).some((call) =>
        ["apt-get", "systemctl", "useradd"].includes(call.command),
      ),
    ).toBe(false);
    expect(
      (await host.calls()).some((call) =>
        call.args.some((argument) => ["purge", "remove"].includes(argument)),
      ),
    ).toBe(false);
  });

  it("deploys verified release bytes with the caller's private data and keeps the setup code off captured output", async () => {
    const host = await fixture();
    const installer = await host.installer();
    const result = await host.run(installer.file);
    expect(result.exitCode, result.stderr).toBe(0);
    expect(result.stdout).toMatch(/http:\/\/127\.0\.0\.1:\d+/);
    expect(`${result.stdout}${result.stderr}`).not.toContain(INSTALL_SETUP_CODE);
    expect(deployments(await host.calls())).toHaveLength(1);
    const state = await readFile(join(host.state, "installation.json"), "utf8");
    expect(state).toContain(installer.version);
    expect(
      await readFile(join(host.state, `streamskope-${installer.version}.clab.yml`), "utf8"),
    ).toBe(installer.topology);
    expect(state).toContain(INSTALL_SOURCE);
    expect(state).not.toContain(INSTALL_SETUP_CODE);
    expect((await stat(host.data)).uid).toBe(host.owner.uid);
    expect((await stat(host.data)).mode & 0o777).toBe(0o700);
    expect((await stat(join(host.state, "installation.json"))).mode & 0o777).toBe(0o600);
    expect(
      (await host.calls()).some((call) =>
        ["apt-get", "useradd", "systemctl"].includes(call.command),
      ),
    ).toBe(false);
  });

  it("repeated and newer installers retain the already pinned deployment and existing vault", async () => {
    const host = await fixture();
    const original = await host.installer("0.10.1");
    expect((await host.run(original.file)).exitCode).toBe(0);
    const vault = '{"fixture":"existing encrypted vault bytes"}\n';
    await writeFile(join(host.data, "vault.json"), vault, { mode: 0o600 });
    const savedState = await readFile(join(host.state, "installation.json"), "utf8");
    const same = await host.run(original.file);
    expect(same.exitCode, same.stderr).toBe(0);
    expect(same.stdout).toMatch(/unlock/i);
    const newer = await host.installer("0.10.2");
    const result = await host.run(newer.file);
    expect(result.exitCode, result.stderr).toBe(0);
    expect(`${result.stdout}${result.stderr}`).toContain("0.10.1");
    expect(await readFile(join(host.state, "installation.json"), "utf8")).toBe(savedState);
    expect(await readFile(join(host.data, "vault.json"), "utf8")).toBe(vault);
    expect(deployments(await host.calls())).toHaveLength(1);
    expect(
      (await host.calls()).filter(
        (call) =>
          call.command === "curl" &&
          call.args.some((argument) => argument.includes("/download/v0.10.2/")),
      ),
    ).toHaveLength(0);
  });

  it("resumes an interrupted deployment without replacing its private data", async () => {
    const host = await fixture({ failedDeploy: true });
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).not.toBe(0);
    await writeFile(join(host.data, "vault.json"), "preserved encrypted vault\n", { mode: 0o600 });
    await host.control({ failedDeploy: false });
    const retry = await host.run(installer.file);
    expect(retry.exitCode, retry.stderr).toBe(0);
    expect(await readFile(join(host.data, "vault.json"), "utf8")).toBe(
      "preserved encrypted vault\n",
    );
    expect(deployments(await host.calls())).toHaveLength(2);
  });

  it.each(["streamskope-0.10.1.clab.yml", "streamskope-0.10.1-container.json"])(
    "rejects corrupted downloaded %s before deployment",
    async (asset) => {
      const host = await fixture({ corruptDownload: asset });
      const installer = await host.installer();
      expect((await host.run(installer.file)).exitCode).not.toBe(0);
      expect(deployments(await host.calls())).toHaveLength(0);
    },
  );

  it("fails on an unavailable release download rather than deploying another version", async () => {
    const host = await fixture({ failedDownload: "streamskope-0.10.1.clab.yml" });
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).not.toBe(0);
    expect(deployments(await host.calls())).toHaveLength(0);
  });

  it.each(["version", "sourceRevision"])(
    "rejects a checksummed manifest with the wrong %s",
    async (field) => {
      const host = await fixture();
      const installer = await host.installer("0.10.1", (manifest) => {
        manifest[field] = field === "version" ? "0.10.2" : "f".repeat(40);
      });
      expect((await host.run(installer.file)).exitCode).not.toBe(0);
      expect(deployments(await host.calls())).toHaveLength(0);
    },
  );

  it("does not adopt or remove an unrelated container using the installation name", async () => {
    const host = await fixture({
      container: {
        Name: "/clab-streamskope-app",
        Config: { Image: "unrelated:latest", Labels: { containerlab: "another-lab" } },
        State: { Running: true },
      },
    });
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).not.toBe(0);
    expect(deployments(await host.calls())).toHaveLength(0);
    expect(
      (await host.calls()).some((call) =>
        call.args.some((argument) => ["destroy", "rm", "stop"].includes(argument)),
      ),
    ).toBe(false);
  });

  it("rejects a symlinked data directory and leaves its destination untouched", async () => {
    const host = await fixture();
    await mkdir(host.state, { mode: 0o700 });
    const outside = join(host.root, "unrelated-data");
    await mkdir(outside, { mode: 0o700 });
    await writeFile(join(outside, "marker"), "untouched\n");
    await symlink(outside, host.data);
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).not.toBe(0);
    expect(await readFile(join(outside, "marker"), "utf8")).toBe("untouched\n");
    expect(deployments(await host.calls())).toHaveLength(0);
  });

  it("rejects permissive existing private data instead of silently repairing it", async () => {
    const host = await fixture();
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).toBe(0);
    await chmod(host.data, 0o755);
    const result = await host.run(installer.file);
    expect(result.exitCode).not.toBe(0);
    expect((await stat(host.data)).mode & 0o777).toBe(0o755);
    expect(deployments(await host.calls())).toHaveLength(1);
  });

  it("starts an owned stopped instance without redeploying or replacing its state", async () => {
    const host = await fixture();
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).toBe(0);
    const control = JSON.parse(await readFile(join(host.root, "control.json"), "utf8")) as {
      container: Record<string, unknown> & { State: Record<string, unknown> };
    };
    await host.control({
      container: { ...control.container, State: { ...control.container.State, Running: false } },
    });
    const saved = await readFile(join(host.state, "installation.json"), "utf8");
    const result = await host.run(installer.file);
    expect(result.exitCode, result.stderr).toBe(0);
    expect(deployments(await host.calls())).toHaveLength(1);
    expect(
      (await host.calls()).filter(
        (call) => call.command === "docker" && call.args.includes("start"),
      ),
    ).toHaveLength(1);
    expect(await readFile(join(host.state, "installation.json"), "utf8")).toBe(saved);
  });

  it("rejects a saved owner mismatch without changing the caller's data", async () => {
    const host = await fixture();
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).toBe(0);
    const path = join(host.state, "installation.json");
    const state = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
    await writeFile(path, JSON.stringify({ ...state, uid: host.owner.uid + 1 }));
    const result = await host.run(installer.file);
    expect(result.exitCode).not.toBe(0);
    expect((await stat(host.data)).uid).toBe(host.owner.uid);
    expect(deployments(await host.calls())).toHaveLength(1);
  });

  it("rejects a changed saved topology before restarting or adopting the existing instance", async () => {
    const host = await fixture();
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).toBe(0);
    const path = join(host.state, `streamskope-${installer.version}.clab.yml`);
    await writeFile(path, `${installer.topology}\n# unreviewed deployment change\n`);
    const result = await host.run(installer.file);
    expect(result.exitCode).not.toBe(0);
    expect(deployments(await host.calls())).toHaveLength(1);
    expect(
      (await host.calls()).some((call) => call.command === "docker" && call.args.includes("start")),
    ).toBe(false);
  });

  it.each(["all", "install", "--version"])(
    "rejects positional mode %s before changing the host",
    async (argument) => {
      const host = await fixture();
      const installer = await host.installer();
      const result = await host.run(installer.file, [argument]);
      expect(result.exitCode).not.toBe(0);
      expect(await host.calls()).toHaveLength(0);
    },
  );

  it("chooses a free new loopback port and keeps its configured browser origin consistent", async () => {
    const occupied = await occupiedPort();
    const host = await fixture({ firstPort: occupied });
    const installer = await host.installer();
    const result = await host.run(installer.file);
    expect(result.exitCode, result.stderr).toBe(0);
    const port = Number(/http:\/\/127\.0\.0\.1:(\d+)/u.exec(result.stdout)?.[1]);
    expect(port).toBeGreaterThan(occupied);
    const control = JSON.parse(await readFile(join(host.root, "control.json"), "utf8")) as {
      container: {
        Config: { Env: string[] };
        HostConfig: { PortBindings: Record<string, Array<{ HostPort: string }>> };
      };
    };
    expect(control.container.Config.Env).toContain(
      `STREAMSKOPE_PUBLIC_ORIGIN=http://127.0.0.1:${port}`,
    );
    expect(control.container.HostConfig.PortBindings["8080/tcp"]?.[0]?.HostPort).toBe(String(port));
  });

  it("refuses to silently move a saved installation when its stopped container's port is occupied", async () => {
    const listener = createServer();
    await new Promise<void>((accept) => listener.listen(0, "127.0.0.1", () => accept()));
    const port = (listener.address() as { port: number }).port;
    await new Promise<void>((accept) => listener.close(() => accept()));
    const host = await fixture({ firstPort: port });
    const installer = await host.installer();
    expect((await host.run(installer.file)).exitCode).toBe(0);
    const saved = await readFile(join(host.state, "installation.json"), "utf8");
    await host.control({ container: undefined });
    await occupiedPort(port);
    const retry = await host.run(installer.file);
    expect(retry.exitCode).not.toBe(0);
    expect(await readFile(join(host.state, "installation.json"), "utf8")).toBe(saved);
    expect(deployments(await host.calls())).toHaveLength(1);
  });
});

it.each([
  { version: "0.0.0-dev" },
  { sourceRevision: "mutable-main" },
  { topologySha256: "not-a-digest" },
  { manifestSha256: "sha256:" + "a".repeat(64) },
])("rejects an unassigned or unverifiable installer identity %j", (change) => {
  expect(() =>
    renderBrowserWorkbenchInstaller(
      {
        version: "0.10.1",
        sourceRevision: INSTALL_SOURCE,
        topologySha256: "c".repeat(64),
        manifestSha256: "d".repeat(64),
        ...change,
      },
      "@STREAMSKOPE_INSTALL_VERSION@\n@STREAMSKOPE_INSTALL_SOURCE@\n@STREAMSKOPE_TOPOLOGY_SHA256@\n@STREAMSKOPE_MANIFEST_SHA256@\n",
    ),
  ).toThrow();
});
