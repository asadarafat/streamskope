import { chmod, mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it, vi } from "vitest";

import { ContainerlabNatsRuntime } from "../../tools/dev/nats-fixture/containerlab-runtime";
import { NATS_SERVER_IMAGES } from "../../tools/dev/nats-fixture/definition";
import {
  natsContainerlabPaths,
  type NatsContainerlabFixtureRecord,
} from "../../tools/dev/nats-fixture/ownership";

const fixtures: { directory: string; server: Server }[] = [];
afterEach(async () => {
  vi.unstubAllEnvs();
  for (const fixture of fixtures.splice(0)) {
    fixture.server.closeAllConnections();
    await new Promise<void>((resolve) => fixture.server.close(() => resolve()));
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

async function daemon(
  options: {
    present?: boolean;
    residualNetwork?: boolean;
    residualVolume?: boolean;
    daemon?: string;
    extraLabContainer?: boolean;
    foreignVolumeUser?: boolean;
    changedNetworkLabels?: boolean;
    changedVolumeLabels?: boolean;
    host?: Readonly<Record<string, unknown>>;
  } = {},
): Promise<{
  record: NatsContainerlabFixtureRecord;
  runtime: ContainerlabNatsRuntime;
  mutations: string[];
}> {
  const directory = await mkdtemp(join(tmpdir(), "skope-runtime-test-"));
  await chmod(directory, 0o700);
  const socket = join(directory, "daemon.sock");
  const identity = "11111111-1111-4111-8111-111111111111";
  const record: NatsContainerlabFixtureRecord = {
    format: 2,
    identity,
    directory,
    image: NATS_SERVER_IMAGES.arm64,
    port: 15328,
    ...natsContainerlabPaths(identity, directory),
    daemon: "fixture-daemon-test",
    container: "b".repeat(64),
    network: "a".repeat(64),
    volume: "c".repeat(64),
  };
  const present = options.present ?? false;
  let networkPresent = options.residualNetwork ?? present;
  let volumePresent = options.residualVolume ?? present;
  let networkReads = 0;
  let volumeReads = 0;
  const labels = { "io.streamskope.fixture": "nats", "io.streamskope.fixture.id": identity };
  const mutations: string[] = [];
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://docker");
    if (request.method !== "GET") mutations.push(`${request.method} ${url.pathname}`);
    let value: unknown;
    if (url.pathname === "/info") value = { ID: options.daemon ?? record.daemon };
    else if (url.pathname === "/containers/json") {
      const filters = JSON.parse(url.searchParams.get("filters") ?? "{}") as { volume?: string[] };
      value =
        options.extraLabContainer || (filters.volume !== undefined && options.foreignVolumeUser)
          ? [{ Id: "f".repeat(64) }]
          : present
            ? [{ Id: record.container }]
            : [];
    } else if (url.pathname === `/containers/${record.container}/json` && present) {
      value = {
        Id: record.container,
        Name: `/${record.name}`,
        Config: {
          Image: record.image,
          User: "0:0",
          Entrypoint: ["nats-server"],
          Cmd: ["--config", "/fixture/nats.conf"],
          Labels: {
            ...labels,
            containerlab: record.lab,
            "clab-node-name": "server",
            "clab-topo-file": record.topologyPath,
          },
        },
        HostConfig: {
          ReadonlyRootfs: true,
          Privileged: false,
          CapDrop: ["ALL"],
          CapAdd: ["DAC_OVERRIDE"],
          PidsLimit: 64,
          Memory: 134217728,
          MemorySwap: 134217728,
          NanoCpus: 500000000,
          SecurityOpt: ["no-new-privileges:true"],
          Binds: null,
          PidMode: "",
          IpcMode: "shareable",
          NetworkMode: record.networkName,
          PortBindings: { "4222/tcp": [{ HostPort: String(record.port), HostIp: "127.0.0.1" }] },
          ...options.host,
        },
        Mounts: [{ Type: "volume", Name: record.volume, Destination: "/fixture" }],
        NetworkSettings: { Networks: { [record.networkName]: { NetworkID: record.network } } },
        State: { Status: "running" },
      };
    } else if (
      [`/networks/${record.network}`, `/networks/${record.networkName}`].includes(url.pathname) &&
      networkPresent
    ) {
      if (request.method === "DELETE") networkPresent = false;
      else {
        networkReads++;
        value = {
          Id: record.network,
          Name: record.networkName,
          Labels: options.changedNetworkLabels && networkReads > 1 ? {} : labels,
          Containers: present ? { [record.container]: {} } : {},
        };
      }
    } else if (url.pathname === `/volumes/${record.volume}` && volumePresent) {
      if (request.method === "DELETE") volumePresent = false;
      else {
        volumeReads++;
        value = {
          Name: record.volume,
          Labels: options.changedVolumeLabels && volumeReads > 1 ? {} : labels,
        };
      }
    }
    response.writeHead(request.method === "DELETE" ? 204 : value === undefined ? 404 : 200, {
      "content-type": "application/json",
    });
    response.end(value === undefined ? undefined : JSON.stringify(value));
  });
  await new Promise<void>((resolve) => server.listen(socket, resolve));
  fixtures.push({ directory, server });
  vi.stubEnv("DOCKER_CONTEXT", "fixture-runtime-test");
  const runtime = new ContainerlabNatsRuntime(directory, () =>
    Promise.resolve(JSON.stringify(`unix://${socket}`)),
  );
  return { record, runtime, mutations };
}

it("accepts a hardened owned server with the exact private volume and loopback binding", async () => {
  const fixture = await daemon({ present: true });
  expect(await fixture.runtime.status(fixture.record)).toBe("running");
  expect(fixture.mutations).toEqual([]);
});

it.each([
  { ReadonlyRootfs: false },
  { CapAdd: ["SYS_ADMIN"] },
  { PidsLimit: 0 },
  { Memory: 0 },
  { Binds: ["/:/foreign"] },
  { PidMode: "host" },
  { PortBindings: { "4222/tcp": [{ HostIp: "0.0.0.0", HostPort: "15328" }] } },
])("refuses changed immutable isolation before orchestration: %j", async (host) => {
  const fixture = await daemon({ present: true, host });
  await expect(fixture.runtime.stop(fixture.record)).rejects.toThrow("isolation");
  expect(fixture.mutations).toEqual([]);
});

it.each([{ extraLabContainer: true }, { residualVolume: true, foreignVolumeUser: true }])(
  "refuses unrecorded lab containers and foreign private-volume users: %j",
  async (options) => {
    const fixture = await daemon(options);
    await expect(fixture.runtime.stop(fixture.record)).rejects.toThrow("unrecorded container");
    expect(fixture.mutations).toEqual([]);
  },
);

it.each([
  { residualNetwork: true, changedNetworkLabels: true },
  { residualVolume: true, changedVolumeLabels: true },
])("revalidates residual resource ownership immediately before removal: %j", async (options) => {
  const fixture = await daemon(options);
  await expect(fixture.runtime.stop(fixture.record)).rejects.toThrow("ownership");
  expect(fixture.mutations).toEqual([]);
});

it("refuses absence-based cleanup against a different local Docker daemon", async () => {
  const fixture = await daemon({ daemon: "another-fixture-daemon" });
  await expect(fixture.runtime.stop(fixture.record)).rejects.toThrow("original NATS Docker daemon");
  expect(fixture.mutations).toEqual([]);
});

it("retains unknown creation even when all current resource queries are empty", async () => {
  const fixture = await daemon();
  await expect(
    fixture.runtime.recover({ ...fixture.record, creationStarted: true, mutationsSettled: false }),
  ).rejects.toThrow("has not settled");
  expect(fixture.mutations).toEqual([]);
});

it("removes settled owned residual resources and confirms independent absence idempotently", async () => {
  const fixture = await daemon({ residualNetwork: true, residualVolume: true });
  await fixture.runtime.stop(fixture.record);
  await fixture.runtime.stop(fixture.record);
  expect(fixture.mutations).toEqual([
    `DELETE /networks/${fixture.record.network}`,
    `DELETE /volumes/${fixture.record.volume}`,
  ]);
});
