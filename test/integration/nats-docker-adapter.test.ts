import { chmod, mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it } from "vitest";

import {
  dockerApiRequest,
  startNatsDockerAdapter,
} from "../../tools/dev/nats-fixture/docker-api-adapter";
import { NATS_SERVER_IMAGES } from "../../tools/dev/nats-fixture/definition";
import {
  natsContainerlabPaths,
  type NatsContainerlabFixtureIntent,
} from "../../tools/dev/nats-fixture/ownership";

const owned: { server: Server; directory: string }[] = [];
afterEach(async () => {
  for (const fixture of owned.splice(0)) {
    fixture.server.closeAllConnections();
    await new Promise<void>((resolve) => fixture.server.close(() => resolve()));
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

async function daemon(
  options: {
    networkReply?: unknown;
    networkBegan?: () => void;
    networkGate?: Promise<void>;
  } = {},
): Promise<{
  socket: string;
  posts: unknown[];
  intent: NatsContainerlabFixtureIntent;
}> {
  const directory = await mkdtemp(join(tmpdir(), "skope-adapter-test-"));
  await chmod(directory, 0o700);
  const socket = join(directory, "daemon.sock");
  const identity = "11111111-1111-4111-8111-111111111111";
  const intent: NatsContainerlabFixtureIntent = {
    format: 2,
    identity,
    directory,
    image: NATS_SERVER_IMAGES.arm64,
    port: 15228,
    ...natsContainerlabPaths(identity, directory),
    creationStarted: false,
    mutationsSettled: true,
    daemon: "fixture-daemon-test",
  };
  const posts: unknown[] = [];
  const labels = { "io.streamskope.fixture": "nats", "io.streamskope.fixture.id": identity };
  const server = createServer((request, response) => {
    const serve = async (): Promise<void> => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk as Uint8Array));
      if (request.method === "POST")
        posts.push(JSON.parse(Buffer.concat(chunks).toString() || "{}") as unknown);
      const path = request.url?.split("?")[0];
      if (path === "/networks/create") {
        options.networkBegan?.();
        await options.networkGate;
      }
      const value =
        path === "/networks/create"
          ? (options.networkReply ?? { Id: "a".repeat(64) })
          : path === "/containers/create"
            ? { Id: "b".repeat(64) }
            : path === `/containers/${"b".repeat(64)}/json`
              ? {
                  Id: "b".repeat(64),
                  Name: `/${intent.name}`,
                  Config: { Labels: labels },
                  Mounts: [{ Type: "volume", Destination: "/fixture", Name: "c".repeat(64) }],
                }
              : path === `/volumes/${"c".repeat(64)}`
                ? { Name: "c".repeat(64), Labels: labels }
                : undefined;
      response.writeHead(value === undefined ? 404 : request.method === "POST" ? 201 : 200, {
        "content-type": "application/json",
      });
      response.end(JSON.stringify(value ?? { message: "absent" }));
    };
    void serve().catch(() => {
      response.writeHead(500);
      response.end();
    });
  });
  await new Promise<void>((resolve) => server.listen(socket, resolve));
  owned.push({ server, directory });
  return { socket, posts, intent };
}

it("journals full resource IDs and transfers private material before acknowledging an immutable hardened create", async () => {
  const fixture = await daemon();
  const journals: NatsContainerlabFixtureIntent[] = [];
  let finishCopy!: () => void;
  let beganCopy!: () => void;
  const copying = new Promise<void>((resolve) => {
    beganCopy = resolve;
  });
  const copied = new Promise<void>((resolve) => {
    finishCopy = resolve;
  });
  const adapter = await startNatsDockerAdapter({
    socketPath: fixture.socket,
    intent: fixture.intent,
    saveProgress: (next) => {
      journals.push(next);
      return Promise.resolve();
    },
    copyMaterial: () => {
      beganCopy();
      return copied;
    },
  });
  const socket = adapter.host.slice("unix://".length);
  try {
    await dockerApiRequest(
      socket,
      "POST",
      "/networks/create",
      Buffer.from(JSON.stringify({ Name: fixture.intent.networkName, Driver: "bridge" })),
    );
    const create = dockerApiRequest(
      socket,
      "POST",
      `/containers/create?name=${fixture.intent.name}`,
      Buffer.from(
        JSON.stringify({
          Image: fixture.intent.image,
          User: "0:0",
          Entrypoint: ["nats-server"],
          Cmd: ["--config", "/fixture/nats.conf"],
          Labels: {
            "io.streamskope.fixture": "nats",
            "io.streamskope.fixture.id": fixture.intent.identity,
          },
          HostConfig: {
            NetworkMode: fixture.intent.networkName,
            PortBindings: {
              "4222/tcp": [{ HostIp: "127.0.0.1", HostPort: String(fixture.intent.port) }],
            },
            CpuPeriod: 100000,
            CpuQuota: 50000,
          },
        }),
      ),
    );
    let acknowledged = false;
    void create.then(() => {
      acknowledged = true;
    });
    await copying;
    expect(acknowledged).toBe(false);
    expect(journals.at(-1)).toMatchObject({
      container: "b".repeat(64),
      network: "a".repeat(64),
      volume: "c".repeat(64),
      mutationsSettled: false,
    });
    expect(fixture.posts.at(-1)).toMatchObject({
      HostConfig: {
        ReadonlyRootfs: true,
        CapDrop: ["ALL"],
        CapAdd: ["DAC_OVERRIDE"],
        PidsLimit: 64,
        Memory: 134217728,
        NanoCpus: 500000000,
        CpuPeriod: 0,
        CpuQuota: 0,
        SecurityOpt: ["no-new-privileges:true"],
        Mounts: [
          {
            Type: "volume",
            Target: "/fixture",
            VolumeOptions: { Labels: { "io.streamskope.fixture.id": fixture.intent.identity } },
          },
        ],
      },
    });
    finishCopy();
    expect((await create).status).toBe(201);
    expect(adapter.progress().mutationsSettled).toBe(false);
  } finally {
    finishCopy();
    await adapter.close();
  }
  expect(journals.at(-1)?.mutationsSettled).toBe(true);
});

it("refuses foreign creates and unexpected executable mutations before forwarding or claiming ownership", async () => {
  const fixture = await daemon();
  const adapter = await startNatsDockerAdapter({
    socketPath: fixture.socket,
    intent: fixture.intent,
    saveProgress: () => Promise.resolve(),
    copyMaterial: () => Promise.resolve(),
  });
  try {
    const socket = adapter.host.slice("unix://".length);
    expect(
      (
        await dockerApiRequest(
          socket,
          "POST",
          "/containers/create?name=unrelated",
          Buffer.from('{"Image":"unrelated"}'),
        )
      ).status,
    ).toBe(403);
    expect(
      (
        await dockerApiRequest(
          socket,
          "POST",
          "/containers/foreign/exec",
          Buffer.from('{"Cmd":["sh"]}'),
        )
      ).status,
    ).toBe(403);
    expect(fixture.posts).toEqual([]);
    expect(adapter.progress().creationStarted).toBe(false);
  } finally {
    await adapter.close();
  }
});

it("closes the private adapter even when final journal persistence fails", async () => {
  const fixture = await daemon();
  const adapter = await startNatsDockerAdapter({
    socketPath: fixture.socket,
    intent: fixture.intent,
    saveProgress: () => Promise.reject(new Error("journal unavailable")),
    copyMaterial: () => Promise.resolve(),
  });
  await expect(adapter.close()).rejects.toThrow("journal unavailable");
  await expect(
    dockerApiRequest(adapter.host.slice("unix://".length), "GET", "/version"),
  ).rejects.toThrow("Owned Docker request failed");
});

it("retains an unsettled journal after an ambiguous successful daemon creation reply", async () => {
  const fixture = await daemon({ networkReply: { Id: "invalid-full-id" } });
  const journals: NatsContainerlabFixtureIntent[] = [];
  const adapter = await startNatsDockerAdapter({
    socketPath: fixture.socket,
    intent: fixture.intent,
    saveProgress: (next) => {
      journals.push(next);
      return Promise.resolve();
    },
    copyMaterial: () => Promise.resolve(),
  });
  const reply = await dockerApiRequest(
    adapter.host.slice("unix://".length),
    "POST",
    "/networks/create",
    Buffer.from(JSON.stringify({ Name: fixture.intent.networkName, Driver: "bridge" })),
  );
  expect(reply.status).toBe(403);
  await adapter.close();
  expect(journals.at(-1)).toMatchObject({ creationStarted: true, mutationsSettled: false });
  expect(journals.at(-1)?.network).toBeUndefined();
});

it("seals new mutations and waits for an admitted daemon response before declaring settlement", async () => {
  let began!: () => void;
  let finish!: () => void;
  const started = new Promise<void>((resolve) => {
    began = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const fixture = await daemon({ networkBegan: began, networkGate: gate });
  const adapter = await startNatsDockerAdapter({
    socketPath: fixture.socket,
    intent: fixture.intent,
    saveProgress: () => Promise.resolve(),
    copyMaterial: () => Promise.resolve(),
  });
  const reply = dockerApiRequest(
    adapter.host.slice("unix://".length),
    "POST",
    "/networks/create",
    Buffer.from(JSON.stringify({ Name: fixture.intent.networkName, Driver: "bridge" })),
  );
  await started;
  const close = adapter.close();
  expect(adapter.progress().mutationsSettled).toBe(false);
  finish();
  expect((await reply).status).toBe(201);
  await close;
  expect(adapter.progress()).toMatchObject({ mutationsSettled: true, network: "a".repeat(64) });
});
