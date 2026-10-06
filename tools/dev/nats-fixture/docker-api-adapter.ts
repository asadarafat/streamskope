import { chmod, mkdtemp, rm } from "node:fs/promises";
import { createServer, request, type IncomingHttpHeaders } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { NatsContainerlabFixtureIntent } from "./ownership";

export interface DockerApiReply {
  readonly status: number;
  readonly headers: IncomingHttpHeaders;
  readonly bytes: Buffer;
}

/** A bounded Unix-daemon request; errors never include request bodies or daemon diagnostics. */
export function dockerApiRequest(
  socketPath: string,
  method: string,
  path: string,
  bytes?: Buffer,
): Promise<DockerApiReply> {
  return new Promise((accept, reject) => {
    const outgoing = request(
      {
        socketPath,
        method,
        path,
        headers:
          bytes === undefined
            ? {}
            : { "content-type": "application/json", "content-length": bytes.length },
      },
      (response) => {
        const chunks: Buffer[] = [];
        let length = 0;
        response.on("data", (chunk: Buffer) => {
          length += chunk.length;
          if (length > 8 * 1024 * 1024)
            outgoing.destroy(new Error("Docker response exceeded its bound."));
          else chunks.push(chunk);
        });
        response.once("error", () => reject(new Error("Owned Docker response failed.")));
        response.once("end", () =>
          accept({
            status: response.statusCode ?? 500,
            headers: response.headers,
            bytes: Buffer.concat(chunks),
          }),
        );
      },
    );
    const deadline = setTimeout(
      () => outgoing.destroy(new Error("Owned Docker request exceeded its deadline.")),
      30_000,
    );
    outgoing.once("close", () => clearTimeout(deadline));
    outgoing.once("error", () => reject(new Error("Owned Docker request failed.")));
    outgoing.end(bytes);
  });
}

export function dockerObject(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Owned Docker metadata was invalid.");
  return value as Record<string, unknown>;
}

export async function dockerMetadata(
  socket: string,
  path: string,
): Promise<Record<string, unknown> | undefined> {
  const reply = await dockerApiRequest(socket, "GET", path);
  if (reply.status === 404) return undefined;
  if (reply.status !== 200) throw new Error("Owned Docker metadata could not be verified.");
  return dockerObject(JSON.parse(reply.bytes.toString()) as unknown);
}

export async function dockerContainers(
  socket: string,
  filters: Readonly<Record<string, readonly string[]>>,
): Promise<readonly Record<string, unknown>[]> {
  const reply = await dockerApiRequest(
    socket,
    "GET",
    `/containers/json?all=1&filters=${encodeURIComponent(JSON.stringify(filters))}`,
  );
  if (reply.status !== 200) throw new Error("Owned Docker inventory could not be verified.");
  const entries: unknown = JSON.parse(reply.bytes.toString());
  if (!Array.isArray(entries)) throw new Error("Owned Docker inventory was invalid.");
  return entries.map(dockerObject);
}

export function fixtureLabels(value: unknown, identity: string): boolean {
  const labels = dockerObject(value);
  return (
    labels["io.streamskope.fixture"] === "nats" && labels["io.streamskope.fixture.id"] === identity
  );
}

interface AdapterOptions {
  readonly socketPath: string;
  readonly intent: NatsContainerlabFixtureIntent;
  readonly saveProgress: (intent: NatsContainerlabFixtureIntent) => Promise<void>;
  readonly copyMaterial: (container: string) => Promise<void>;
}
export interface NatsDockerAdapter {
  readonly host: string;
  readonly progress: () => NatsContainerlabFixtureIntent;
  close(): Promise<void>;
}

/** Only this lab's three mutable Docker resources are admitted; this is not a general Docker proxy. */
export async function startNatsDockerAdapter(options: AdapterOptions): Promise<NatsDockerAdapter> {
  const directory = await mkdtemp(join(tmpdir(), "skope-nats-api-"));
  await chmod(directory, 0o700);
  const socket = join(directory, "docker.sock");
  let intent = options.intent;
  let copied = options.intent.container !== undefined;
  let uncertain = false;
  let accepting = true;
  let work: Promise<unknown> = Promise.resolve();
  const save = async (patch: Partial<NatsContainerlabFixtureIntent>): Promise<void> => {
    intent = { ...intent, ...patch };
    await options.saveProgress(intent);
  };
  const ownedContainer = async (id: string): Promise<Record<string, unknown>> => {
    const value = await dockerMetadata(
      options.socketPath,
      `/containers/${encodeURIComponent(id)}/json`,
    );
    if (
      value === undefined ||
      value.Id !== intent.container ||
      value.Name !== `/${intent.name}` ||
      !fixtureLabels(dockerObject(value.Config).Labels, intent.identity)
    )
      throw new Error("Container ownership could not be verified.");
    return value;
  };
  const ownedNetwork = async (id: string): Promise<Record<string, unknown>> => {
    const value = await dockerMetadata(options.socketPath, `/networks/${encodeURIComponent(id)}`);
    if (
      value === undefined ||
      value.Id !== intent.network ||
      value.Name !== intent.networkName ||
      !fixtureLabels(value.Labels, intent.identity)
    )
      throw new Error("Network ownership could not be verified.");
    return value;
  };
  const mutate = async (
    method: string,
    originalPath: string,
    bytes: Buffer,
  ): Promise<DockerApiReply> => {
    const url = new URL(originalPath, "http://docker");
    const path = url.pathname.replace(/^\/v[0-9]+\.[0-9]+/u, "");
    let forwardPath = originalPath;
    let body: Record<string, unknown> =
      bytes.length === 0 ? {} : dockerObject(JSON.parse(bytes.toString()) as unknown);
    if (method === "POST" && path === "/networks/create") {
      if (
        body.Name !== intent.networkName ||
        (body.Driver !== undefined && body.Driver !== "bridge") ||
        intent.network !== undefined
      )
        throw new Error("Unexpected network creation was refused.");
      body = {
        ...body,
        Labels: {
          ...dockerObject(body.Labels ?? {}),
          "io.streamskope.fixture": "nats",
          "io.streamskope.fixture.id": intent.identity,
        },
      };
    } else if (method === "POST" && path === "/containers/create") {
      if (
        url.searchParams.get("name") !== intent.name ||
        body.Image !== intent.image ||
        body.User !== "0:0" ||
        JSON.stringify(body.Entrypoint) !== JSON.stringify(["nats-server"]) ||
        JSON.stringify(body.Cmd) !== JSON.stringify(["--config", "/fixture/nats.conf"]) ||
        !fixtureLabels(body.Labels, intent.identity) ||
        intent.container !== undefined
      )
        throw new Error("Unexpected container creation was refused.");
      const host = dockerObject(body.HostConfig);
      const ports = dockerObject(host.PortBindings);
      const bindings = ports["4222/tcp"];
      const networks = dockerObject(
        dockerObject(body.NetworkingConfig ?? {}).EndpointsConfig ?? {},
      );
      if (
        host.Privileged === true ||
        (Array.isArray(host.Binds) && host.Binds.length !== 0) ||
        (Array.isArray(host.Mounts) && host.Mounts.length !== 0) ||
        Object.keys(ports).length !== 1 ||
        !Array.isArray(bindings) ||
        bindings.length !== 1 ||
        dockerObject(bindings[0]).HostIp !== "127.0.0.1" ||
        dockerObject(bindings[0]).HostPort !== String(intent.port) ||
        Object.keys(networks).some(
          (name) => name !== intent.networkName && name !== intent.network,
        ) ||
        [host.PidMode, host.IpcMode, host.UTSMode, host.UsernsMode].some(
          (mode) => typeof mode === "string" && mode !== "" && mode !== "private",
        ) ||
        [host.Devices, host.DeviceRequests, host.VolumesFrom].some(
          (entries) => Array.isArray(entries) && entries.length !== 0,
        ) ||
        (host.NetworkMode !== intent.networkName && host.NetworkMode !== intent.network)
      )
        throw new Error("Unexpected container access was refused.");
      body = {
        ...body,
        HostConfig: {
          ...host,
          ReadonlyRootfs: true,
          CapDrop: ["ALL"],
          CapAdd: ["DAC_OVERRIDE"],
          PidsLimit: 64,
          Memory: 134217728,
          MemorySwap: 134217728,
          NanoCpus: 500000000,
          CpuPeriod: 0,
          CpuQuota: 0,
          Privileged: false,
          SecurityOpt: ["no-new-privileges:true"],
          Mounts: [
            {
              Type: "volume",
              Target: "/fixture",
              VolumeOptions: {
                Labels: {
                  "io.streamskope.fixture": "nats",
                  "io.streamskope.fixture.id": intent.identity,
                },
              },
            },
          ],
        },
      };
    } else {
      const container = /^\/containers\/([^/]+)(?:\/(start|stop|kill))?$/u.exec(path);
      const network = /^\/networks\/([^/]+)(?:\/(connect|disconnect))?$/u.exec(path);
      if (
        container !== null &&
        ((method === "DELETE" && container[2] === undefined) ||
          (method === "POST" && container[2] !== undefined))
      ) {
        await ownedContainer(decodeURIComponent(container[1]!));
        forwardPath = `/containers/${intent.container}${container[2] === undefined ? "" : `/${container[2]}`}${url.search}`;
        if (container[2] === "start" && !copied)
          throw new Error("Container start before private material transfer was refused.");
      } else if (
        network !== null &&
        ((method === "DELETE" && network[2] === undefined) ||
          (method === "POST" && network[2] !== undefined))
      ) {
        const value = await ownedNetwork(decodeURIComponent(network[1]!));
        forwardPath = `/networks/${intent.network}${network[2] === undefined ? "" : `/${network[2]}`}${url.search}`;
        if (method === "DELETE" && Object.keys(dockerObject(value.Containers ?? {})).length > 0)
          throw new Error("Network removal with attached endpoints was refused.");
        if (
          method === "POST" &&
          body.Container !== intent.container &&
          body.Container !== intent.name
        )
          throw new Error("Foreign network endpoint mutation was refused.");
        if (method === "POST") body = { ...body, Container: intent.container };
      } else throw new Error("Unexpected Docker mutation was refused.");
    }
    await save({ creationStarted: true, mutationsSettled: false });
    let reply: DockerApiReply;
    try {
      reply = await dockerApiRequest(
        options.socketPath,
        method,
        forwardPath,
        Buffer.from(JSON.stringify(body)),
      );
    } catch (error) {
      uncertain = true;
      throw error;
    }
    try {
      if (
        reply.status >= 200 &&
        reply.status < 300 &&
        method === "POST" &&
        path === "/networks/create"
      ) {
        const id = dockerObject(JSON.parse(reply.bytes.toString()) as unknown).Id;
        if (typeof id !== "string" || !/^[a-f0-9]{64}$/u.test(id))
          throw new Error("Network creation identity was invalid.");
        await save({ network: id });
      }
      if (
        reply.status >= 200 &&
        reply.status < 300 &&
        method === "POST" &&
        path === "/containers/create"
      ) {
        const id = dockerObject(JSON.parse(reply.bytes.toString()) as unknown).Id;
        if (typeof id !== "string" || !/^[a-f0-9]{64}$/u.test(id))
          throw new Error("Container creation identity was invalid.");
        await save({ container: id });
        const metadata = await ownedContainer(id);
        const mounts = metadata.Mounts;
        if (!Array.isArray(mounts) || mounts.length !== 1)
          throw new Error("Private fixture volume could not be verified.");
        const mount = dockerObject(mounts[0]);
        const volume = mount.Name;
        if (
          mount.Destination !== "/fixture" ||
          mount.Type !== "volume" ||
          typeof volume !== "string" ||
          !/^[a-f0-9]{64}$/u.test(volume)
        )
          throw new Error("Private fixture volume was invalid.");
        const volumeMetadata = await dockerMetadata(options.socketPath, `/volumes/${volume}`);
        if (volumeMetadata === undefined || !fixtureLabels(volumeMetadata.Labels, intent.identity))
          throw new Error("Private fixture volume ownership was invalid.");
        await save({ volume });
        try {
          await options.copyMaterial(id);
        } catch (error) {
          uncertain = true;
          throw error;
        }
        copied = true;
      }
    } catch (error) {
      // A successful mutation with an incomplete receipt cannot prove daemon quiescence.
      // Retain the unsettled journal rather than infer cleanup from an empty inventory.
      uncertain = true;
      throw error;
    }
    return reply;
  };
  const server = createServer((incoming, response) => {
    const dispatch = async (): Promise<void> => {
      if (!accepting) throw new Error("Fixture adapter is closing.");
      const chunks: Buffer[] = [];
      let size = 0;
      for await (const chunk of incoming) {
        const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string);
        size += bytes.length;
        if (size > 1024 * 1024) throw new Error("Docker request exceeded its bound.");
        chunks.push(bytes);
      }
      if (!accepting) throw new Error("Fixture adapter is closing.");
      const method = incoming.method ?? "GET";
      let reply: DockerApiReply;
      if (method === "GET" || method === "HEAD")
        reply = await dockerApiRequest(options.socketPath, method, incoming.url ?? "/");
      else {
        const operation = work.then(() =>
          mutate(method, incoming.url ?? "/", Buffer.concat(chunks)),
        );
        work = operation.catch(() => undefined);
        reply = await operation;
      }
      response.writeHead(reply.status, {
        "content-type": reply.headers["content-type"] ?? "application/json",
      });
      response.end(reply.bytes);
    };
    void dispatch().catch((error: unknown) => {
      if (!response.destroyed) {
        response.writeHead(403, { "content-type": "application/json" });
        const detail =
          error instanceof Error &&
          /^(Unexpected|Container |Network |Private fixture|Owned Docker|Docker request)/u.test(
            error.message,
          )
            ? error.message
            : "Owned fixture Docker request was refused or could not complete.";
        response.end(JSON.stringify({ message: detail }));
      }
    });
  });
  server.requestTimeout = 30_000;
  server.headersTimeout = 15_000;
  try {
    await new Promise<void>((accept, reject) => {
      server.once("error", reject);
      server.listen(socket, accept);
    });
    await chmod(socket, 0o600);
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
  let closing: Promise<void> | undefined;
  return {
    host: `unix://${socket}`,
    progress: () => intent,
    close: (): Promise<void> => {
      closing ??= Promise.resolve().then(async () => {
        accepting = false;
        await work;
        try {
          await save({ mutationsSettled: !uncertain });
        } finally {
          server.closeAllConnections();
          await new Promise<void>((accept, reject) =>
            server.close((error) => (error === undefined ? accept() : reject(error))),
          );
          await rm(directory, { recursive: true, force: true });
        }
      });
      return closing;
    },
  };
}
