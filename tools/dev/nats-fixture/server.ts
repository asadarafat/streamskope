import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { connect, type NatsConnection } from "@nats-io/transport-node";

import type { NatsConnectionInput } from "../../../src/features/nats/application/profile-types";
import { ensureContainerImage, ContainerImageAvailabilityError } from "../container-image";

import { NATS_SERVER_IMAGES } from "./definition";
import { boundedNatsOperation } from "./client";
import { prepareNatsMaterial, writeNatsMaterialConfig } from "./materials";

const execute = promisify(execFile);

export class NatsServerCreationUncertainError extends Error {
  constructor() {
    super(
      "NATS container creation completion could not be confirmed; private ownership evidence was retained.",
    );
    this.name = "NatsServerCreationUncertainError";
  }
}

export class NatsServerCleanedFailure extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NatsServerCleanedFailure";
  }
}

export interface NatsServer {
  readonly ownership: {
    readonly name: string;
    readonly identity: string;
    readonly container: string;
    readonly directory: string;
    readonly image: string;
    readonly port: number;
  };
  readonly server: string;
  readonly ipServer: string;
  readonly caPem: string;
  readonly untrustedCaPem: string;
  readonly token: string;
  readonly connection: NatsConnectionInput;
  /** A separate public SDK client; callers can publish without going through the engine. */
  publisher(): Promise<NatsConnection>;
  dispose(): Promise<void>;
}

export interface NatsServerOptions {
  readonly certificate?: "dns-and-ip" | "dns-only";
  readonly authentication?: "token" | "anonymous-restricted";
  readonly network?: "bridge" | "host-loopback";
  readonly name?: string;
  readonly directory?: string;
  readonly port?: number;
  readonly certificateDays?: number;
  readonly identity?: string;
  readonly signal?: AbortSignal;
  readonly onCreating?: () => Promise<void>;
  readonly onCreated?: (container: string) => Promise<void>;
}

/** Shared secure server definition for disposable qualification and persistent AIO labs. */
export async function startNatsServer(options: NatsServerOptions = {}): Promise<NatsServer> {
  if (process.platform !== "linux" || (process.arch !== "arm64" && process.arch !== "x64"))
    throw new Error("The isolated NATS fixture requires Linux arm64 or amd64 Docker.");
  const image = NATS_SERVER_IMAGES[process.arch];
  const platform = process.arch === "arm64" ? "linux/arm64" : "linux/amd64";
  const identity = options.identity ?? randomUUID();
  if (!/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u.test(identity))
    throw new Error("NATS fixture identity is invalid.");
  const name = options.name ?? `streamskope-nats-qualification-${identity}`;
  if (!/^[a-z][a-z0-9-]{1,100}$/u.test(name)) throw new Error("NATS fixture name is invalid.");
  if (
    options.port !== undefined &&
    (!Number.isSafeInteger(options.port) || options.port < 1 || options.port > 65_535)
  )
    throw new Error("NATS fixture port is invalid.");
  const certificateDays = options.certificateDays ?? 1;
  if (!Number.isSafeInteger(certificateDays) || certificateDays < 1 || certificateDays > 365)
    throw new Error("NATS fixture certificate lifetime is invalid.");
  const directory = options.directory ?? (await mkdtemp(join(tmpdir(), "streamskope-nats-")));
  const anonymous = options.authentication === "anonymous-restricted";
  const hostLoopback = options.network === "host-loopback";
  const clients = new Set<NatsConnection>();
  const openingClients = new Set<Promise<NatsConnection>>();
  let closing = false;
  let lateClientCleanupFailed = false;
  let container: string | undefined;
  let creationStarted = false;
  let disposeWork: Promise<void> | undefined;
  let portLease: Server | undefined;
  let phase = "private certificate preparation";
  const run = async (
    command: string,
    arguments_: readonly string[],
    settings: { readonly timeout: number },
  ): Promise<{ readonly stdout: string; readonly stderr: string }> =>
    execute(command, [...arguments_], {
      ...settings,
      encoding: "utf8",
      ...(options.signal === undefined ||
      (command === "docker" && ["create", "cp", "start"].includes(arguments_[0] ?? ""))
        ? {}
        : { signal: options.signal }),
    });

  const releasePortLease = async (): Promise<void> => {
    const lease = portLease;
    if (lease === undefined) return;
    if (lease.listening) {
      await new Promise<void>((accept, reject) => {
        lease.close((error) => (error === undefined ? accept() : reject(error)));
      });
    }
    portLease = undefined;
  };

  const recoverContainer = async (): Promise<void> => {
    if (container !== undefined || !creationStarted) return;
    // A failed CLI call may have completed its daemon-side create; accept only our UUID labels.
    for (let attempt = 0; attempt < 10; attempt += 1) {
      try {
        const { stdout } = await execute(
          "docker",
          [
            "inspect",
            "--format",
            '{{.Id}} {{index .Config.Labels "io.streamskope.fixture.id"}} {{index .Config.Labels "io.streamskope.fixture"}}',
            name,
          ],
          { timeout: 5_000 },
        );
        const match = /^([a-f0-9]{64}) ([a-f0-9-]{36}) nats\s*$/u.exec(stdout);
        if (match?.[2] === identity) {
          container = match[1];
          return;
        }
        throw new Error("The fixture name did not identify our owned container.");
      } catch {
        if (attempt < 9) await delay(100);
      }
    }
  };

  const dispose = (): Promise<void> => {
    if (disposeWork !== undefined) return disposeWork;
    closing = true;
    disposeWork = Promise.resolve().then(async (): Promise<void> => {
      await recoverContainer();
      if (creationStarted && container === undefined) {
        await releasePortLease();
        throw new NatsServerCreationUncertainError();
      }
      const cleanup = await Promise.allSettled([
        Promise.resolve().then(async (): Promise<void> => {
          const results = await Promise.allSettled([
            ...[...clients].map(async (client): Promise<void> => {
              const cleanup = await Promise.allSettled([
                Promise.resolve().then(() => client.close()),
                Promise.resolve().then(() => client.closed()),
              ]);
              if (cleanup.some((result) => result.status === "rejected"))
                throw new Error("An owned NATS fixture client did not close.");
            }),
            // Includes late-return closure; server removal independently ends pending dials.
            ...[...openingClients].map((work) =>
              work.then(
                () => undefined,
                () => undefined,
              ),
            ),
          ]);
          if (lateClientCleanupFailed || results.some((result) => result.status === "rejected"))
            throw new Error("An owned NATS fixture client did not close.");
        }),
        Promise.resolve().then(async (): Promise<void> => {
          if (container !== undefined) {
            await execute("docker", ["rm", "--force", "--volumes", container], { timeout: 30_000 });
          }
        }),
        Promise.resolve().then(releasePortLease),
      ]);
      // Removing the private directory still runs after client, container, or port lease failure.
      const files = await Promise.allSettled([rm(directory, { recursive: true, force: true })]);
      if ([...cleanup, ...files].some((result) => result.status === "rejected"))
        throw new Error("Owned NATS fixture cleanup could not be confirmed.");
    });
    return disposeWork;
  };

  try {
    options.signal?.throwIfAborted();
    const material = await prepareNatsMaterial({
      directory,
      certificateDays,
      certificate: options.certificate,
      signal: options.signal,
    });
    const token = material.token;
    let configuredPort = 4222;
    if (hostLoopback) {
      phase = "owned loopback port reservation";
      // Reject stray connections without retaining sockets that could delay lease closure.
      const lease = createServer((socket) => socket.destroy());
      portLease = lease;
      await new Promise<void>((accept, reject) => {
        lease.once("error", reject);
        lease.listen(options.port ?? 0, "127.0.0.1", () => {
          lease.off("error", reject);
          accept();
        });
      });
      const address = lease.address();
      if (address === null || typeof address === "string")
        throw new Error("The owned NATS loopback port reservation was invalid.");
      configuredPort = address.port;
    }
    await writeNatsMaterialConfig(material, {
      name,
      host: hostLoopback ? "127.0.0.1" : "0.0.0.0",
      port: configuredPort,
      authentication: options.authentication,
    });
    phase = "pinned image availability";
    await ensureContainerImage(image, platform, (arguments_, timeout) =>
      run("docker", arguments_, { timeout }),
    );
    phase = "owned container creation";
    options.signal?.throwIfAborted();
    await options.onCreating?.();
    creationStarted = true;
    const { stdout } = await run(
      "docker",
      [
        "create",
        "--name",
        name,
        "--label",
        "io.streamskope.fixture=nats",
        "--label",
        `io.streamskope.fixture.id=${identity}`,
        "--platform",
        platform,
        "--user",
        "0:0",
        // Run as the explicit private-file owner, independently of image entrypoint privilege changes.
        "--entrypoint",
        "nats-server",
        "--read-only",
        "--cap-drop",
        "ALL",
        // Docker may retain the client's file owner; root reads only this owned private volume.
        "--cap-add",
        "DAC_OVERRIDE",
        "--security-opt",
        "no-new-privileges",
        "--pids-limit",
        "64",
        "--memory",
        "128m",
        "--cpus",
        "0.5",
        ...(hostLoopback
          ? ["--network", "host"]
          : ["--publish", `127.0.0.1:${options.port ?? ""}:4222`]),
        // Copy secrets through Docker instead of assuming the daemon shares the client's /tmp.
        "--volume",
        "/fixture",
        image,
        "--config",
        "/fixture/nats.conf",
      ],
      { timeout: 120_000 },
    );
    const identifier = stdout.trim();
    if (!/^[a-f0-9]{64}$/u.test(identifier))
      throw new Error("Owned NATS container identity is invalid.");
    container = identifier;
    await options.onCreated?.(container);
    options.signal?.throwIfAborted();
    phase = "owned private file transfer";
    // The server needs only its leaf/key/config; CA signing keys never enter the container.
    for (const file of ["server.pem", "server-key.pem", "nats.conf"])
      await run("docker", ["cp", join(directory, file), `${container}:/fixture/${file}`], {
        timeout: 30_000,
      });
    phase = "owned container start";
    // Keep the host-loopback port reserved through image/container/file preparation.
    await releasePortLease();
    await run("docker", ["start", container], { timeout: 30_000 });
    let port = configuredPort;
    if (!hostLoopback) {
      phase = "loopback port discovery";
      const { stdout: mapping } = await run("docker", ["port", container, "4222/tcp"], {
        timeout: 10_000,
      });
      const match = /^127\.0\.0\.1:([0-9]+)\s*$/u.exec(mapping);
      if (match === null) {
        phase = "strict loopback port parsing";
        throw new Error("Owned NATS fixture has no loopback port.");
      }
      port = Number(match[1]);
    }
    if (!Number.isSafeInteger(port) || port <= 0 || port > 65_535)
      throw new Error("Owned NATS fixture port is invalid.");
    const server = `nats://localhost:${port}`;
    const ipServer = `nats://127.0.0.1:${port}`;
    const caPem = material.caPem;
    const untrustedCaPem = material.untrustedCaPem;
    const publisher = (): Promise<NatsConnection> => {
      if (closing) return Promise.reject(new Error("The owned NATS fixture is already closing."));
      const opening = connect({
        servers: [server],
        ...(anonymous ? {} : { token }),
        tls: { rejectUnauthorized: true, ca: caPem },
        timeout: 1_000,
        reconnect: false,
        waitOnFirstConnect: false,
        ignoreClusterUpdates: true,
        noRandomize: true,
        debug: false,
      });
      const work = opening.then(async (client): Promise<NatsConnection> => {
        if (closing || options.signal?.aborted) {
          const cleanup = await Promise.allSettled([
            Promise.resolve().then(() => client.close()),
            Promise.resolve().then(() => client.closed()),
          ]);
          lateClientCleanupFailed ||= cleanup.some((result) => result.status === "rejected");
          throw new Error("The owned NATS fixture closed during public client setup.");
        }
        clients.add(client);
        // SDK connect can return an already-closed connection after a failed DNS alternative.
        if (client.isClosed())
          throw new Error("The owned NATS client was already closed during setup.");
        try {
          await boundedNatsOperation(client.flush());
          if (client.isClosed())
            throw new Error("The owned NATS client closed before readiness confirmation.");
          return client;
        } catch (error) {
          await client.close();
          await client.closed();
          clients.delete(client);
          throw error;
        }
      });
      openingClients.add(work);
      work.then(
        () => openingClients.delete(work),
        () => openingClients.delete(work),
      );
      return work;
    };
    let ready = false;
    phase = "verified TLS readiness";
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      options.signal?.throwIfAborted();
      try {
        const client = await publisher();
        await client.close();
        await client.closed();
        clients.delete(client);
        ready = true;
        break;
      } catch {
        await delay(100);
      }
    }
    if (!ready) throw new Error("Owned NATS fixture readiness timed out.");
    return {
      ownership: { name, identity, container, directory, image, port },
      server,
      ipServer,
      caPem,
      untrustedCaPem,
      token,
      connection: {
        servers: [server],
        authentication: anonymous ? { mode: "none" } : { mode: "token", token },
        tls: { mode: "tls", caPem },
      },
      publisher,
      dispose,
    };
  } catch (error) {
    if (error instanceof ContainerImageAvailabilityError) phase += ` (${error.reason})`;
    if (container !== undefined) {
      try {
        const { stdout } = await execute(
          "docker",
          ["inspect", "--format", "{{.State.Status}}/{{.State.ExitCode}}", container],
          { timeout: 10_000 },
        );
        if (/^(created|running|paused|restarting|removing|exited|dead)\/[0-9]+\s*$/u.test(stdout))
          phase += ` (${stdout.trim()})`;
        const logs = await execute("docker", ["logs", container], { timeout: 10_000 });
        const log = logs.stdout + logs.stderr;
        if (log.includes("write_deadline")) phase += " (write_deadline configuration)";
        if (log.includes("permission denied")) phase += " (private file access)";
        if (log.includes("unknown field")) phase += " (unsupported configuration field)";
        if (log.includes("no such file or directory")) phase += " (private file unavailable)";
      } catch {
        /* Safe stage diagnosis cannot replace actual resource cleanup. */
      }
    }
    await dispose();
    // Never expose Docker/openssl arguments, server logs or generated credentials.
    throw new NatsServerCleanedFailure(
      `The isolated NATS fixture failed during ${phase}; existing labs were unchanged.`,
    );
  }
}
