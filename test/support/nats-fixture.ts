import { execFile } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { connect, type NatsConnection } from "@nats-io/transport-node";

import type { NatsConnectionInput } from "../../src/features/nats/application/profile-types";

const execute = promisify(execFile);

const images = {
  arm64:
    "nats:2.15.0-alpine@sha256:d5091b05d2033732bf4b282301e4d588f49e8fa9bb58ea0387a0ecea08277bd3",
  x64: "nats:2.15.0-alpine@sha256:eda962d67930eda338222072d9a9f3818855d922ad224c399b0b01d251e9b91b",
} as const;

export interface NatsFixture {
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

/** Owned, pinned token/TLS server; never edits or removes a developer's existing lab. */
export async function startNatsFixture(
  options: {
    readonly certificate?: "dns-and-ip" | "dns-only";
    readonly authentication?: "token" | "anonymous-restricted";
  } = {},
): Promise<NatsFixture> {
  if (process.platform !== "linux" || (process.arch !== "arm64" && process.arch !== "x64"))
    throw new Error("The isolated NATS fixture requires Linux arm64 or amd64 Docker.");
  const image = images[process.arch];
  const platform = process.arch === "arm64" ? "linux/arm64" : "linux/amd64";
  const identity = randomUUID();
  const name = `streamskope-nats-qualification-${identity}`;
  const directory = await mkdtemp(join(tmpdir(), "streamskope-nats-"));
  const token = randomBytes(32).toString("hex");
  const anonymous = options.authentication === "anonymous-restricted";
  const clients = new Set<NatsConnection>();
  const openingClients = new Set<Promise<NatsConnection>>();
  let closing = false;
  let lateClientCleanupFailed = false;
  let container: string | undefined;
  let creationStarted = false;
  let disposeWork: Promise<void> | undefined;
  let phase = "private certificate preparation";

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
      ]);
      // Removing the private directory still runs after either client/container failure.
      const files = await Promise.allSettled([rm(directory, { recursive: true, force: true })]);
      if ([...cleanup, ...files].some((result) => result.status === "rejected"))
        throw new Error("Owned NATS fixture cleanup could not be confirmed.");
    });
    return disposeWork;
  };

  try {
    await chmod(directory, 0o700);
    await execute(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        join(directory, "ca-key.pem"),
        "-out",
        join(directory, "ca.pem"),
        "-days",
        "1",
        "-subj",
        "/CN=StreamSkope isolated NATS CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
      ],
      { timeout: 30_000 },
    );
    await execute(
      "openssl",
      [
        "req",
        "-new",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        join(directory, "server-key.pem"),
        "-out",
        join(directory, "server.csr"),
        "-subj",
        "/CN=localhost",
      ],
      { timeout: 30_000 },
    );
    await execute(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        join(directory, "untrusted-ca-key.pem"),
        "-out",
        join(directory, "untrusted-ca.pem"),
        "-days",
        "1",
        "-subj",
        "/CN=StreamSkope unrelated NATS CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
      ],
      { timeout: 30_000 },
    );
    await writeFile(
      join(directory, "server.ext"),
      [
        "basicConstraints=critical,CA:FALSE",
        "keyUsage=critical,digitalSignature,keyEncipherment",
        "extendedKeyUsage=serverAuth",
        options.certificate === "dns-only"
          ? "subjectAltName=DNS:localhost"
          : "subjectAltName=DNS:localhost,IP:127.0.0.1",
        "",
      ].join("\n"),
      { mode: 0o600 },
    );
    await execute(
      "openssl",
      [
        "x509",
        "-req",
        "-in",
        join(directory, "server.csr"),
        "-CA",
        join(directory, "ca.pem"),
        "-CAkey",
        join(directory, "ca-key.pem"),
        "-CAcreateserial",
        "-out",
        join(directory, "server.pem"),
        "-days",
        "1",
        "-sha256",
        "-extfile",
        join(directory, "server.ext"),
      ],
      { timeout: 30_000 },
    );
    await Promise.all(
      [
        "ca-key.pem",
        "ca.pem",
        "untrusted-ca-key.pem",
        "untrusted-ca.pem",
        "server-key.pem",
        "server.csr",
        "server.pem",
        "ca.srl",
      ].map((file) => chmod(join(directory, file), 0o600)),
    );
    await writeFile(
      join(directory, "nats.conf"),
      [
        `server_name: "${name}"`,
        'host: "0.0.0.0"',
        "port: 4222",
        "max_payload: 1048576",
        'write_deadline: "2s"',
        "debug: false",
        "trace: false",
        ...(anonymous
          ? [
              'no_auth_user: "fixture-anonymous"',
              'authorization { users: [{user: "fixture-anonymous", permissions: {publish: ">", subscribe: "qualification.allowed"}}], timeout: 2 }',
            ]
          : [`authorization { token: "${token}", timeout: 2 }`]),
        'tls { cert_file: "/fixture/server.pem", key_file: "/fixture/server-key.pem", timeout: 2 }',
        "",
      ].join("\n"),
      { mode: 0o600 },
    );
    phase = "pinned image availability";
    try {
      await execute("docker", ["image", "inspect", image], { timeout: 10_000 });
    } catch {
      // Separate network/pull work from creating an owned resource.
      await execute("docker", ["pull", "--platform", platform, image], { timeout: 120_000 });
    }
    phase = "owned container creation";
    creationStarted = true;
    const { stdout } = await execute(
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
        "--publish",
        "127.0.0.1::4222",
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
    phase = "owned private file transfer";
    // The server needs only its leaf/key/config; CA signing keys never enter the container.
    for (const file of ["server.pem", "server-key.pem", "nats.conf"])
      await execute("docker", ["cp", join(directory, file), `${container}:/fixture/${file}`], {
        timeout: 30_000,
      });
    phase = "owned container start";
    await execute("docker", ["start", container], { timeout: 30_000 });
    phase = "loopback port discovery";
    const { stdout: mapping } = await execute("docker", ["port", container, "4222/tcp"], {
      timeout: 10_000,
    });
    const match = /^127\.0\.0\.1:([0-9]+)\s*$/u.exec(mapping);
    if (match === null) {
      phase = "strict loopback port parsing";
      throw new Error("Owned NATS fixture has no loopback port.");
    }
    const port = Number(match[1]);
    if (!Number.isSafeInteger(port) || port <= 0 || port > 65_535)
      throw new Error("Owned NATS fixture port is invalid.");
    const server = `nats://localhost:${port}`;
    const ipServer = `nats://127.0.0.1:${port}`;
    const caPem = await readFile(join(directory, "ca.pem"), "utf8");
    const untrustedCaPem = await readFile(join(directory, "untrusted-ca.pem"), "utf8");
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
        if (closing) {
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
        await client.flush();
        if (client.isClosed())
          throw new Error("The owned NATS client closed before readiness confirmation.");
        return client;
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
  } catch {
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
    throw new Error(
      `The isolated NATS fixture failed during ${phase}; existing labs were unchanged.`,
    );
  }
}
