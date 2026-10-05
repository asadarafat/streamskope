// Independent public SDK/bundle TLS identity probe; counts precede fixture teardown.
import { execFile } from "node:child_process";
import { once } from "node:events";
import { chmod, mkdtemp, readFile, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import process from "node:process";
import { clearTimeout, setTimeout } from "node:timers";
import { createSecureContext, TLSSocket } from "node:tls";
import { promisify } from "node:util";

if (!isAbsolute(process.argv[2] ?? ""))
  throw new Error("An absolute public SDK module is required.");
const { connect } = createRequire(import.meta.url)(process.argv[2]);
const execute = promisify(execFile);
const observationMs = 4_000;
const unhandledRejections = [];
const onUnhandled = () => {
  unhandledRejections.push("unhandled public SDK rejection");
};
process.on("unhandledRejection", onUnhandled);

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function within(work, label) {
  let timer;
  try {
    return await Promise.race([
      work,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(label)), observationMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
function hostnameFailure(error) {
  const seen = new Set();
  for (let value = error; value !== undefined && seen.size < 8; value = value?.cause) {
    if (value === null || typeof value !== "object" || seen.has(value)) return false;
    seen.add(value);
    if (value.code === "ERR_TLS_CERT_ALTNAME_INVALID") return true;
  }
  return false;
}
async function closePublic(connection) {
  const results = await Promise.allSettled([
    Promise.resolve().then(() => connection.close()),
    Promise.resolve().then(() => connection.closed()),
  ]);
  if (results.some((item) => item.status === "rejected"))
    throw new Error("Public TLS client cleanup did not finish.");
}

async function probe(mode, cert, key) {
  const peers = new Set();
  let accepted = 0;
  let closed = 0;
  let pings = 0;
  let connectWork;
  let connection;
  let teardownStarted = false;
  let lateCleanup;
  let lateCleanupFailed = false;
  let facts;
  const teardownFailures = [];
  const context = createSecureContext({ cert, key });
  const server = createServer((socket) => {
    accepted += 1;
    const peer = { raw: socket, secured: undefined, closed: false };
    peers.add(peer);
    const close = () => {
      if (peer.closed) return;
      peer.closed = true;
      peers.delete(peer);
      closed += 1;
    };
    socket.on("error", () => undefined);
    socket.on("close", close);
    socket.write(
      `INFO ${JSON.stringify({
        server_id: "STREAMSKOPE_OWNED_TLS_IDENTITY_PROBE",
        server_name: "streamskope-tls-identity",
        version: "2.15.0",
        proto: 1,
        host: "127.0.0.1",
        port: server.address().port,
        headers: true,
        max_payload: 1_048_576,
        tls_required: true,
        tls_available: true,
        auth_required: false,
      })}\r\n`,
    );
    const secured = new TLSSocket(socket, { isServer: true, secureContext: context });
    peer.secured = secured;
    secured.on("error", () => undefined);
    secured.on("close", close);
    let input = "";
    secured.on("data", (data) => {
      input += data.toString("utf8");
      if (input.length > 64 * 1_024) {
        secured.destroy();
        return;
      }
      const lines = input.split("\r\n");
      input = lines.pop();
      for (const line of lines)
        if (line === "PING") {
          pings += 1;
          secured.write("PONG\r\n");
        }
    });
    secured.on("end", () => secured.end());
  });
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const hostname = mode === "dns-match" ? "localhost" : "127.0.0.1";
    connectWork = Promise.resolve()
      .then(() =>
        connect({
          servers: [`nats://${hostname}:${server.address().port}`],
          timeout: 500,
          reconnect: false,
          waitOnFirstConnect: false,
          noRandomize: true,
          ignoreClusterUpdates: true,
          debug: false,
          tls: { rejectUnauthorized: true, ca: cert },
        }),
      )
      .then(
        async (value) => {
          connection = value;
          if (teardownStarted) {
            lateCleanup = Promise.resolve().then(async () => {
              try {
                await closePublic(value);
              } catch {
                lateCleanupFailed = true;
                throw new Error("Late public TLS client cleanup failed.");
              }
            });
            void lateCleanup.catch(() => undefined);
            await lateCleanup;
            return { outcome: "late-return", hostnameRejected: false, pongConfirmed: false };
          }
          if (value.isClosed())
            return { outcome: "closed-return", hostnameRejected: false, pongConfirmed: false };
          await within(value.flush(), "Public TLS PONG did not arrive.");
          const pongConfirmed = !value.isClosed();
          await closePublic(value);
          return { outcome: "accepted", hostnameRejected: false, pongConfirmed };
        },
        (error) => ({
          outcome: "rejected",
          hostnameRejected: hostnameFailure(error),
          pongConfirmed: false,
        }),
      );
    const outcome = await within(connectWork, "Public TLS connect did not settle.");
    await wait(500);
    facts = {
      mode,
      ...outcome,
      connected: outcome.outcome === "accepted" && outcome.pongConfirmed,
      accepted,
      pings,
      closedBeforeFixtureTeardown: closed,
      activeBeforeFixtureTeardown: peers.size,
      identityConfirmed:
        mode === "dns-match"
          ? outcome.outcome === "accepted" && outcome.pongConfirmed
          : outcome.outcome === "rejected" && outcome.hostnameRejected,
      peerCleanupConfirmed: accepted > 0 && closed === accepted && peers.size === 0,
    };
  } catch {
    facts = {
      mode,
      outcome: "observation-failed",
      connected: false,
      hostnameRejected: false,
      accepted,
      pings,
      closedBeforeFixtureTeardown: closed,
      activeBeforeFixtureTeardown: peers.size,
      identityConfirmed: false,
      peerCleanupConfirmed: false,
    };
  } finally {
    teardownStarted = true;
    const results = await Promise.allSettled([
      Promise.resolve().then(async () => {
        if (connection !== undefined) {
          await closePublic(connection);
        }
      }),
      Promise.resolve().then(async () => {
        const stopped = new Promise((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        );
        for (const peer of peers) {
          peer.secured?.destroy();
          peer.raw.destroy();
        }
        await stopped;
      }),
    ]);
    if (results.some((item) => item.status === "rejected"))
      teardownFailures.push("owned TLS peer cleanup failed");
    if (connectWork !== undefined)
      try {
        await within(connectWork, "Public TLS work remained pending.");
      } catch {
        teardownFailures.push("public TLS work remained pending after teardown");
      }
    if (lateCleanup !== undefined)
      try {
        await within(lateCleanup, "Late public TLS cleanup remained pending.");
      } catch {
        teardownFailures.push("late public TLS cleanup did not finish");
      }
    if (lateCleanupFailed) teardownFailures.push("late public TLS client cleanup failed");
  }
  return { ...facts, teardownFailures };
}

let directory;
try {
  directory = await mkdtemp(join(tmpdir(), "streamskope-nats-tls-identity-"));
  await chmod(directory, 0o700);
  const certPath = join(directory, "peer.pem");
  const keyPath = join(directory, "peer-key.pem");
  await execute(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      keyPath,
      "-out",
      certPath,
      "-days",
      "1",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
    ],
    { timeout: 30_000 },
  );
  await chmod(certPath, 0o600);
  await chmod(keyPath, 0o600);
  const [cert, key] = await Promise.all([readFile(certPath, "utf8"), readFile(keyPath, "utf8")]);
  const results = [];
  for (const mode of ["dns-match", "ip-mismatch"]) results.push(await probe(mode, cert, key));
  await wait(100);
  process.stdout.write(JSON.stringify({ results, unhandledRejections }) + "\n");
} catch {
  process.stdout.write(
    JSON.stringify({ results: [], unhandledRejections, fixtureFailure: true }) + "\n",
  );
  process.exitCode = 1;
} finally {
  process.removeListener("unhandledRejection", onUnhandled);
  if (directory !== undefined) await rm(directory, { recursive: true, force: true });
}
