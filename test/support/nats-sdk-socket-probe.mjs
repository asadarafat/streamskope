// Independent public SDK/bundle probe; peer-side counts precede fixture teardown.
import { createRequire } from "node:module";
import process from "node:process";
import { setTimeout, clearTimeout } from "node:timers";
import { once } from "node:events";
import { createServer } from "node:net";
import { performance } from "node:perf_hooks";

const { connect } = createRequire(import.meta.url)(process.argv[2]);

const handshakeTimeoutMs = 500;
const observationTimeoutMs = 3_000;
const postRejectionObservationMs = 1_500;
const cases =
  process.argv[3] === undefined
    ? ["silent", "late-info", "no-pong", "tls-handshake", "tls-upgrade", "malformed-info"]
    : JSON.parse(process.argv[3]);
const unhandled = [];
const onUnhandled = (reason) => {
  // Record a safe fact only; never print raw protocol/errors/credentials.
  unhandled.push(reason instanceof Error ? reason.name : typeof reason);
};
process.on("unhandledRejection", onUnhandled);

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function within(promise, ms, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(label)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function runPeer(mode) {
  const sockets = new Set();
  const scheduled = new Set();
  let accepted = 0;
  let closed = 0;
  let bytesReceived = 0;
  let connection;
  let connectWork;
  let teardownStarted = false;
  let lateCloseWork;
  let outcome;
  let result;
  let fallbackServer;
  const teardownFailures = [];
  const server = createServer((socket) => {
    accepted += 1;
    sockets.add(socket);
    socket.on("data", (data) => {
      bytesReceived += data.length;
      // Failure peers deliberately withhold PONG; the healthy fallback confirms it.
      if (mode === "fallback" && data.toString("utf8").includes("PING\r\n"))
        socket.write("PONG\r\n");
    });
    socket.on("error", () => undefined);
    socket.on("close", () => {
      sockets.delete(socket);
      closed += 1;
    });
    if (mode === "silent" || mode === "tls-handshake") return;
    const sendInfo = () => {
      if (socket.destroyed) return;
      socket.write(
        `INFO ${JSON.stringify({
          server_id: "STREAMSKOPE_OWNED_SOCKET_FIXTURE",
          server_name: "streamskope-owned-socket-fixture",
          version: "2.15.0",
          proto: 1,
          host: "127.0.0.1",
          port: server.address().port,
          headers: true,
          max_payload: 1_048_576,
          tls_required: mode === "tls-upgrade",
          tls_available: mode === "tls-upgrade",
          auth_required: false,
        })}\r\n`,
      );
    };
    if (mode === "malformed-info") socket.write("INFO {broken}\r\n");
    else if (mode === "no-pong" || mode === "tls-upgrade" || mode === "fallback") sendInfo();
    else {
      const timer = setTimeout(() => {
        scheduled.delete(timer);
        sendInfo();
      }, handshakeTimeoutMs + 250);
      scheduled.add(timer);
    }
  });
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    if (mode === "fallback") {
      fallbackServer = createServer((socket) => {
        accepted += 1;
        sockets.add(socket);
        socket.on("error", () => undefined);
        socket.on("close", () => {
          sockets.delete(socket);
          closed += 1;
        });
        socket.write("INFO {broken}\r\n");
      });
      fallbackServer.listen(0, "127.0.0.1");
      await once(fallbackServer, "listening");
    }
    const startedAt = performance.now();
    // Observe the actual work before evaluating cancellation/deadline outcomes.
    // This test has no caller-side cancellation and never hides an SDK socket.
    connectWork = Promise.resolve()
      .then(() =>
        connect({
          servers: [
            ...(fallbackServer === undefined
              ? []
              : [`nats://127.0.0.1:${fallbackServer.address().port}`]),
            `nats://127.0.0.1:${server.address().port}`,
          ],
          timeout: handshakeTimeoutMs,
          reconnect: false,
          waitOnFirstConnect: false,
          noRandomize: true,
          ignoreClusterUpdates: true,
          debug: false,
          tls:
            mode === "tls-handshake"
              ? { handshakeFirst: true, rejectUnauthorized: true }
              : mode === "tls-upgrade"
                ? { rejectUnauthorized: true }
                : null,
        }),
      )
      .then(
        async (value) => {
          connection = value;
          if (teardownStarted) {
            lateCloseWork = Promise.resolve().then(async () => {
              await value.close();
              await value.closed();
            });
            // Observe late cleanup immediately, retaining the actual work below.
            void lateCloseWork.catch(() => undefined);
          }
          if (mode === "fallback") {
            if (value.isClosed()) return { kind: "closed-return" };
            await within(value.flush(), observationTimeoutMs, "Fallback PONG did not arrive");
            if (value.isClosed()) return { kind: "closed-return" };
            await value.close();
            await value.closed();
          }
          return { kind: "resolved" };
        },
        () => ({ kind: "rejected" }),
      );
    outcome = await within(connectWork, observationTimeoutMs, "SDK connect did not settle");
    const settledAfterMs = performance.now() - startedAt;
    // Independent peer-side close observation happens BEFORE our teardown.
    await wait(postRejectionObservationMs);
    result = {
      mode,
      outcome: outcome.kind,
      settledAfterMs: Math.round(settledAfterMs),
      accepted,
      closedBeforeFixtureTeardown: closed,
      activeBeforeFixtureTeardown: sockets.size,
      bytesReceived,
      boundedFailure: outcome.kind === "rejected" && settledAfterMs < observationTimeoutMs,
      fallbackConfirmed: mode === "fallback" && outcome.kind === "resolved" && accepted === 2,
      socketCleanupConfirmed: accepted > 0 && sockets.size === 0 && closed === accepted,
    };
  } catch (error) {
    result = {
      mode,
      outcome: "observation-failed",
      accepted,
      closedBeforeFixtureTeardown: closed,
      activeBeforeFixtureTeardown: sockets.size,
      boundedFailure: false,
      fallbackConfirmed: false,
      socketCleanupConfirmed: false,
      safeFailure: error instanceof Error ? error.name : typeof error,
    };
  } finally {
    teardownStarted = true;
    for (const timer of scheduled) clearTimeout(timer);
    // Own only this fixture's sockets/server and any public returned connection.
    // A failure in one cleanup must not skip another cleanup owner.
    const cleanup = [
      Promise.resolve().then(async () => {
        if (connection !== undefined) {
          await connection.close();
          await connection.closed();
        }
      }),
      Promise.resolve().then(async () => {
        const closures = [server, ...(fallbackServer === undefined ? [] : [fallbackServer])].map(
          (owned) =>
            new Promise((resolve, reject) => {
              owned.close((error) => (error ? reject(error) : resolve()));
            }),
        );
        for (const socket of sockets) socket.destroy();
        await Promise.all(closures);
      }),
    ];
    const settled = await Promise.allSettled(cleanup);
    for (const item of settled) {
      if (item.status === "rejected") teardownFailures.push("fixture cleanup failed");
    }
    if (connectWork !== undefined) {
      try {
        await within(
          connectWork,
          observationTimeoutMs,
          "SDK connect remains pending after teardown",
        );
      } catch {
        teardownFailures.push("SDK work remains unresolved after fixture teardown");
      }
    }
    if (lateCloseWork !== undefined) {
      try {
        await within(
          lateCloseWork,
          observationTimeoutMs,
          "Late connection cleanup remains pending",
        );
      } catch {
        teardownFailures.push("Late connection cleanup did not finish");
      }
    }
  }
  return { ...result, teardownFailures };
}

try {
  const results = [];
  for (const mode of cases) results.push(await runPeer(mode));
  await wait(100);
  process.stdout.write(JSON.stringify({ results, unhandledRejections: unhandled }) + "\n");
} finally {
  process.removeListener("unhandledRejection", onUnhandled);
}
