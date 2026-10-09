import assert from "node:assert/strict";
import { once } from "node:events";
import { createConnection, createServer, type Socket } from "node:net";

import { WebSocketServer, type WebSocket } from "ws";

import { EdaApiClient } from "../../plugins/eda/backend/eda-api-client";
import { EdaAgentTunnel } from "../../plugins/eda/backend/eda-agent-tunnel";

async function main(): Promise<void> {
  const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(upstream, "listening");
  const upstreamAddress = upstream.address();
  assert(upstreamAddress !== null && typeof upstreamAddress === "object");
  let echo = false;
  let closedUpstreamConnections = 0;
  upstream.on("connection", (peer) => {
    peer.on("error", () => undefined);
    const timer = echo
      ? undefined
      : setInterval(() => {
          if (peer.readyState === peer.OPEN) peer.send(Buffer.alloc(65_536, 1));
        }, 1);
    if (echo) peer.on("message", (data) => peer.send(data));
    peer.once("close", () => {
      clearInterval(timer);
      closedUpstreamConnections += 1;
    });
  });

  const reservation = createServer();
  reservation.listen(0, "127.0.0.1");
  await once(reservation, "listening");
  const local = reservation.address();
  assert(local !== null && typeof local !== "string");
  await new Promise<void>((resolve, reject) =>
    reservation.close((error) => (error ? reject(error) : resolve())),
  );
  const client = new EdaApiClient({
    baseUrl: "https://eda.example.test",
    username: "fixture",
    password: "fixture",
  });
  client.captureTunnelAccess = (): ReturnType<EdaApiClient["captureTunnelAccess"]> =>
    Promise.resolve({
      url: `ws://127.0.0.1:${upstreamAddress.port}`,
      authorization: "fixture",
      rejectUnauthorized: false,
    });
  const tunnel = await EdaAgentTunnel.listen(client, "socket-reset-fixture", local.port);
  tunnel.activate();
  const sockets = new Set<Socket>();
  try {
    // More than the admission limit: each failed connection must release its slot.
    for (let index = 0; index < 13; index += 1) {
      echo = index === 12;
      const accepted = new Promise<WebSocket>((resolve) => upstream.once("connection", resolve));
      const socket = createConnection({ host: "127.0.0.1", port: local.port });
      sockets.add(socket);
      socket.on("error", () => socket.destroy());
      const localClosed = new Promise<void>((resolve) =>
        socket.once("close", () => {
          sockets.delete(socket);
          resolve();
        }),
      );
      const received = new Promise<Buffer>((resolve) => socket.once("data", resolve));
      const peer = await accepted;
      const upstreamClosed = once(peer, "close");
      if (echo) socket.write("after-reset");
      const data = await received;
      if (echo) {
        assert.equal(data.toString(), "after-reset");
        socket.end();
      } else {
        assert.equal(data[0], 1);
        // A real TCP reset while broker frames are arriving, not a mocked error event.
        socket.resetAndDestroy();
      }
      await Promise.all([localClosed, upstreamClosed]);
    }
    // Observe cleanup before the fixture closes the tunnel or upstream server.
    assert.equal(closedUpstreamConnections, 13);
    assert.equal(upstream.clients.size, 0);
    assert.equal(sockets.size, 0);
    process.stdout.write(
      JSON.stringify({
        outcome: "passed",
        resetConnections: 12,
        closedUpstreamConnections,
        activeUpstreamConnections: upstream.clients.size,
        roundTrip: "after-reset",
      }) + "\n",
    );
  } finally {
    for (const socket of sockets) socket.destroy();
    await tunnel.close();
    for (const peer of upstream.clients) peer.terminate();
    await new Promise<void>((resolve, reject) =>
      upstream.close((error) => (error ? reject(error) : resolve())),
    );
  }
}

void main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : "Tunnel probe failed"}\n`);
  process.exitCode = 1;
});
