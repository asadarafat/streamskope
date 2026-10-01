import { createServer, type Server, type Socket } from "node:net";

import { createWebSocketStream, WebSocket } from "ws";

import { type EdaApiClient, EdaApiError } from "./eda-api-client";

const MAXIMUM_CONNECTIONS = 8;

export class EdaAgentTunnel {
  private readonly sockets = new Set<Socket>();
  private ready = false;

  private constructor(
    private readonly server: Server,
    readonly port: number,
  ) {}

  static async listen(
    client: EdaApiClient,
    sessionId: string,
    port: number,
  ): Promise<EdaAgentTunnel> {
    const server = createServer();
    const tunnel = new EdaAgentTunnel(server, port);
    server.on("connection", (socket) => {
      if (!tunnel.ready || tunnel.sockets.size >= MAXIMUM_CONNECTIONS) {
        socket.destroy();
        return;
      }
      tunnel.sockets.add(socket);
      socket.on("close", () => tunnel.sockets.delete(socket));
      socket.pause();
      void client.captureTunnelAccess(sessionId).then(
        (access) => {
          if (socket.destroyed) return;
          const websocket = new WebSocket(access.url, {
            headers: { authorization: access.authorization },
            ...(access.caPem === undefined ? {} : { ca: access.caPem }),
            ...(access.serverName === undefined ? {} : { servername: access.serverName }),
            rejectUnauthorized: access.rejectUnauthorized,
            perMessageDeflate: false,
            handshakeTimeout: 10_000,
            maxPayload: 16 * 1_048_576,
          });
          const close = (): void => {
            websocket.terminate();
            socket.destroy();
          };
          socket.once("close", () => websocket.terminate());
          websocket.once("error", close);
          websocket.once("close", () => socket.destroy());
          websocket.once("open", () => {
            if (socket.destroyed) {
              websocket.terminate();
              return;
            }
            const stream = createWebSocketStream(websocket, {
              highWaterMark: 64 * 1024,
            });
            stream.once("error", close);
            socket.pipe(stream).pipe(socket);
            socket.resume();
          });
        },
        () => socket.destroy(),
      );
    });
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(port, "127.0.0.1", () => {
          server.off("error", reject);
          resolve();
        });
      });
    } catch (error) {
      server.close();
      if (error instanceof Error && "code" in error && error.code === "EADDRINUSE") {
        const endpoint = `127.0.0.1:${String(port)}`;
        throw new EdaApiError(`The local Kafka endpoint ${endpoint} is already in use.`, {
          cause: error,
          code: "BACKEND_UNAVAILABLE",
          recovery:
            "Choose a different Local Kafka port, or stop the existing listener on this computer, then start capture again.",
          retryable: true,
          stage: "backend",
          target: endpoint,
        });
      }
      throw error;
    }
    return tunnel;
  }

  activate(): void {
    this.ready = true;
  }

  async close(): Promise<void> {
    this.ready = false;
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => (error === undefined ? resolve() : reject(error)));
    });
  }
}
