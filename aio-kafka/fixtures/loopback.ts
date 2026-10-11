import { createServer, type Server } from "node:net";

/** Hold every reservation until the set is complete, preventing intra-fixture port reuse. */
export async function loopbackPorts(count: number): Promise<number[]> {
  if (!Number.isSafeInteger(count) || count < 1 || count > 32)
    throw new Error("Invalid AIO Kafka fixture port count.");
  const servers: Server[] = [];
  const ports: number[] = [];
  try {
    for (let index = 0; index < count; index++) {
      const server = createServer();
      servers.push(server);
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address();
      if (address === null || typeof address === "string")
        throw new Error("AIO Kafka fixture port allocation failed.");
      ports.push(address.port);
    }
    return ports;
  } finally {
    await Promise.all(
      servers.map((server) =>
        server.listening
          ? new Promise<void>((resolve, reject) =>
              server.close((error) => (error ? reject(error) : resolve())),
            )
          : Promise.resolve(),
      ),
    );
  }
}
