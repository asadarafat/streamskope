import { createServer } from "node:http";

import type { SchemaVersionDetail } from "../../src/features/kafka/contracts";

/** Controlled read-only protocol fixture; it does not establish vendor Registry behavior. */
export function createSchemaRegistryProtocolFixture(schemas: readonly SchemaVersionDetail[]): {
  readonly lookups: number[];
  listen(): Promise<string>;
  close(): Promise<void>;
} {
  const lookups: number[] = [];
  const server = createServer((request, response) => {
    const path = new URL(request.url ?? "/", "http://fixture.invalid").pathname;
    let body: unknown;
    if (request.method === "GET") {
      if (path === "/subjects") body = [...new Set(schemas.map((schema) => schema.subject))];
      const writer = /^\/schemas\/ids\/(\d+)$/u.exec(path);
      if (writer !== null) {
        const id = Number(writer[1]);
        lookups.push(id);
        body = schemas.find((schema) => schema.id === id);
      }
      const subject = /^\/subjects\/([^/]+)\/versions(?:\/(latest|\d+))?$/u.exec(path);
      if (subject !== null) {
        const versions = schemas
          .filter((schema) => schema.subject === decodeURIComponent(subject[1]!))
          .sort((left, right) => left.version - right.version);
        body =
          subject[2] === undefined
            ? versions.map((schema) => schema.version)
            : subject[2] === "latest"
              ? versions.at(-1)
              : versions.find((schema) => schema.version === Number(subject[2]));
      }
      if (/^\/config(?:\/[^/]+)?$/u.test(path)) body = { compatibilityLevel: "BACKWARD" };
    }
    response.writeHead(body === undefined ? 404 : 200, { "content-type": "application/json" });
    response.end(
      JSON.stringify(body ?? { error_code: 40403, message: "Fixture schema unavailable" }),
    );
  });
  return {
    lookups,
    async listen(): Promise<string> {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address();
      if (address === null || typeof address === "string")
        throw new Error("Registry fixture not listening");
      return `http://127.0.0.1:${address.port}`;
    },
    close(): Promise<void> {
      if (!server.listening) return Promise.resolve();
      server.closeAllConnections();
      return new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
