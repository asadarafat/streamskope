import { request as httpRequest, type IncomingMessage, type ServerResponse } from "node:http";

import { GatewayProblem } from "./web-gateway-errors";
import { readJson } from "./web-gateway-http";

export interface WebGatewayUpstream {
  close(): void;
}
interface WebGatewayProxyOptions {
  readonly request: IncomingMessage;
  readonly response: ServerResponse;
  readonly path: string;
  readonly port: number;
  readonly origin: string;
  readonly token: string;
  readonly upstreams: Set<WebGatewayUpstream>;
  readonly authorized: () => boolean;
  readonly discardPluginFile?: (commandId: string) => void;
}

/** Keep private invocation authority server-side and preserve stream backpressure. */
export async function proxyWebGatewayProvider(options: WebGatewayProxyOptions): Promise<void> {
  const { request, response } = options;
  const path = options.path;
  const action = /^\/(?:providers\/[a-z][a-z0-9-]{0,31}\/)?(commands|events|health)$/u.exec(
    path,
  )?.[1];
  if (action === undefined || request.method !== (action === "commands" ? "POST" : "GET")) {
    throw new GatewayProblem(404, "NOT_FOUND", "Provider route was not found.");
  }
  let commandId: string | undefined;
  let bytes: Buffer | undefined;
  if (action === "commands") {
    const wire = await readJson(request, 256 * 1_024);
    if (wire !== null && typeof wire === "object" && "id" in wire && typeof wire.id === "string")
      commandId = wire.id;
    bytes = Buffer.from(JSON.stringify(wire));
    if (!options.authorized()) throw new GatewayProblem(401, "LOCKED", "The session has ended.");
  }
  try {
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      let stream: IncomingMessage | undefined;
      const outgoing = httpRequest(
        {
          hostname: "127.0.0.1",
          port: options.port,
          path,
          method: request.method,
          headers: {
            origin: options.origin,
            "x-streamskope-token": options.token,
            ...(bytes === undefined
              ? {}
              : { "content-type": "application/json", "content-length": String(bytes.length) }),
          },
        },
        (incoming) => {
          stream = incoming;
          response.statusCode = incoming.statusCode ?? 502;
          for (const name of ["content-type", "cache-control", "x-accel-buffering"]) {
            const value = incoming.headers[name];
            if (typeof value === "string") response.setHeader(name, value);
          }
          incoming.once("error", reject);
          incoming.once("end", () => {
            settled = true;
            resolve();
          });
          incoming.pipe(response);
        },
      );
      const upstream: WebGatewayUpstream = {
        close: (): void => {
          try {
            if (action === "events") {
              // Ending delivery does not confirm provider cleanup or release its vault authority.
              stream?.unpipe(response);
              if (!response.destroyed && !response.writableEnded) response.end();
              settled = true;
              resolve();
            }
          } finally {
            outgoing.destroy();
          }
        },
      };
      options.upstreams.add(upstream);
      outgoing.once("error", (error) => {
        if (!settled) reject(error);
      });
      outgoing.once("close", () => options.upstreams.delete(upstream));
      response.once("close", () => outgoing.destroy());
      outgoing.end(bytes);
    });
  } finally {
    if (commandId !== undefined) options.discardPluginFile?.(commandId);
  }
}
