import { timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  KAFKA_MESSAGE_LIMITS,
  parseCorrelatedHostResponse,
  parseHostCommand,
  parseHostEvent,
  type HostEvent,
  type StreamSkopeBackend,
} from "../../kafka/contracts";

import { SseClientEventQueue } from "./sse-client-event-queue";
import {
  developmentOrigin,
  resolveDevelopmentNetwork,
  type DevelopmentNetworkOptions,
} from "./network";

const DEFAULT_MAX_COMMAND_BODY_BYTES = 256 * 1_024;
const DEFAULT_MAX_EVENT_BYTES = 2 * 1_024 * 1_024;
const DEFAULT_MAX_QUEUED_EVENTS = 32;
export interface DevelopmentBackend extends StreamSkopeBackend {
  shutdown(): Promise<void>;
}

export interface DevelopmentHostOptions extends DevelopmentNetworkOptions {
  readonly backend: DevelopmentBackend;
  readonly maxCommandBodyBytes?: number;
  readonly maxEventBytes?: number;
  readonly maxQueuedEvents?: number;
  readonly maxQueuedMessageBytes?: number;
  readonly maxQueuedMessages?: number;
  readonly port: number;
  readonly rendererOrigin: string;
  readonly token: string;
}

export interface RunningDevelopmentHost {
  readonly hostname: string;
  readonly origin: string;
  readonly port: number;
  close(): Promise<void>;
}

export class DevelopmentHostStartupError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "DevelopmentHostStartupError";
  }
}

class CommandBodyTooLargeError extends Error {
  constructor() {
    super("Command body exceeds the configured byte limit.");
    this.name = "CommandBodyTooLargeError";
  }
}

interface SseClient {
  blocked: boolean;
  closed: boolean;
  readonly queue: SseClientEventQueue;
  readonly response: ServerResponse;
}

interface HostRuntime {
  readonly backend: DevelopmentBackend;
  readonly clients: Set<SseClient>;
  readonly maxCommandBodyBytes: number;
  readonly maxEventBytes: number;
  readonly maxQueuedEvents: number;
  readonly maxQueuedMessageBytes: number;
  readonly maxQueuedMessages: number;
  readonly rendererOrigin: string;
  readonly token: string;
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  const selected = value ?? fallback;
  if (!Number.isSafeInteger(selected) || selected <= 0) {
    throw new DevelopmentHostStartupError(`${name} must be a positive safe integer.`);
  }
  return selected;
}

function validatePort(port: number): void {
  if (!Number.isSafeInteger(port) || port < 0 || port > 65_535) {
    throw new DevelopmentHostStartupError("Development host port must be between 0 and 65535.");
  }
}

function validateRendererOrigin(origin: string, publicHostname: string): void {
  let parsed: URL;
  try {
    parsed = new URL(origin);
  } catch (error) {
    throw new DevelopmentHostStartupError("Renderer origin must be an absolute URL.", {
      cause: error,
    });
  }
  if (
    parsed.origin !== origin ||
    parsed.protocol !== "http:" ||
    parsed.hostname !== publicHostname
  ) {
    throw new DevelopmentHostStartupError(
      `Renderer origin must be an exact HTTP origin on ${publicHostname} without a path.`,
    );
  }
}

function validateToken(token: string): void {
  if (token.length < 32 || token.length > 512) {
    throw new DevelopmentHostStartupError(
      "Invocation token must contain between 32 and 512 characters.",
    );
  }
}

function header(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return typeof value === "string" ? value : undefined;
}

function tokensMatch(actual: string | undefined, expected: string): boolean {
  if (actual === undefined) {
    return false;
  }
  const actualBytes = Buffer.from(actual);
  const expectedBytes = Buffer.from(expected);
  return actualBytes.length === expectedBytes.length && timingSafeEqual(actualBytes, expectedBytes);
}

function setCors(response: ServerResponse, rendererOrigin: string): void {
  response.setHeader("Access-Control-Allow-Origin", rendererOrigin);
  response.setHeader("Vary", "Origin");
}

function sendJson(
  response: ServerResponse,
  status: number,
  body: Readonly<Record<string, unknown>>,
  rendererOrigin?: string,
): void {
  if (response.destroyed || response.writableEnded) {
    return;
  }
  if (rendererOrigin !== undefined) {
    setCors(response, rendererOrigin);
  }
  response.statusCode = status;
  response.setHeader("Cache-Control", "no-store");
  response.setHeader("Content-Type", "application/json; charset=utf-8");
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.end(JSON.stringify(body));
}

function sendProblem(
  response: ServerResponse,
  status: number,
  code: string,
  summary: string,
  rendererOrigin?: string,
): void {
  sendJson(response, status, { error: { code, summary } }, rendererOrigin);
}

function isAuthorized(request: IncomingMessage, runtime: HostRuntime): boolean {
  return (
    header(request, "origin") === runtime.rendererOrigin &&
    tokensMatch(header(request, "x-streamskope-token"), runtime.token)
  );
}

function handlePreflight(
  request: IncomingMessage,
  response: ServerResponse,
  runtime: HostRuntime,
): void {
  if (header(request, "origin") !== runtime.rendererOrigin) {
    sendProblem(response, 403, "FORBIDDEN", "Renderer origin is not authorized.");
    return;
  }
  setCors(response, runtime.rendererOrigin);
  response.statusCode = 204;
  response.setHeader("Access-Control-Allow-Headers", "content-type, x-streamskope-token");
  response.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  response.setHeader("Access-Control-Max-Age", "600");
  response.end();
}

async function readCommandBody(request: IncomingMessage, maximumBytes: number): Promise<string> {
  const declaredLength = header(request, "content-length");
  if (
    declaredLength !== undefined &&
    Number.isFinite(Number(declaredLength)) &&
    Number(declaredLength) > maximumBytes
  ) {
    request.resume();
    throw new CommandBodyTooLargeError();
  }

  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
    bytes += buffer.byteLength;
    if (bytes > maximumBytes) {
      request.resume();
      throw new CommandBodyTooLargeError();
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function closeSseClient(runtime: HostRuntime, client: SseClient): void {
  if (client.closed) {
    return;
  }
  client.closed = true;
  runtime.clients.delete(client);
  if (!client.response.writableEnded) {
    client.response.end();
  }
}

function serializeEvent(event: HostEvent): string {
  return `data: ${JSON.stringify(event)}\n\n`;
}

function flushSseClient(runtime: HostRuntime, client: SseClient): void {
  if (client.closed) {
    return;
  }
  client.blocked = false;
  while (client.queue.length > 0) {
    const next = client.queue.dequeue();
    if (next === undefined) {
      return;
    }
    const serialized = serializeEvent(next);
    if (Buffer.byteLength(serialized) > runtime.maxEventBytes) {
      closeSseClient(runtime, client);
      return;
    }
    if (!client.response.write(serialized)) {
      client.blocked = true;
      return;
    }
  }
}

function sendSse(runtime: HostRuntime, client: SseClient, event: HostEvent): void {
  if (client.closed) {
    return;
  }
  if (client.blocked) {
    if (!client.queue.enqueue(event)) {
      closeSseClient(runtime, client);
    }
    return;
  }
  const serialized = serializeEvent(event);
  if (!client.response.write(serialized)) {
    client.blocked = true;
  }
}

function broadcastEvent(runtime: HostRuntime, event: HostEvent): void {
  let parsed: HostEvent;
  try {
    parsed = parseHostEvent(event);
  } catch {
    for (const client of [...runtime.clients]) {
      closeSseClient(runtime, client);
    }
    return;
  }
  const serialized = serializeEvent(parsed);
  if (Buffer.byteLength(serialized) > runtime.maxEventBytes) {
    for (const client of [...runtime.clients]) {
      closeSseClient(runtime, client);
    }
    return;
  }
  for (const client of runtime.clients) {
    sendSse(runtime, client, parsed);
  }
}

function openEventStream(response: ServerResponse, runtime: HostRuntime): void {
  setCors(response, runtime.rendererOrigin);
  response.statusCode = 200;
  response.setHeader("Cache-Control", "no-cache, no-store");
  response.setHeader("Connection", "keep-alive");
  response.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  response.setHeader("X-Accel-Buffering", "no");
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.flushHeaders();

  const client: SseClient = {
    blocked: false,
    closed: false,
    queue: new SseClientEventQueue({
      maxEvents: runtime.maxQueuedEvents,
      maxMessageBytes: runtime.maxQueuedMessageBytes,
      maxMessages: runtime.maxQueuedMessages,
    }),
    response,
  };
  runtime.clients.add(client);
  response.on("close", () => {
    closeSseClient(runtime, client);
  });
  response.on("drain", () => {
    flushSseClient(runtime, client);
  });
  response.write(": streamskope development host ready\n\n");
}

async function executeCommand(
  request: IncomingMessage,
  response: ServerResponse,
  runtime: HostRuntime,
): Promise<void> {
  if (!header(request, "content-type")?.toLowerCase().startsWith("application/json")) {
    sendProblem(
      response,
      415,
      "UNSUPPORTED_MEDIA_TYPE",
      "Commands require application/json.",
      runtime.rendererOrigin,
    );
    return;
  }

  let body: string;
  try {
    body = await readCommandBody(request, runtime.maxCommandBodyBytes);
  } catch (error) {
    if (error instanceof CommandBodyTooLargeError) {
      sendProblem(
        response,
        413,
        "BODY_TOO_LARGE",
        "Command body exceeds the configured byte limit.",
        runtime.rendererOrigin,
      );
      return;
    }
    throw error;
  }

  let command;
  try {
    command = parseHostCommand(JSON.parse(body) as unknown);
  } catch (error) {
    const summary =
      error instanceof HostContractValidationError
        ? error.message
        : "Command body must contain valid JSON.";
    sendProblem(response, 400, "INVALID_COMMAND", summary, runtime.rendererOrigin);
    return;
  }

  const rawResponse = await runtime.backend.execute(command);
  let hostResponse;
  try {
    hostResponse = parseCorrelatedHostResponse(rawResponse, command);
  } catch {
    sendProblem(
      response,
      502,
      "INVALID_BACKEND_RESPONSE",
      "Backend response did not correlate to the submitted command.",
      runtime.rendererOrigin,
    );
    return;
  }
  sendJson(response, 200, hostResponse, runtime.rendererOrigin);
}

async function handleRequest(
  request: IncomingMessage,
  response: ServerResponse,
  runtime: HostRuntime,
): Promise<void> {
  if (request.method === "OPTIONS") {
    handlePreflight(request, response, runtime);
    return;
  }
  if (!isAuthorized(request, runtime)) {
    sendProblem(
      response,
      403,
      "FORBIDDEN",
      "Development-host request is not authorized.",
      header(request, "origin") === runtime.rendererOrigin ? runtime.rendererOrigin : undefined,
    );
    return;
  }

  const url = new URL(request.url ?? "/", "http://127.0.0.1");
  if (request.method === "GET" && url.pathname === "/health") {
    sendJson(
      response,
      200,
      { protocolVersion: HOST_PROTOCOL_VERSION, status: "ready" },
      runtime.rendererOrigin,
    );
    return;
  }
  if (request.method === "GET" && url.pathname === "/events") {
    openEventStream(response, runtime);
    return;
  }
  if (request.method === "POST" && url.pathname === "/commands") {
    await executeCommand(request, response, runtime);
    return;
  }
  sendProblem(response, 404, "NOT_FOUND", "Development-host route was not found.");
}

function listen(server: Server, port: number, hostname: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error): void => {
      reject(error);
    };
    server.once("error", onError);
    server.listen(port, hostname, () => {
      server.off("error", onError);
      resolve();
    });
  });
}

function closeServer(server: Server): Promise<void> {
  if (!server.listening) {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
    server.closeAllConnections();
  });
}

export async function startDevelopmentHost(
  options: DevelopmentHostOptions,
): Promise<RunningDevelopmentHost> {
  const network = resolveDevelopmentNetwork(options);
  validatePort(options.port);
  validateRendererOrigin(options.rendererOrigin, network.publicHostname);
  validateToken(options.token);
  const runtime: HostRuntime = {
    backend: options.backend,
    clients: new Set(),
    maxCommandBodyBytes: positiveInteger(
      options.maxCommandBodyBytes,
      DEFAULT_MAX_COMMAND_BODY_BYTES,
      "maxCommandBodyBytes",
    ),
    maxEventBytes: positiveInteger(options.maxEventBytes, DEFAULT_MAX_EVENT_BYTES, "maxEventBytes"),
    maxQueuedEvents: positiveInteger(
      options.maxQueuedEvents,
      DEFAULT_MAX_QUEUED_EVENTS,
      "maxQueuedEvents",
    ),
    maxQueuedMessageBytes: positiveInteger(
      options.maxQueuedMessageBytes,
      KAFKA_MESSAGE_LIMITS.queuedBytes,
      "maxQueuedMessageBytes",
    ),
    maxQueuedMessages: positiveInteger(
      options.maxQueuedMessages,
      KAFKA_MESSAGE_LIMITS.queuedMessages,
      "maxQueuedMessages",
    ),
    rendererOrigin: options.rendererOrigin,
    token: options.token,
  };
  const server = createServer((request, response) => {
    void handleRequest(request, response, runtime).catch(() => {
      sendProblem(
        response,
        500,
        "DEVELOPMENT_HOST_FAILURE",
        "The local application host could not complete the request.",
        runtime.rendererOrigin,
      );
    });
  });
  server.on("clientError", (_error, socket) => {
    socket.destroy();
  });

  const unsubscribe = runtime.backend.subscribe((event) => {
    broadcastEvent(runtime, event);
  });
  try {
    await listen(server, options.port, network.listenHostname);
  } catch (error) {
    unsubscribe();
    const cleanup = await Promise.allSettled([closeServer(server), runtime.backend.shutdown()]);
    const cleanupFailures = cleanup
      .filter((result): result is PromiseRejectedResult => result.status === "rejected")
      .map((result) => result.reason as unknown);
    throw new DevelopmentHostStartupError(
      `Development host endpoint ${network.listenHostname}:${options.port} is unavailable.`,
      {
        cause:
          cleanupFailures.length === 0
            ? error
            : new AggregateError(
                [error, ...cleanupFailures],
                "Development host startup and cleanup failed.",
              ),
      },
    );
  }

  const address = server.address();
  if (address === null || typeof address === "string") {
    unsubscribe();
    await closeServer(server);
    throw new DevelopmentHostStartupError("Development host did not expose a TCP endpoint.");
  }

  let closePromise: Promise<void> | undefined;
  const close = (): Promise<void> => {
    closePromise ??= (async (): Promise<void> => {
      unsubscribe();
      for (const client of [...runtime.clients]) {
        closeSseClient(runtime, client);
      }
      const results = await Promise.allSettled([closeServer(server), runtime.backend.shutdown()]);
      const failures = results
        .filter((result): result is PromiseRejectedResult => result.status === "rejected")
        .map((result) => result.reason as unknown);
      if (failures.length > 0) {
        throw new AggregateError(failures, "Development host did not stop cleanly.");
      }
    })();
    return closePromise;
  };

  return {
    close,
    hostname: network.publicHostname,
    origin: developmentOrigin(network.publicHostname, address.port),
    port: address.port,
  };
}
