import { timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import { KAFKA_MESSAGE_LIMITS, type StreamSkopeBackend } from "../../features/kafka/contracts";
import type { PluginRendererAsset } from "../node/plugins/runtime";
import { createKafkaProviderEndpoint } from "../node/kafka-provider";
import {
  ProviderHostRegistry,
  ProviderWireValidationError,
  type ProviderEventQueue,
  type ProviderWireEndpoint,
} from "../node/provider-host";
import type { ProviderWireEvent } from "../providers/host";

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
  pluginAsset?(pathname: string): Promise<PluginRendererAsset | undefined>;
}

export type DevelopmentProviderSource =
  | { readonly backend: DevelopmentBackend; readonly providers?: never }
  | { readonly providers: ProviderHostRegistry; readonly backend?: never };

export type DevelopmentHostOptions = DevelopmentNetworkOptions &
  DevelopmentProviderSource & {
    readonly maxCommandBodyBytes?: number;
    readonly maxEventBytes?: number;
    readonly maxQueuedEvents?: number;
    readonly maxQueuedMessageBytes?: number;
    readonly maxQueuedMessages?: number;
    readonly port: number;
    readonly rendererOrigin: string;
    readonly token: string;
  };

export function resolveDevelopmentProviders(
  source: DevelopmentProviderSource,
): ProviderHostRegistry {
  return (
    source.providers ?? new ProviderHostRegistry([createKafkaProviderEndpoint(source.backend)])
  );
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
  readonly queue: ProviderEventQueue;
  readonly response: ServerResponse;
}

interface HostRuntime {
  readonly routes: ReadonlyMap<string, ProviderRouteRuntime>;
  readonly maxCommandBodyBytes: number;
  readonly maxEventBytes: number;
  readonly maxQueuedEvents: number;
  readonly maxQueuedMessageBytes: number;
  readonly maxQueuedMessages: number;
  readonly rendererOrigin: string;
  readonly token: string;
}

interface ProviderRouteRuntime {
  readonly endpoint: ProviderWireEndpoint;
  readonly clients: Set<SseClient>;
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
  body: unknown,
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

function closeSseClient(route: ProviderRouteRuntime, client: SseClient): void {
  if (client.closed) {
    return;
  }
  client.closed = true;
  route.clients.delete(client);
  if (!client.response.writableEnded) {
    client.response.end();
  }
}

function serializeEvent(event: ProviderWireEvent): string {
  return `data: ${JSON.stringify(event)}\n\n`;
}

function flushSseClient(
  runtime: HostRuntime,
  route: ProviderRouteRuntime,
  client: SseClient,
): void {
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
      closeSseClient(route, client);
      return;
    }
    if (!client.response.write(serialized)) {
      client.blocked = true;
      return;
    }
  }
}

function sendSse(route: ProviderRouteRuntime, client: SseClient, event: ProviderWireEvent): void {
  if (client.closed) {
    return;
  }
  if (client.blocked) {
    if (!client.queue.enqueue(event)) {
      closeSseClient(route, client);
    }
    return;
  }
  const serialized = serializeEvent(event);
  if (!client.response.write(serialized)) {
    client.blocked = true;
  }
}

function broadcastEvent(runtime: HostRuntime, route: ProviderRouteRuntime, event: unknown): void {
  let parsed: ProviderWireEvent;
  try {
    parsed = route.endpoint.parseEvent(event);
  } catch {
    for (const client of [...route.clients]) {
      closeSseClient(route, client);
    }
    return;
  }
  const serialized = serializeEvent(parsed);
  if (Buffer.byteLength(serialized) > runtime.maxEventBytes) {
    for (const client of [...route.clients]) {
      closeSseClient(route, client);
    }
    return;
  }
  for (const client of route.clients) {
    sendSse(route, client, parsed);
  }
}

function openEventStream(
  response: ServerResponse,
  runtime: HostRuntime,
  route: ProviderRouteRuntime,
): void {
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
    queue: route.endpoint.createEventQueue({
      maxEvents: runtime.maxQueuedEvents,
      maxMessageBytes: runtime.maxQueuedMessageBytes,
      maxMessages: runtime.maxQueuedMessages,
    }),
    response,
  };
  route.clients.add(client);
  response.on("close", () => {
    closeSseClient(route, client);
  });
  response.on("drain", () => {
    flushSseClient(runtime, route, client);
  });
  response.write(": streamskope development host ready\n\n");
}

async function executeCommand(
  request: IncomingMessage,
  response: ServerResponse,
  runtime: HostRuntime,
  route: ProviderRouteRuntime,
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

  let wire: unknown;
  try {
    wire = JSON.parse(body) as unknown;
  } catch {
    sendProblem(
      response,
      400,
      "INVALID_COMMAND",
      "Command body must contain valid JSON.",
      runtime.rendererOrigin,
    );
    return;
  }

  let hostResponse;
  try {
    hostResponse = await route.endpoint.dispatch(wire);
  } catch (error) {
    if (!(error instanceof ProviderWireValidationError)) throw error;
    const invalidCommand = error.stage === "command";
    sendProblem(
      response,
      invalidCommand ? 400 : 502,
      invalidCommand ? "INVALID_COMMAND" : "INVALID_BACKEND_RESPONSE",
      invalidCommand
        ? error.message
        : "Backend response did not correlate to the submitted command.",
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
  const selected = resolveProviderRoute(url.pathname, runtime);
  if (selected === undefined) {
    sendProblem(response, 404, "NOT_FOUND", "Development-host route was not found.");
    return;
  }
  const { action, route } = selected;
  if (request.method === "GET" && action === "health") {
    sendJson(
      response,
      200,
      { protocolVersion: route.endpoint.version, status: "ready" },
      runtime.rendererOrigin,
    );
    return;
  }
  if (request.method === "GET" && action === "events") {
    openEventStream(response, runtime, route);
    return;
  }
  if (request.method === "POST" && action === "commands") {
    await executeCommand(request, response, runtime, route);
    return;
  }
  sendProblem(response, 404, "NOT_FOUND", "Development-host route was not found.");
}

function resolveProviderRoute(
  pathname: string,
  runtime: HostRuntime,
): { readonly action: string; readonly route: ProviderRouteRuntime } | undefined {
  const legacy = /^\/(commands|events|health)$/u.exec(pathname);
  const named = /^\/providers\/([a-z][a-z0-9-]{0,31})\/(commands|events|health)$/u.exec(pathname);
  const providerId = legacy === null ? named?.[1] : "kafka";
  const action = legacy?.[1] ?? named?.[2];
  if (providerId === undefined || action === undefined) return undefined;
  const route = runtime.routes.get(providerId);
  return route === undefined ? undefined : { action, route };
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
  const providers = resolveDevelopmentProviders(options);
  let server: Server | undefined;
  const routes = new Map<string, ProviderRouteRuntime>();
  const subscriptions: (() => void)[] = [];
  let closePromise: Promise<void> | undefined;
  const close = (): Promise<void> => {
    if (closePromise !== undefined) return closePromise;
    let providerShutdown = Promise.resolve();
    closePromise = Promise.resolve().then(async (): Promise<void> => {
      const ownedServer = server;
      const operations: (() => void | Promise<void>)[] = [
        ...subscriptions.splice(0),
        ...[...routes.values()].flatMap((route) =>
          [...route.clients].map((client) => (): void => closeSseClient(route, client)),
        ),
        ...(ownedServer === undefined ? [] : [(): Promise<void> => closeServer(ownedServer)]),
      ];
      const results = await Promise.allSettled([
        ...operations.map((operation) => Promise.resolve().then(operation)),
        providerShutdown,
      ]);
      const failures = results
        .filter((result): result is PromiseRejectedResult => result.status === "rejected")
        .map((result) => result.reason as unknown);
      if (failures.length > 0) {
        throw new AggregateError(failures, "Development host did not stop cleanly.");
      }
    });
    // Publish the completion barrier before closing admission for every owned provider.
    providerShutdown = providers.shutdown();
    return closePromise;
  };

  let listeningEndpoint: string | undefined;
  try {
    const network = resolveDevelopmentNetwork(options);
    validatePort(options.port);
    validateRendererOrigin(options.rendererOrigin, network.publicHostname);
    validateToken(options.token);
    for (const endpoint of providers.endpoints()) {
      routes.set(endpoint.id, { endpoint, clients: new Set() });
    }
    const runtime: HostRuntime = {
      routes,
      maxCommandBodyBytes: positiveInteger(
        options.maxCommandBodyBytes,
        DEFAULT_MAX_COMMAND_BODY_BYTES,
        "maxCommandBodyBytes",
      ),
      maxEventBytes: positiveInteger(
        options.maxEventBytes,
        DEFAULT_MAX_EVENT_BYTES,
        "maxEventBytes",
      ),
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
    server = createServer((request, response) => {
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

    for (const route of routes.values()) {
      subscriptions.push(route.endpoint.subscribe((wire) => broadcastEvent(runtime, route, wire)));
    }
    listeningEndpoint = `${network.listenHostname}:${String(options.port)}`;
    await listen(server, options.port, network.listenHostname);
    const address = server.address();
    if (address === null || typeof address === "string") {
      throw new DevelopmentHostStartupError("Development host did not expose a TCP endpoint.");
    }
    return {
      close,
      hostname: network.publicHostname,
      origin: developmentOrigin(network.publicHostname, address.port),
      port: address.port,
    };
  } catch (error) {
    let cause: unknown = error;
    try {
      await close();
    } catch (cleanupError) {
      cause = new AggregateError(
        [error, cleanupError],
        "Development host startup and cleanup failed.",
      );
    }
    if (listeningEndpoint !== undefined) {
      throw new DevelopmentHostStartupError(
        `Development host endpoint ${listeningEndpoint} is unavailable.`,
        { cause },
      );
    }
    if (cause !== error) {
      throw new DevelopmentHostStartupError("Development host startup and cleanup failed.", {
        cause,
      });
    }
    throw error;
  }
}
