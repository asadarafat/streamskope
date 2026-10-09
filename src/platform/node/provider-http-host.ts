import { timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import { KAFKA_MESSAGE_LIMITS, type StreamSkopeBackend } from "../../features/kafka/contracts";

import type { PluginRendererAsset } from "./plugins/runtime";
import { createKafkaProviderEndpoint } from "./kafka-provider";
import type { RecordExportDelivery } from "./record-export-artifacts";
import {
  ProviderHostRegistry,
  ProviderWireValidationError,
  type ProviderWireEndpoint,
} from "./provider-host";
import { ProviderSseDelivery, type ProviderSseCloseReport } from "./provider-sse-delivery";
import {
  developmentOrigin,
  resolveDevelopmentNetwork,
  type DevelopmentNetworkOptions,
} from "./host-network";

const DEFAULT_MAX_COMMAND_BODY_BYTES = 256 * 1_024;
const DEFAULT_MAX_EVENT_BYTES = 2 * 1_024 * 1_024;
const DEFAULT_MAX_QUEUED_EVENTS = 32;
const DEFAULT_MAX_QUEUED_SERIALIZED_BYTES = 8 * 1_024 * 1_024;
const DEFAULT_EVENT_WRITE_TIMEOUT_MS = 30_000;
export interface DevelopmentBackend extends StreamSkopeBackend {
  readonly exportFiles?: RecordExportDelivery;
  shutdown(): Promise<void>;
  stopStream?(): Promise<void>;
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
    readonly maxQueuedSerializedBytes?: number;
    readonly eventWriteTimeoutMs?: number;
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

interface HostRuntime {
  closing: boolean;
  readonly routes: ReadonlyMap<string, ProviderRouteRuntime>;
  readonly maxCommandBodyBytes: number;
  readonly maxEventBytes: number;
  readonly maxQueuedEvents: number;
  readonly maxQueuedMessageBytes: number;
  readonly maxQueuedMessages: number;
  readonly maxQueuedSerializedBytes: number;
  readonly eventWriteTimeoutMs: number;
  readonly rendererOrigin: string;
  readonly token: string;
}

interface ProviderRouteRuntime {
  readonly endpoint: ProviderWireEndpoint;
  readonly clients: Set<ProviderSseDelivery>;
  readonly pendingStops: Set<Promise<void>>;
  readonly cleanupFailures: unknown[];
  recovery: { state: "pending" | "failed"; readonly promise: Promise<void> } | undefined;
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

function beginRouteRecovery(runtime: HostRuntime, route: ProviderRouteRuntime): void {
  if (runtime.closing || route.recovery !== undefined) return;
  let complete = (): void => undefined;
  let fail = (_error: unknown): void => undefined;
  const promise = new Promise<void>((resolve, reject) => {
    complete = resolve;
    fail = reject;
  });
  const attempt = { state: "pending" as "pending" | "failed", promise };
  route.recovery = attempt;
  route.pendingStops.add(promise);
  // Observe now, retain failures for explicit host close, and never renew an old route owner.
  void promise.then(
    () => {
      route.pendingStops.delete(promise);
      if (route.recovery === attempt) route.recovery = undefined;
    },
    (error: unknown) => {
      route.pendingStops.delete(promise);
      route.cleanupFailures.push(error);
      if (route.recovery === attempt) attempt.state = "failed";
    },
  );
  // Admission is already closed; invoke synchronously to revoke pending provider starts.
  try {
    void route.endpoint.stopStream().then(complete, fail);
  } catch (error) {
    fail(error);
  }
}

function closeRouteClient(
  runtime: HostRuntime,
  route: ProviderRouteRuntime,
  client: ProviderSseDelivery,
  report: ProviderSseCloseReport,
): void {
  route.cleanupFailures.push(...report.cleanupFailures);
  if (!route.clients.delete(client)) return;
  if (route.clients.size === 0) beginRouteRecovery(runtime, route);
}

function rejectAdmission(
  response: ServerResponse,
  runtime: HostRuntime,
  route: ProviderRouteRuntime,
): boolean {
  if (runtime.closing) {
    sendProblem(
      response,
      503,
      "DEVELOPMENT_HOST_CLOSING",
      "The local application host is closing.",
      runtime.rendererOrigin,
    );
    return true;
  }
  if (route.recovery !== undefined) {
    const failed = route.recovery.state === "failed";
    sendProblem(
      response,
      503,
      failed ? "PROVIDER_STREAM_CLEANUP_UNCONFIRMED" : "PROVIDER_STREAM_RECOVERING",
      failed
        ? "Provider stream cleanup could not be confirmed. Restart the local StreamSkope host before reconnecting."
        : "The provider stream is stopping after renderer delivery ended. Wait for cleanup, then reconnect.",
      runtime.rendererOrigin,
    );
    return true;
  }
  return false;
}

function broadcastEvent(_runtime: HostRuntime, route: ProviderRouteRuntime, event: unknown): void {
  try {
    const parsed = route.endpoint.parseEvent(event);
    for (const client of [...route.clients]) client.enqueue(parsed);
  } catch (error) {
    for (const client of [...route.clients]) client.close(error);
  }
}

function openEventStream(
  response: ServerResponse,
  runtime: HostRuntime,
  route: ProviderRouteRuntime,
): void {
  const client = new ProviderSseDelivery({
    queue: route.endpoint.createEventQueue({
      maxEvents: runtime.maxQueuedEvents,
      maxMessageBytes: runtime.maxQueuedMessageBytes,
      maxMessages: runtime.maxQueuedMessages,
      maxSerializedBytes: runtime.maxQueuedSerializedBytes,
    }),
    response,
    maxEventBytes: runtime.maxEventBytes,
    writeTimeoutMs: runtime.eventWriteTimeoutMs,
    onClose: (report): void => closeRouteClient(runtime, route, client, report),
  });
  setCors(response, runtime.rendererOrigin);
  response.statusCode = 200;
  response.setHeader("Cache-Control", "no-cache, no-store");
  response.setHeader("Connection", "keep-alive");
  response.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  response.setHeader("X-Accel-Buffering", "no");
  response.setHeader("X-Content-Type-Options", "nosniff");
  route.clients.add(client);
  try {
    response.flushHeaders();
    client.start();
  } catch (error) {
    client.close(error);
  }
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
    // A final renderer can disappear while this request's body is still being read.
    if (rejectAdmission(response, runtime, route)) return;
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
  if (rejectAdmission(response, runtime, route)) return;
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

export async function startProviderHttpHost(
  options: DevelopmentHostOptions,
): Promise<RunningDevelopmentHost> {
  const providers = resolveDevelopmentProviders(options);
  let server: Server | undefined;
  const routes = new Map<string, ProviderRouteRuntime>();
  const subscriptions: (() => void)[] = [];
  let runtime: HostRuntime | undefined;
  let closePromise: Promise<void> | undefined;
  const close = (): Promise<void> => {
    if (closePromise !== undefined) return closePromise;
    if (runtime !== undefined) runtime.closing = true;
    const pendingStops = [...routes.values()].flatMap((route) => [...route.pendingStops]);
    let providerShutdown = Promise.resolve();
    closePromise = Promise.resolve().then(async (): Promise<void> => {
      const ownedServer = server;
      const operations: (() => void | Promise<void>)[] = [
        ...subscriptions.splice(0),
        ...[...routes.values()].flatMap((route) =>
          [...route.clients].map((client) => (): void => client.close()),
        ),
        ...(ownedServer === undefined ? [] : [(): Promise<void> => closeServer(ownedServer)]),
      ];
      const results = await Promise.allSettled([
        ...operations.map((operation) => Promise.resolve().then(operation)),
        providerShutdown,
        ...pendingStops,
      ]);
      const failures = [
        ...new Set([
          ...results
            .filter((result): result is PromiseRejectedResult => result.status === "rejected")
            .map((result) => result.reason as unknown),
          ...[...routes.values()].flatMap((route) => route.cleanupFailures),
        ]),
      ];
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
      routes.set(endpoint.id, {
        endpoint,
        clients: new Set(),
        pendingStops: new Set(),
        cleanupFailures: [],
        recovery: undefined,
      });
    }
    const selectedRuntime: HostRuntime = {
      closing: false,
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
      maxQueuedSerializedBytes: positiveInteger(
        options.maxQueuedSerializedBytes,
        DEFAULT_MAX_QUEUED_SERIALIZED_BYTES,
        "maxQueuedSerializedBytes",
      ),
      eventWriteTimeoutMs: positiveInteger(
        options.eventWriteTimeoutMs,
        DEFAULT_EVENT_WRITE_TIMEOUT_MS,
        "eventWriteTimeoutMs",
      ),
      rendererOrigin: options.rendererOrigin,
      token: options.token,
    };
    runtime = selectedRuntime;
    server = createServer((request, response) => {
      void handleRequest(request, response, selectedRuntime).catch(() => {
        sendProblem(
          response,
          500,
          "DEVELOPMENT_HOST_FAILURE",
          "The local application host could not complete the request.",
          selectedRuntime.rendererOrigin,
        );
      });
    });
    server.on("clientError", (_error, socket) => {
      socket.destroy();
    });

    for (const route of routes.values()) {
      subscriptions.push(
        route.endpoint.subscribe((wire) => broadcastEvent(selectedRuntime, route, wire)),
      );
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

/** Development hosts use the same provider admission and delivery boundary. */
export const startDevelopmentHost = startProviderHttpHost;
