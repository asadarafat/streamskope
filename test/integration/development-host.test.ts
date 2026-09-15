import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, request, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type StreamSkopeBackend,
} from "../../src/kafka/contracts";
import {
  launchWebDevelopment,
  startDevelopmentHost,
  type RunningDevelopmentHost,
  type RunningWebDevelopment,
} from "../../src/platform/dev-host";

const RENDERER_ORIGIN = "http://127.0.0.1:4173";
const INVOCATION_TOKEN = "0123456789abcdef0123456789abcdef";
const latencyStartCommand: Extract<HostCommand, { readonly command: "latency.start" }> = {
  command: "latency.start",
  id: "latency-start",
  payload: {
    acknowledgements: -1,
    messageCount: 5,
    timeoutMs: 10_000,
    topic: "orders.events",
  },
  version: HOST_PROTOCOL_VERSION,
};

class FakeBackend implements StreamSkopeBackend {
  readonly commands: HostCommand[] = [];
  shutdownCalls = 0;
  private readonly listeners = new Set<(event: HostEvent) => void>();

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: `correlation-${command.id}` },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  subscribe(listener: (event: HostEvent) => void): () => void {
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }

  shutdown(): Promise<void> {
    this.shutdownCalls += 1;
    return Promise.resolve();
  }
}

type AsyncCleanup = () => Promise<void>;

const cleanups: AsyncCleanup[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup();
  }
});

function trackHost(host: RunningDevelopmentHost): RunningDevelopmentHost {
  cleanups.push(() => host.close());
  return host;
}

function trackLaunch(launch: RunningWebDevelopment): RunningWebDevelopment {
  cleanups.push(() => launch.close());
  return launch;
}

function authorizedHeaders(
  origin = RENDERER_ORIGIN,
  token = INVOCATION_TOKEN,
): Record<string, string> {
  return {
    origin,
    "x-streamskope-token": token,
  };
}

async function listen(server: Server, port = 0): Promise<number> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Expected a TCP listener.");
  }
  return address.port;
}

async function closeServer(server: Server): Promise<void> {
  if (!server.listening) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

async function occupyPort(body = "unrelated owner"): Promise<{
  readonly close: AsyncCleanup;
  readonly port: number;
}> {
  const server = createServer((_request, response) => {
    response.end(body);
  });
  const port = await listen(server);
  return {
    close: () => closeServer(server),
    port,
  };
}

async function reserveFreePort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await closeServer(server);
  return port;
}

async function assertPortCanBeRebound(port: number): Promise<void> {
  const server = createServer();
  await listen(server, port);
  await closeServer(server);
}

function requestRenderer(
  port: number,
  host: string,
): Promise<{
  readonly body: string;
  readonly status: number;
}> {
  return new Promise((resolve, reject) => {
    const outgoing = request(
      {
        headers: { host },
        hostname: "127.0.0.1",
        method: "GET",
        path: "/",
        port,
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => {
          chunks.push(chunk);
        });
        response.once("end", () => {
          resolve({
            body: Buffer.concat(chunks).toString("utf8"),
            status: response.statusCode ?? 0,
          });
        });
      },
    );
    outgoing.once("error", reject);
    outgoing.end();
  });
}

async function readSseData(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Promise<HostEvent | null> {
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const result = await reader.read();
    if (result.done) {
      return null;
    }
    buffer += decoder.decode(result.value, { stream: true });
    let boundary = buffer.indexOf("\n\n");
    while (boundary >= 0) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const data = block
        .split("\n")
        .filter((line) => line.startsWith("data: "))
        .map((line) => line.slice(6))
        .join("\n");
      if (data.length > 0) {
        return JSON.parse(data) as HostEvent;
      }
      boundary = buffer.indexOf("\n\n");
    }
  }
}

async function within<T>(promise: Promise<T>, milliseconds = 3_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`Timed out after ${milliseconds} ms.`));
        }, milliseconds);
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

describe("browser development host", () => {
  it("binds loopback and reports authenticated readiness only after listening", async () => {
    const backend = new FakeBackend();
    const host = trackHost(
      await startDevelopmentHost({
        backend,
        port: 0,
        rendererOrigin: RENDERER_ORIGIN,
        token: INVOCATION_TOKEN,
      }),
    );

    expect(host.hostname).toBe("127.0.0.1");
    expect(host.origin).toBe(`http://127.0.0.1:${host.port}`);

    const response = await fetch(`${host.origin}/health`, {
      headers: authorizedHeaders(),
    });

    expect(response.status).toBe(200);
    expect(response.headers.get("access-control-allow-origin")).toBe(RENDERER_ORIGIN);
    await expect(response.json()).resolves.toEqual({
      protocolVersion: HOST_PROTOCOL_VERSION,
      status: "ready",
    });
  });

  it("binds an explicit interface and authorizes one browser-visible origin", async () => {
    const backend = new FakeBackend();
    const publicHostname = "clab.orb.local";
    const rendererOrigin = `http://${publicHostname}:4173`;
    const host = trackHost(
      await startDevelopmentHost({
        backend,
        listenHostname: "127.0.0.1",
        port: 0,
        publicHostname,
        rendererOrigin,
        token: INVOCATION_TOKEN,
      }),
    );

    expect(host.hostname).toBe(publicHostname);
    expect(host.origin).toBe(`http://${publicHostname}:${host.port}`);

    const response = await fetch(`http://127.0.0.1:${host.port}/health`, {
      headers: authorizedHeaders(rendererOrigin),
    });

    expect(response.status).toBe(200);
    expect(response.headers.get("access-control-allow-origin")).toBe(rendererOrigin);
  });

  it("rejects an unspecified public hostname before reporting readiness", async () => {
    await expect(
      startDevelopmentHost({
        backend: new FakeBackend(),
        listenHostname: "127.0.0.1",
        port: 0,
        publicHostname: "0.0.0.0",
        rendererOrigin: "http://0.0.0.0:4173",
        token: INVOCATION_TOKEN,
      }),
    ).rejects.toThrow("Public development hostname must identify a browser-reachable host.");
  });

  it("shuts down its application backend exactly once when the host closes", async () => {
    const backend = new FakeBackend();
    const host = await startDevelopmentHost({
      backend,
      port: 0,
      rendererOrigin: RENDERER_ORIGIN,
      token: INVOCATION_TOKEN,
    });

    await host.close();
    await host.close();

    expect(backend.shutdownCalls).toBe(1);
    await assertPortCanBeRebound(host.port);
  });

  it("rejects an untrusted origin, wrong token, and undeclared command", async () => {
    const backend = new FakeBackend();
    const host = trackHost(
      await startDevelopmentHost({
        backend,
        port: 0,
        rendererOrigin: RENDERER_ORIGIN,
        token: INVOCATION_TOKEN,
      }),
    );

    const wrongOrigin = await fetch(`${host.origin}/health`, {
      headers: authorizedHeaders("http://127.0.0.1:9999"),
    });
    const wrongToken = await fetch(`${host.origin}/health`, {
      headers: authorizedHeaders(RENDERER_ORIGIN, "wrong-token"),
    });
    const unsupported = await fetch(`${host.origin}/commands`, {
      body: JSON.stringify({
        command: "cluster.delete",
        id: "request-1",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
      headers: {
        ...authorizedHeaders(),
        "content-type": "application/json",
      },
      method: "POST",
    });

    expect(wrongOrigin.status).toBe(403);
    expect(wrongOrigin.headers.has("access-control-allow-origin")).toBe(false);
    expect(wrongToken.status).toBe(403);
    expect(wrongToken.headers.get("access-control-allow-origin")).toBe(RENDERER_ORIGIN);
    expect(unsupported.status).toBe(400);
    expect(backend.commands).toEqual([]);
  });

  it("rejects a command body above the configured bound before dispatch", async () => {
    const backend = new FakeBackend();
    const host = trackHost(
      await startDevelopmentHost({
        backend,
        maxCommandBodyBytes: 128,
        port: 0,
        rendererOrigin: RENDERER_ORIGIN,
        token: INVOCATION_TOKEN,
      }),
    );

    const response = await fetch(`${host.origin}/commands`, {
      body: JSON.stringify({ padding: "x".repeat(256) }),
      headers: {
        ...authorizedHeaders(),
        "content-type": "application/json",
      },
      method: "POST",
    });

    expect(response.status).toBe(413);
    expect(backend.commands).toEqual([]);
  });

  it("transports a bounded latency command and lifecycle event", async () => {
    const backend = new FakeBackend();
    const host = trackHost(
      await startDevelopmentHost({
        backend,
        port: 0,
        rendererOrigin: RENDERER_ORIGIN,
        token: INVOCATION_TOKEN,
      }),
    );
    const eventController = new AbortController();
    cleanups.push(() => {
      eventController.abort();
      return Promise.resolve();
    });
    const eventResponse = await fetch(`${host.origin}/events`, {
      headers: authorizedHeaders(),
      signal: eventController.signal,
    });
    if (eventResponse.body === null) {
      throw new Error("Expected an SSE response body.");
    }
    const receivedEvent = readSseData(eventResponse.body.getReader());

    const commandResponse = await fetch(`${host.origin}/commands`, {
      body: JSON.stringify(latencyStartCommand),
      headers: {
        ...authorizedHeaders(),
        "content-type": "application/json",
      },
      method: "POST",
    });
    expect(commandResponse.status).toBe(200);
    await expect(commandResponse.json()).resolves.toMatchObject({
      command: "latency.start",
      id: "latency-start",
      ok: true,
    });
    expect(backend.commands).toEqual([latencyStartCommand]);

    backend.emit({
      event: "latency.changed",
      payload: {
        evidence: null,
        request: latencyStartCommand.payload,
        state: "running",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });
    await expect(within(receivedEvent)).resolves.toMatchObject({
      event: "latency.changed",
      payload: { state: "running" },
      sequence: 1,
    });
  });

  it("strictly transports aggregate Stream Monitor evidence over SSE", async () => {
    const backend = new FakeBackend();
    const host = trackHost(
      await startDevelopmentHost({
        backend,
        port: 0,
        rendererOrigin: RENDERER_ORIGIN,
        token: INVOCATION_TOKEN,
      }),
    );
    const eventController = new AbortController();
    cleanups.push(() => {
      eventController.abort();
      return Promise.resolve();
    });
    const eventResponse = await fetch(`${host.origin}/events`, {
      headers: authorizedHeaders(),
      signal: eventController.signal,
    });
    if (eventResponse.body === null) {
      throw new Error("Expected an SSE response body.");
    }
    const receivedEvent = readSseData(eventResponse.body.getReader());
    const event: Extract<HostEvent, { readonly event: "streamMetrics.changed" }> = {
      event: "streamMetrics.changed",
      payload: {
        connectionName: "Local aio",
        delivery: {
          batchCount: 1,
          batchSize: 200,
          deliveredMessages: 1,
          historySamples: 50,
          intervalMs: 20,
          lastBatchMessages: 1,
          messagesPerSecond: 10,
          publicationDurationMs: 0.5,
          queueWaitMs: 1,
          receivedMessages: 1,
          tuningSource: "confirmed",
        },
        queue: {
          capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
          capacityMessages: KAFKA_MESSAGE_LIMITS.queuedMessages,
          currentBytes: 0,
          currentMessages: 0,
          droppedMessages: 0,
          droppedPerSecond: 0,
          droppedSincePrevious: 0,
          peakBytes: 64,
          peakMessages: 1,
        },
        request: {
          maxMessages: 10,
          mode: "earliest",
          topic: "orders.events",
        },
        sampledAt: "2026-07-26T12:00:00.000Z",
        state: "complete",
        status: "nominal",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    };

    backend.emit(event);

    await expect(within(receivedEvent)).resolves.toEqual(event);
    expect(JSON.stringify(event)).not.toMatch(
      /clientSecret|oauth-secret|message-key|message-payload/u,
    );
  });

  it("streams an explicit backend-loss event", async () => {
    const backend = new FakeBackend();
    const host = trackHost(
      await startDevelopmentHost({
        backend,
        port: 0,
        rendererOrigin: RENDERER_ORIGIN,
        token: INVOCATION_TOKEN,
      }),
    );
    const controller = new AbortController();
    cleanups.push(() => {
      controller.abort();
      return Promise.resolve();
    });
    const response = await fetch(`${host.origin}/events`, {
      headers: authorizedHeaders(),
      signal: controller.signal,
    });
    if (response.body === null) {
      throw new Error("Expected an SSE response body.");
    }

    const eventPromise = readSseData(response.body.getReader());
    backend.emit({
      event: "backend.availability",
      payload: {
        recovery: "Restart the local application host.",
        state: "unavailable",
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    });

    await expect(within(eventPromise)).resolves.toMatchObject({
      event: "backend.availability",
      payload: { state: "unavailable" },
      sequence: 1,
    });
  });

  it("closes an event stream rather than emitting an oversized event", async () => {
    const backend = new FakeBackend();
    const host = trackHost(
      await startDevelopmentHost({
        backend,
        maxEventBytes: 256,
        port: 0,
        rendererOrigin: RENDERER_ORIGIN,
        token: INVOCATION_TOKEN,
      }),
    );
    const response = await fetch(`${host.origin}/events`, {
      headers: authorizedHeaders(),
    });
    if (response.body === null) {
      throw new Error("Expected an SSE response body.");
    }

    const eventPromise = readSseData(response.body.getReader());
    backend.emit({
      event: "activity.recorded",
      payload: {
        correlationId: "correlation-1",
        detail: "x".repeat(220),
        id: "activity-1",
        object: "local validation",
        operation: "connection test",
        outcome: "failed",
        severity: "error",
        timestamp: "2026-07-25T11:00:00.000Z",
      },
      sequence: 2,
      version: HOST_PROTOCOL_VERSION,
    });

    await expect(within(eventPromise)).resolves.toBeNull();
  });

  it("identifies an occupied host endpoint and preserves its owner", async () => {
    const occupied = await occupyPort();
    cleanups.push(occupied.close);
    const backend = new FakeBackend();

    await expect(
      startDevelopmentHost({
        backend,
        port: occupied.port,
        rendererOrigin: RENDERER_ORIGIN,
        token: INVOCATION_TOKEN,
      }),
    ).rejects.toThrow(`127.0.0.1:${occupied.port}`);

    expect(backend.shutdownCalls).toBe(1);
    const response = await fetch(`http://127.0.0.1:${occupied.port}`);
    await expect(response.text()).resolves.toBe("unrelated owner");
  });

  it("stops the renderer it owns when host startup fails", async () => {
    const root = await mkdtemp(join(tmpdir(), "streamskope-renderer-"));
    cleanups.push(() => rm(root, { force: true, recursive: true }));
    await writeFile(join(root, "index.html"), "<main>StreamSkope test renderer</main>");
    const rendererPort = await reserveFreePort();
    const occupiedHost = await occupyPort();
    cleanups.push(occupiedHost.close);

    await expect(
      launchWebDevelopment({
        backend: new FakeBackend(),
        hostPort: occupiedHost.port,
        rendererPort,
        rendererRoot: root,
        token: INVOCATION_TOKEN,
      }),
    ).rejects.toThrow(`127.0.0.1:${occupiedHost.port}`);

    await assertPortCanBeRebound(rendererPort);
    const ownerResponse = await fetch(`http://127.0.0.1:${occupiedHost.port}`);
    await expect(ownerResponse.text()).resolves.toBe("unrelated owner");
  });

  it("reports an occupied renderer endpoint without starting the host", async () => {
    const root = await mkdtemp(join(tmpdir(), "streamskope-renderer-"));
    cleanups.push(() => rm(root, { force: true, recursive: true }));
    await writeFile(join(root, "index.html"), "<main>StreamSkope test renderer</main>");
    const occupiedRenderer = await occupyPort();
    cleanups.push(occupiedRenderer.close);
    const hostPort = await reserveFreePort();

    await expect(
      launchWebDevelopment({
        backend: new FakeBackend(),
        hostPort,
        rendererPort: occupiedRenderer.port,
        rendererRoot: root,
        token: INVOCATION_TOKEN,
      }),
    ).rejects.toThrow(`127.0.0.1:${occupiedRenderer.port}`);

    await assertPortCanBeRebound(hostPort);
    const ownerResponse = await fetch(`http://127.0.0.1:${occupiedRenderer.port}`);
    await expect(ownerResponse.text()).resolves.toBe("unrelated owner");
  });

  it("reports a clean renderer URL and keeps host authorization inside its gateway", async () => {
    const root = await mkdtemp(join(tmpdir(), "streamskope-renderer-"));
    cleanups.push(() => rm(root, { force: true, recursive: true }));
    await writeFile(join(root, "index.html"), "<main>StreamSkope test renderer</main>");
    const rendererPort = await reserveFreePort();
    const hostPort = await reserveFreePort();
    const backend = new FakeBackend();
    const launch = trackLaunch(
      await launchWebDevelopment({
        backend,
        hostPort,
        rendererPort,
        rendererRoot: root,
        token: INVOCATION_TOKEN,
      }),
    );

    expect(launch.browserUrl).toBe(`http://127.0.0.1:${rendererPort}/`);
    expect(launch.rendererOrigin).toBe(`http://127.0.0.1:${rendererPort}`);
    expect(launch.host.origin).toBe(`http://127.0.0.1:${hostPort}`);
    expect(new URL(launch.browserUrl).hash).toBe("");
    expect(new URL(launch.browserUrl).search).toBe("");

    const rendererResponse = await fetch(launch.browserUrl);
    const gatewayCookie = rendererResponse.headers.get("set-cookie")?.split(";", 1)[0];
    expect(gatewayCookie).toMatch(new RegExp(`^streamskope_dev_session_${rendererPort}=`));

    const aliasResponse = await fetch(`http://localhost:${rendererPort}/`, {
      redirect: "manual",
    });
    expect(aliasResponse.status).toBe(307);
    expect(aliasResponse.headers.get("location")).toBe(launch.browserUrl);

    const unauthenticatedGateway = await fetch(
      `${launch.rendererOrigin}/__streamskope_host/health`,
    );
    expect(unauthenticatedGateway.status).toBe(403);

    const gatewayHealth = await fetch(`${launch.rendererOrigin}/__streamskope_host/health`, {
      headers: { cookie: gatewayCookie ?? "" },
    });
    expect(gatewayHealth.status).toBe(200);
    await expect(gatewayHealth.json()).resolves.toEqual({
      protocolVersion: HOST_PROTOCOL_VERSION,
      status: "ready",
    });

    const second = trackLaunch(
      await launchWebDevelopment({
        backend: new FakeBackend(),
        hostPort: await reserveFreePort(),
        rendererPort: await reserveFreePort(),
        rendererRoot: root,
        token: INVOCATION_TOKEN,
      }),
    );
    const secondDocument = await fetch(second.browserUrl);
    const secondCookie = secondDocument.headers.get("set-cookie")?.split(";", 1)[0] ?? "";
    expect(secondCookie.split("=")[0]).not.toBe(gatewayCookie?.split("=")[0]);
    for (const origin of [launch.rendererOrigin, second.rendererOrigin]) {
      const response = await fetch(`${origin}/__streamskope_host/health`, {
        headers: { cookie: `${secondCookie}; ${gatewayCookie ?? ""}` },
      });
      expect(response.status).toBe(200);
    }
    const foreignCookie = await fetch(`${launch.rendererOrigin}/__streamskope_host/health`, {
      headers: { cookie: secondCookie },
    });
    expect(foreignCookie.status).toBe(403);

    const crossOriginGateway = await fetch(`${launch.rendererOrigin}/__streamskope_host/commands`, {
      body: JSON.stringify(latencyStartCommand),
      headers: {
        "content-type": "application/json",
        cookie: gatewayCookie ?? "",
        origin: "http://attacker.orb.local:4173",
        "sec-fetch-site": "same-site",
      },
      method: "POST",
    });
    expect(crossOriginGateway.status).toBe(403);
    expect(backend.commands).toEqual([]);

    const directHost = await fetch(`${launch.host.origin}/health`, {
      headers: { origin: launch.rendererOrigin },
    });
    expect(directHost.status).toBe(403);
  });

  it("reports an explicit browser-visible hostname as one clean address", async () => {
    const root = await mkdtemp(join(tmpdir(), "streamskope-renderer-"));
    cleanups.push(() => rm(root, { force: true, recursive: true }));
    await writeFile(
      join(root, "index.html"),
      `<meta http-equiv="Content-Security-Policy" content="default-src 'self'; connect-src http://127.0.0.1:*;">
       <main>StreamSkope test renderer</main>`,
    );
    const rendererPort = await reserveFreePort();
    const hostPort = await reserveFreePort();
    const publicHostname = "clab.orb.local";
    const launch = trackLaunch(
      await launchWebDevelopment({
        backend: new FakeBackend(),
        hostPort,
        publicHostname,
        rendererPort,
        rendererRoot: root,
        token: INVOCATION_TOKEN,
      }),
    );

    expect(launch.browserUrl).toBe(`http://${publicHostname}:${rendererPort}/`);
    expect(launch.rendererOrigin).toBe(`http://${publicHostname}:${rendererPort}`);
    expect(launch.host.origin).toBe(`http://${publicHostname}:${hostPort}`);
    expect(new URL(launch.browserUrl).hash).toBe("");
    expect(new URL(launch.browserUrl).search).toBe("");

    const rendererResponse = await requestRenderer(
      rendererPort,
      `${publicHostname}:${rendererPort}`,
    );
    expect(rendererResponse.status).toBe(200);
    expect(rendererResponse.body).toContain("StreamSkope test renderer");
    expect(rendererResponse.body).toContain(
      `connect-src http://${publicHostname}:* ws://${publicHostname}:*;`,
    );
    expect(rendererResponse.body).not.toContain("connect-src http://127.0.0.1:*");
  });
});
