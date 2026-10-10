import { createServer, type Server, type RequestListener } from "node:http";

import { afterEach, expect, it, vi } from "vitest";

import {
  NodeBoundedJsonHttp,
  type OwnedJsonHttpPort,
} from "../../src/features/kafka/engine/bounded-json-http";
import { OwnedKafkaResources } from "../../src/features/kafka/engine/owned-kafka-resources";
import {
  mutateConnectHttp,
  readConnectHttp,
} from "../../src/features/kafka/engine/connect-http-request";
import type { OwnedHttpRequest } from "../../src/features/kafka/engine/owned-http-request";
import type { KafkaClusterServiceContext } from "../../src/features/kafka/application/types";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const servers: Server[] = [];
async function endpoint(handler: RequestListener): Promise<{ url: string; sockets: Set<object> }> {
  const server = createServer(handler),
    sockets = new Set<object>();
  servers.push(server);
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("No fixture endpoint");
  return { url: `http://127.0.0.1:${address.port}`, sockets };
}
afterEach(async () => {
  await Promise.all(
    servers.splice(0).map((server) => {
      server.closeAllConnections();
      return new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }),
  );
});
function context(url: string): KafkaClusterServiceContext {
  const signal = new AbortController().signal;
  return {
    baseUrl: url,
    signal,
    requestOwner: new OwnedKafkaResources(signal),
    authorization: () => Promise.resolve(undefined),
  };
}
it("preserves actual ACK and explicit denial without waiting for a body and confirms original socket close", async () => {
  for (const status of [202, 403]) {
    let requests = 0;
    const fixture = await endpoint((_incoming, response) => {
      requests++;
      response.writeHead(status, {
        "content-type": "application/json",
        "content-length": "1000000",
      });
      response.flushHeaders();
    });
    const owner = context(fixture.url);
    const result = await mutateConnectHttp(
      new NodeBoundedJsonHttp(),
      owner,
      AbortSignal.timeout(1000),
      "PUT",
      "/connectors/owned/pause",
    );
    expect(result).toMatchObject({
      state: status === 202 ? "acknowledged" : "rejected",
      dispatch: "attempted",
      cleanup: "confirmed",
    });
    expect(requests).toBe(1);
    await expect.poll(() => fixture.sockets.size).toBe(0);
  }
});
it("retains an unknown sent outcome after a lost reply and sends no automatic retry", async () => {
  let requests = 0;
  const fixture = await endpoint((incoming) => {
    requests++;
    incoming.resume();
    incoming.once("end", () => incoming.socket.destroy());
  });
  const result = await mutateConnectHttp(
    new NodeBoundedJsonHttp(),
    context(fixture.url),
    AbortSignal.timeout(1000),
    "PUT",
    "/config",
    { secret: "private-fixture" },
  );
  expect(result).toMatchObject({ state: "unknown", dispatch: "attempted", cleanup: "confirmed" });
  expect(requests).toBe(1);
  expect(JSON.stringify(result)).not.toContain("private-fixture");
  await expect.poll(() => fixture.sockets.size).toBe(0);
});
it("refuses unowned or revoked writes before authorization or dispatch", async () => {
  const http = new NodeBoundedJsonHttp(),
    authorization = vi.fn(() => Promise.resolve(undefined));
  const controller = new AbortController(),
    owner = new OwnedKafkaResources(controller.signal);
  for (const requestOwner of [undefined, owner]) {
    controller.abort();
    const result = await mutateConnectHttp(
      http,
      { baseUrl: "http://127.0.0.1:1", authorization, ...(requestOwner ? { requestOwner } : {}) },
      AbortSignal.timeout(1000),
      "DELETE",
      "/connector",
    );
    expect(result).toMatchObject({ state: "rejected", dispatch: "not-sent" });
  }
  expect(authorization).not.toHaveBeenCalled();
});
it("does not send credentials across an untrusted TLS endpoint", async () => {
  const fixture = await createHttpsTrustFixture((_incoming, response) => response.end("{}"));
  try {
    const result = await mutateConnectHttp(
      new NodeBoundedJsonHttp(),
      {
        ...context(fixture.origin),
        authorization: () => Promise.resolve("Bearer private-fixture"),
      },
      AbortSignal.timeout(1000),
      "POST",
      "/connectors",
      {},
    );
    expect(result).toMatchObject({ state: "rejected", dispatch: "not-sent", cleanup: "confirmed" });
    expect(fixture.requests).toHaveLength(0);
    expect(JSON.stringify(result)).not.toContain("private-fixture");
  } finally {
    await fixture.close();
  }
});
it("retains the original failed cleanup lease, fences new work and drains the original confirmation", async () => {
  let close!: () => void;
  const closed = new Promise<void>((resolve) => {
    close = resolve;
  });
  const closeLease = vi.fn(() => closed);
  const lease: OwnedHttpRequest = {
    response: Promise.resolve({ status: 200, body: null }),
    closed,
    dispatched: () => true,
    close: closeLease,
  };
  const open = vi.fn(() => lease);
  const http: OwnedJsonHttpPort = {
    open,
    request: () => Promise.reject(new Error("Must use original lease")),
  };
  const controller = new AbortController(),
    requestOwner = new OwnedKafkaResources(controller.signal);
  const c = { ...context("http://connect.fixture"), signal: controller.signal, requestOwner };
  const result = await mutateConnectHttp(http, c, AbortSignal.timeout(1000), "PUT", "/pause");
  expect(result).toMatchObject({ state: "acknowledged", cleanup: "unresolved" });
  expect(requestOwner.cleanupUnresolved).toBe(true);
  expect(
    (await mutateConnectHttp(http, c, AbortSignal.timeout(1000), "PUT", "/pause")).dispatch,
  ).toBe("not-sent");
  let drained = false;
  controller.abort();
  const draining = requestOwner.close().then(() => {
    drained = true;
  });
  await Promise.resolve();
  expect(drained).toBe(false);
  expect(open).toHaveBeenCalledTimes(1);
  close();
  await draining;
  expect(closeLease).toHaveBeenCalledTimes(2);
});
it("joins the original in-flight read when its connection is revoked", async () => {
  let admitted!: () => void;
  const received = new Promise<void>((resolve) => {
    admitted = resolve;
  });
  const fixture = await endpoint(() => admitted());
  const controller = new AbortController(),
    requestOwner = new OwnedKafkaResources(controller.signal);
  const pending = readConnectHttp(
    new NodeBoundedJsonHttp(),
    { ...context(fixture.url), signal: controller.signal, requestOwner },
    AbortSignal.timeout(5000),
    "GET",
    "/connectors",
  );
  const rejected = expect(pending).rejects.toThrow();
  await received;
  controller.abort();
  await requestOwner.close();
  await rejected;
  await expect.poll(() => fixture.sockets.size).toBe(0);
});

it("drains original pending authorization and prevents wire dispatch after revocation", async () => {
  let started!: () => void, finish!: (value: string) => void;
  const admitted = new Promise<void>((resolve) => {
    started = resolve;
  });
  const authorization = (): Promise<string> =>
    new Promise((resolve) => {
      finish = resolve;
      started();
    });
  const open = vi.fn<OwnedJsonHttpPort["open"]>();
  const http: OwnedJsonHttpPort = {
    open,
    request: () => Promise.reject(new Error("No request allowed")),
  };
  const controller = new AbortController(),
    owner = new OwnedKafkaResources(controller.signal);
  const pending = mutateConnectHttp(
    http,
    {
      baseUrl: "http://connect.fixture",
      signal: controller.signal,
      requestOwner: owner,
      authorization,
    },
    AbortSignal.timeout(5000),
    "DELETE",
    "/connectors/owned",
  );
  await admitted;
  controller.abort();
  let closed = false;
  const closing = owner.close().then(() => {
    closed = true;
  });
  await Promise.resolve();
  expect(closed).toBe(false);
  finish("Bearer private-delayed-token");
  const result = await pending;
  expect(result).toMatchObject({ state: "rejected", dispatch: "not-sent", cleanup: "confirmed" });
  expect(JSON.stringify(result)).not.toContain("private-delayed-token");
  await closing;
  expect(open).not.toHaveBeenCalled();
});
