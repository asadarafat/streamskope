import { createServer, type RequestListener, type Server } from "node:http";

import { afterEach, describe, expect, it } from "vitest";

import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { requestOAuthToken } from "../../src/features/kafka/engine/oauth";
import { parsePemTrustMaterial } from "../../src/features/kafka/engine/trust-material-shared";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const servers = new Set<Server>();
const tlsFixtures: Awaited<ReturnType<typeof createHttpsTrustFixture>>[] = [];

async function endpoint(handler: RequestListener): Promise<string> {
  const server = createServer(handler);
  servers.add(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Bounded HTTP fixture did not expose a TCP port.");
  }
  return `http://127.0.0.1:${String(address.port)}`;
}

afterEach(async () => {
  await Promise.all(tlsFixtures.splice(0).map((fixture) => fixture.close()));
  await Promise.all(
    [...servers].map(
      (server) =>
        new Promise<void>((resolve, reject) => {
          server.close((error) => {
            servers.delete(server);
            if (error === undefined) resolve();
            else reject(error);
          });
        }),
    ),
  );
});

describe("bounded host JSON transport", () => {
  it("uses a combined profile CA bundle for separately issued OAuth and service certificates", async () => {
    const oauth = await createHttpsTrustFixture((_request, response) => {
      response.end('{"access_token":"fixture-token"}');
    });
    tlsFixtures.push(oauth);
    const registry = await createHttpsTrustFixture((_request, response) => {
      response.end('["example-value"]');
    });
    tlsFixtures.push(registry);
    const http = new NodeBoundedJsonHttp();
    const signal = new AbortController().signal;
    // Reproduce the operator failure: the first CA cannot validate the second service.
    await expect(
      http.request({
        caPem: oauth.caPem,
        url: registry.origin,
        method: "GET",
        signal,
        authorization: "Bearer fixture-token",
      }),
    ).rejects.toThrow();
    expect(registry.requests).toHaveLength(0);
    const { caPem } = parsePemTrustMaterial({
      kind: "pem",
      material: `${oauth.caPem}\n${registry.caPem}`,
    });
    await expect(
      requestOAuthToken({
        caPem,
        tokenEndpoint: oauth.origin,
        clientId: "fixture",
        clientSecret: "fixture",
        scope: "",
        signal,
      }),
    ).resolves.toMatchObject({ value: "fixture-token" });
    await expect(
      http.request({ caPem, url: registry.origin, method: "GET", signal }),
    ).resolves.toMatchObject({ status: 200, body: ["example-value"] });
  });

  it("sends an explicit authorization value and returns bounded JSON", async () => {
    let observedAuthorization: string | undefined;
    const url = await endpoint((request, response) => {
      observedAuthorization = request.headers.authorization;
      response.setHeader("content-type", "application/json");
      response.end('{"state":"ready"}');
    });
    const http = new NodeBoundedJsonHttp();

    await expect(
      http.request({
        authorization: "Bearer fixture-token",
        caPem: "unused-for-http",
        method: "GET",
        signal: new AbortController().signal,
        url,
      }),
    ).resolves.toEqual({ body: { state: "ready" }, status: 200 });
    expect(observedAuthorization).toBe("Bearer fixture-token");
  });

  it("rejects redirects without forwarding the authorization value", async () => {
    let redirectedRequests = 0;
    const destination = await endpoint((_request, response) => {
      redirectedRequests += 1;
      response.end("{}");
    });
    const source = await endpoint((_request, response) => {
      response.statusCode = 302;
      response.setHeader("location", destination);
      response.end();
    });
    const http = new NodeBoundedJsonHttp();

    await expect(
      http.request({
        authorization: "Bearer must-not-forward",
        caPem: "unused-for-http",
        method: "GET",
        signal: new AbortController().signal,
        url: source,
      }),
    ).rejects.toThrow("redirects are not allowed");
    expect(redirectedRequests).toBe(0);
  });

  it("rejects oversized requests and responses", async () => {
    const url = await endpoint((_request, response) => {
      response.end(JSON.stringify({ value: "response-too-large" }));
    });
    const http = new NodeBoundedJsonHttp({
      maximumRequestBytes: 8,
      maximumResponseBytes: 8,
    });

    await expect(
      http.request({
        body: { value: "request-too-large" },
        caPem: "unused-for-http",
        method: "POST",
        signal: new AbortController().signal,
        url,
      }),
    ).rejects.toThrow("request exceeded its byte limit");
    await expect(
      http.request({
        caPem: "unused-for-http",
        method: "GET",
        signal: new AbortController().signal,
        url,
      }),
    ).rejects.toThrow("response exceeded its byte limit");
  });

  it("cancels an incomplete request through the caller signal", async () => {
    const url = await endpoint(() => undefined);
    const controller = new AbortController();
    const http = new NodeBoundedJsonHttp({ timeoutMs: 5_000 });

    const request = http.request({
      caPem: "unused-for-http",
      method: "GET",
      signal: controller.signal,
      url,
    });
    controller.abort();

    await expect(request).rejects.toMatchObject({ name: "AbortError" });
  });
});
