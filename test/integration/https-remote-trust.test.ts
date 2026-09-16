import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { NodeHttpsTrustTransport } from "../../src/platform/electron/main/https-trust-transport";
import {
  extractHttpsTrustMaterial,
  extractHttpsTrustPassword,
} from "../../src/platform/electron/main/https-trust-extraction";
import { parsePemTrustMaterial } from "../../src/features/kafka/engine/trust-material-shared";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

describe("verified bounded HTTPS trust transport", () => {
  let fixture: Awaited<ReturnType<typeof createHttpsTrustFixture>>;
  const transport = new NodeHttpsTrustTransport();
  beforeAll(async () => {
    fixture = await createHttpsTrustFixture((request, response) => {
      if (request.url === "/pem") {
        response.end(fixture.caPem);
      } else if (request.url === "/json-pem") {
        response.end(JSON.stringify({ certificate: fixture.caPem }));
      } else if (request.url === "/json-base64") {
        response.end(
          JSON.stringify({ certificate: Buffer.from(fixture.caPem).toString("base64") }),
        );
      } else if (request.url === "/password") {
        response.end('{"password":"\\r\\n keep spaces \\n"}');
      } else if (request.url === "/redirect") {
        response.writeHead(302, { location: "/target" });
        response.end("sensitive-body");
      } else if (request.url === "/unauthorized") {
        response.writeHead(401);
        response.end("sensitive-body");
      } else if (request.url === "/forbidden") {
        response.writeHead(403);
        response.end("sensitive-body");
      } else if (request.url === "/partial") {
        response.writeHead(206);
        response.end("partial");
      } else if (request.url === "/compressed") {
        response.writeHead(200, { "content-encoding": "gzip" });
        response.end("not accepted");
      } else if (request.url === "/overflow") {
        response.writeHead(200);
        response.write(Buffer.alloc(40));
        response.end(Buffer.alloc(40));
      } else if (request.url === "/declared-overflow") {
        response.writeHead(200, { "content-length": "1000" });
        response.end("x");
      } else if (request.url === "/truncated") {
        response.writeHead(200, { "content-length": "10" });
        response.end("x");
      } else if (request.url === "/slow") {
        response.writeHead(200);
        response.flushHeaders();
      } else {
        response.end(Buffer.from([0, 255, 128]));
      }
    });
  });
  afterAll(async () => fixture.close());

  const run = (
    path: string,
    authentication:
      | { mode: "none" }
      | { mode: "bearer"; token: string }
      | { mode: "basic"; username: string; password: string } = { mode: "none" },
  ): Promise<Uint8Array> =>
    transport.get({
      url: fixture.origin + path,
      authentication,
      tls: { mode: "custom", caPem: fixture.caPem },
      headers: [],
      maximumBytes: 64,
      signal: AbortSignal.timeout(2000),
    });

  it("retrieves exact raw bytes with no implicit authentication", async () => {
    expect([...(await run("/material"))]).toEqual([0, 255, 128]);
    expect(fixture.requests.at(-1)).toEqual({ url: "/material", authorization: undefined });
  });
  it("hands actual HTTPS raw/JSON PEM and base64 bytes to the existing decoder", async () => {
    for (const [path, extraction] of [
      ["/pem", { mode: "raw" }],
      ["/json-pem", { mode: "json-pem", pointer: "/certificate" }],
      ["/json-base64", { mode: "json-base64", pointer: "/certificate" }],
    ] as const) {
      const result = await transport.get({
        url: fixture.origin + path,
        authentication: { mode: "none" },
        tls: { mode: "custom", caPem: fixture.caPem },
        headers: [],
        maximumBytes: 8192,
        signal: AbortSignal.timeout(2000),
      });
      const material = Buffer.from(extractHttpsTrustMaterial(result, extraction)).toString("utf8");
      expect(material).toBe(fixture.caPem);
      expect(parsePemTrustMaterial({ kind: "pem", material }).caPem.trim()).toBe(
        fixture.caPem.trim(),
      );
    }
    const password = await run("/password");
    expect(extractHttpsTrustPassword(password, { mode: "json", pointer: "/password" })).toBe(
      " keep spaces ",
    );
  });
  it("uses only the selected Bearer or Basic credential", async () => {
    await run("/material", { mode: "bearer", token: "api-only" });
    expect(fixture.requests.at(-1)?.authorization).toBe("Bearer api-only");
    await run("/material", { mode: "basic", username: "operator", password: "password" });
    expect(fixture.requests.at(-1)?.authorization).toBe("Basic b3BlcmF0b3I6cGFzc3dvcmQ=");
  });
  it.each([
    "redirect",
    "unauthorized",
    "forbidden",
    "partial",
    "compressed",
    "overflow",
    "declared-overflow",
    "truncated",
  ])("rejects %s without another request or response disclosure", async (path) => {
    const before = fixture.requests.length;
    await expect(run(`/${path}`)).rejects.toThrow();
    expect(fixture.requests).toHaveLength(before + 1);
    try {
      await run(`/${path}`);
    } catch (error) {
      expect(String(error)).not.toContain("sensitive-body");
    }
  });
  it("distinguishes authentication and authorization without leaking queries", async () => {
    await expect(run("/unauthorized")).rejects.toMatchObject({
      category: "authentication",
      origin: fixture.origin,
    });
    await expect(run("/forbidden")).rejects.toMatchObject({
      category: "authorization",
      origin: fixture.origin,
    });
  });
  it("rejects untrusted TLS before HTTP credentials are received", async () => {
    const before = fixture.requests.length;
    await expect(
      transport.get({
        url: fixture.origin + "/material?secret=hidden",
        authentication: { mode: "bearer", token: "sensitive-token" },
        tls: { mode: "system" },
        headers: [],
        maximumBytes: 64,
        signal: AbortSignal.timeout(2000),
      }),
    ).rejects.toMatchObject({ category: "tls", origin: fixture.origin });
    expect(fixture.requests).toHaveLength(before);
  });
  it("rejects unsafe URLs and header overrides before opening a socket", async () => {
    const before = fixture.requests.length;
    for (const url of [
      "http://127.0.0.1/test",
      "https://user:secret@127.0.0.1/test",
      fixture.origin + "/#fragment",
    ])
      await expect(
        transport.get({
          url,
          authentication: { mode: "none" },
          tls: { mode: "system" },
          headers: [],
          maximumBytes: 64,
          signal: AbortSignal.timeout(2000),
        }),
      ).rejects.toThrow();
    for (const name of [
      "Authorization",
      "Host",
      "cookie",
      "Connection",
      "Content-Length",
      "Proxy-Authorization",
      "x\r\nInjected",
    ])
      await expect(
        transport.get({
          url: fixture.origin,
          authentication: { mode: "none" },
          tls: { mode: "custom", caPem: fixture.caPem },
          headers: [{ name, value: "invalid" }],
          maximumBytes: 64,
          signal: AbortSignal.timeout(2000),
        }),
      ).rejects.toThrow();
    expect(fixture.requests).toHaveLength(before);
  });
  it.each(["expired", "hostname-mismatch"] as const)(
    "rejects %s TLS before delivering credentials",
    async (certificate) => {
      const server = await createHttpsTrustFixture(
        (_request, response) => response.end("unexpected"),
        certificate,
      );
      try {
        await expect(
          transport.get({
            url: server.origin,
            authentication: { mode: "basic", username: "fixture", password: "not-for-invalid-tls" },
            tls: { mode: "custom", caPem: server.caPem },
            headers: [],
            maximumBytes: 64,
            signal: AbortSignal.timeout(2000),
          }),
        ).rejects.toMatchObject({ category: "tls", origin: server.origin });
        expect(server.requests).toHaveLength(0);
        await expect.poll(() => server.sockets.size).toBe(0);
      } finally {
        await server.close();
      }
    },
  );
  it("never follows a cross-origin redirect or forwards its authorization", async () => {
    const receiver = await createHttpsTrustFixture((_request, response) =>
      response.end("unexpected"),
    );
    const redirector = await createHttpsTrustFixture((_request, response) => {
      response.writeHead(307, { location: receiver.origin + "/collect" });
      response.end();
    });
    try {
      await expect(
        transport.get({
          url: redirector.origin,
          authentication: { mode: "bearer", token: "fixture-secret" },
          tls: { mode: "custom", caPem: redirector.caPem + receiver.caPem },
          headers: [],
          maximumBytes: 64,
          signal: AbortSignal.timeout(2000),
        }),
      ).rejects.toMatchObject({ category: "redirect", origin: redirector.origin });
      expect(redirector.requests).toHaveLength(1);
      expect(receiver.requests).toHaveLength(0);
      await expect.poll(() => redirector.sockets.size).toBe(0);
      expect(receiver.sockets.size).toBe(0);
    } finally {
      await redirector.close();
      await receiver.close();
    }
  });
  it("cancels an in-flight response without automatic retry or surviving sockets", async () => {
    const controller = new AbortController();
    const pending = transport.get({
      url: fixture.origin + "/slow",
      authentication: { mode: "none" },
      tls: { mode: "custom", caPem: fixture.caPem },
      headers: [],
      maximumBytes: 64,
      signal: controller.signal,
    });
    await expect.poll(() => fixture.requests.at(-1)?.url).toBe("/slow");
    controller.abort();
    await expect(pending).rejects.toMatchObject({ category: "cancelled" });
    await expect.poll(() => fixture.sockets.size).toBe(0);
  });
  it("releases request sockets after twenty consecutive cancellations", async () => {
    for (let index = 0; index < 20; index += 1) {
      const before = fixture.requests.length;
      const controller = new AbortController();
      const pending = transport.get({
        url: fixture.origin + "/slow",
        authentication: { mode: "none" },
        tls: { mode: "custom", caPem: fixture.caPem },
        headers: [],
        maximumBytes: 64,
        signal: controller.signal,
      });
      const rejected = expect(pending).rejects.toMatchObject({ category: "cancelled" });
      await expect.poll(() => fixture.requests.length).toBe(before + 1);
      controller.abort();
      await rejected;
      await expect.poll(() => fixture.sockets.size).toBe(0);
      expect(fixture.requests.length).toBe(before + 1);
    }
  });
});
