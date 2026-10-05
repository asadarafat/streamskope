import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  launchWebDevelopment,
  startDevelopmentHost,
  type RunningDevelopmentHost,
} from "../../src/platform/dev-host";
import { ProviderHostRegistry } from "../../src/platform/node/provider-host";
import { createProviderFixture, type ProviderFixture } from "../support/provider-fixture";

const RENDERER_ORIGIN = "http://127.0.0.1:4173";
const TOKEN = "0123456789abcdef0123456789abcdef";
const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function headers(origin = RENDERER_ORIGIN, token = TOKEN): Record<string, string> {
  return { origin, "x-streamskope-token": token };
}

async function hostFor(fixtures: readonly ProviderFixture[]): Promise<RunningDevelopmentHost> {
  const host = await startDevelopmentHost({
    providers: new ProviderHostRegistry(fixtures.map((fixture) => fixture.endpoint)),
    port: 0,
    rendererOrigin: RENDERER_ORIGIN,
    token: TOKEN,
  });
  cleanups.push(() => host.close());
  return host;
}

function post(host: RunningDevelopmentHost, id: string, command: unknown): Promise<Response> {
  return fetch(`${host.origin}/providers/${id}/commands`, {
    body: JSON.stringify(command),
    headers: { ...headers(), "content-type": "application/json" },
    method: "POST",
  });
}

async function readChunk(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Promise<ReadableStreamReadResult<Uint8Array>> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      reader.read(),
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(
          () => reject(new Error("Provider event stream did not settle.")),
          3_000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

async function eventStream(
  host: RunningDevelopmentHost,
  id: string,
): Promise<ReadableStreamDefaultReader<Uint8Array>> {
  const controller = new AbortController();
  cleanups.push(() => {
    controller.abort();
    return Promise.resolve();
  });
  const response = await fetch(`${host.origin}/providers/${id}/events`, {
    headers: headers(),
    signal: controller.signal,
  });
  expect(response.status).toBe(200);
  const reader = response.body!.getReader();
  expect(new TextDecoder().decode((await readChunk(reader)).value)).toContain("host ready");
  return reader;
}

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("Expected a TCP port.");
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error === undefined ? resolve() : reject(error)));
  });
  return address.port;
}

describe("named development provider routes", () => {
  it("routes two distinct protocols without accepting foreign commands or unknown aliases", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 9 });
    const host = await hostFor([alpha, beta]);
    for (const fixture of [alpha, beta]) {
      const health = await fetch(`${host.origin}/providers/${fixture.endpoint.id}/health`, {
        headers: headers(),
      });
      await expect(health.json()).resolves.toEqual({
        protocolVersion: fixture.endpoint.version,
        status: "ready",
      });
      const command = fixture.command("set", `${fixture.endpoint.id}-value`);
      const result = await post(host, fixture.endpoint.id, command);
      expect(result.status).toBe(200);
      await expect(result.json()).resolves.toEqual(fixture.response(command));
      expect(fixture.requests).toEqual([command]);
    }
    expect((await post(host, "alpha", beta.command())).status).toBe(400);
    expect((await post(host, "beta", { ...beta.command(), version: 7 })).status).toBe(400);
    for (const path of ["/commands", "/health", "/providers/missing/commands"]) {
      const result = await fetch(`${host.origin}${path}`, {
        body: JSON.stringify(alpha.command()),
        headers: { ...headers(), "content-type": "application/json" },
        method: "POST",
      });
      expect(result.status).toBe(404);
    }
    expect(alpha.requests).toHaveLength(1);
    expect(beta.requests).toHaveLength(1);
  });

  it("guards every named route with the existing origin and invocation token", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const host = await hostFor([alpha]);
    for (const action of ["health", "events", "commands"]) {
      for (const auth of [
        headers(RENDERER_ORIGIN, "invalid"),
        headers("http://attacker.test", TOKEN),
      ]) {
        const result = await fetch(`${host.origin}/providers/alpha/${action}`, {
          headers: { ...auth, "content-type": "application/json" },
          ...(action === "commands"
            ? { body: JSON.stringify(alpha.command()), method: "POST" }
            : {}),
        });
        expect(result.status).toBe(403);
      }
    }
    expect(alpha.requests).toEqual([]);
  });

  it("rejects a structurally valid response from a different request and masks parser errors", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 7 });
    const host = await hostFor([alpha, beta]);
    alpha.nextResponse = alpha.response(alpha.command("read", "", "other-request"));
    const wrongRequest = await post(host, "alpha", alpha.command());
    expect(wrongRequest.status).toBe(502);
    await expect(wrongRequest.json()).resolves.toEqual({
      error: {
        code: "INVALID_BACKEND_RESPONSE",
        summary: "Backend response did not correlate to the submitted command.",
      },
    });
    alpha.nextResponse = beta.response(beta.command());
    expect((await post(host, "alpha", alpha.command())).status).toBe(502);
    const wrongProvider = await post(host, "alpha", beta.command());
    await expect(wrongProvider.json()).resolves.toEqual({
      error: { code: "INVALID_COMMAND", summary: "Provider command is invalid." },
    });
    expect(beta.requests).toEqual([]);
  });

  it("closes only the malformed provider stream and keeps its sibling operational", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 9 });
    const host = await hostFor([alpha, beta]);
    const alphaReader = await eventStream(host, "alpha");
    const betaReader = await eventStream(host, "beta");
    alpha.emit(beta.event("foreign"));
    expect((await readChunk(alphaReader)).done).toBe(true);
    beta.emit(beta.event("still-live", 27));
    const delivered = await readChunk(betaReader);
    expect(new TextDecoder().decode(delivered.value)).toContain(
      `data: ${JSON.stringify(beta.event("still-live", 27))}`,
    );
    expect((await post(host, "beta", beta.command())).status).toBe(200);
    expect(beta.requests).toHaveLength(1);
  });

  it("cleans up all owners when a later provider fails to subscribe before listening", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 9 });
    beta.subscribeFailure = new Error("fixture subscription failed");
    await expect(
      startDevelopmentHost({
        providers: new ProviderHostRegistry([alpha.endpoint, beta.endpoint]),
        port: 0,
        rendererOrigin: RENDERER_ORIGIN,
        token: TOKEN,
      }),
    ).rejects.toThrow("fixture subscription failed");
    expect(alpha.listenerCount()).toBe(0);
    expect(beta.listenerCount()).toBe(0);
    expect(alpha.shutdownCalls).toBe(1);
    expect(beta.shutdownCalls).toBe(1);
  });

  it("publishes one close barrier, closes admission immediately and waits for every owner", async () => {
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 9 });
    const providers = new ProviderHostRegistry([alpha.endpoint, beta.endpoint]);
    let finishBeta = (): void => undefined;
    beta.shutdownOperation = (): Promise<void> =>
      new Promise((resolve) => {
        finishBeta = resolve;
      });
    alpha.shutdownOperation = (): Promise<void> => {
      throw new Error("private owner failure");
    };
    const host = await startDevelopmentHost({
      providers,
      port: 0,
      rendererOrigin: RENDERER_ORIGIN,
      token: TOKEN,
    });
    cleanups.push(() => host.close().catch(() => undefined));
    const closing = host.close();
    expect(host.close()).toBe(closing);
    await expect(providers.get("beta")!.dispatch(beta.command())).rejects.toThrow("shutting down");
    await expect.poll(() => beta.shutdownCalls).toBe(1);
    let settled = false;
    const observed = closing.catch(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(alpha.shutdownCalls).toBe(1);
    expect(alpha.listenerCount()).toBe(0);
    finishBeta();
    await expect(closing).rejects.toThrow("Development host did not stop cleanly.");
    await observed;
  });

  it("forwards named routes through the protected gateway and retains explicit plugin assets", async () => {
    const root = await mkdtemp(join(tmpdir(), "streamskope-provider-renderer-"));
    cleanups.push(() => rm(root, { force: true, recursive: true }));
    await writeFile(join(root, "index.html"), "<main>Provider routing test</main>");
    const alpha = createProviderFixture({ id: "alpha", version: 7 });
    const beta = createProviderFixture({ id: "beta", version: 9 });
    const launch = await launchWebDevelopment({
      providers: new ProviderHostRegistry([alpha.endpoint, beta.endpoint]),
      hostPort: await freePort(),
      rendererPort: await freePort(),
      rendererRoot: root,
      token: TOKEN,
      pluginAsset: (pathname) =>
        Promise.resolve(
          pathname === "/plugins/fixture.js"
            ? {
                content: new TextEncoder().encode("export const installed = true;"),
                contentType: "application/javascript",
              }
            : undefined,
        ),
    });
    cleanups.push(() => launch.close());
    const document = await fetch(launch.browserUrl);
    const cookie = document.headers.get("set-cookie")?.split(";", 1)[0] ?? "";
    expect(cookie).not.toBe("");
    const route = `${launch.rendererOrigin}/__streamskope_host/providers/alpha/commands`;
    for (const auth of [
      {},
      { cookie: "streamskope_dev_session_1=foreign" },
      {
        cookie,
        origin: "http://attacker.test",
        "sec-fetch-site": "cross-site",
      },
    ]) {
      const rejected = await fetch(route, {
        body: JSON.stringify(alpha.command()),
        headers: { ...auth, "content-type": "application/json" },
        method: "POST",
      });
      expect(rejected.status).toBe(403);
    }
    expect(alpha.requests).toEqual([]);
    const command = alpha.command("set", "gateway-value");
    const result = await fetch(route, {
      body: JSON.stringify(command),
      headers: { cookie, "content-type": "application/json" },
      method: "POST",
    });
    expect(result.status).toBe(200);
    await expect(result.json()).resolves.toEqual(alpha.response(command));
    const health = await fetch(
      `${launch.rendererOrigin}/__streamskope_host/providers/beta/health`,
      {
        headers: { cookie },
      },
    );
    await expect(health.json()).resolves.toEqual({ protocolVersion: 9, status: "ready" });
    expect((await fetch(`${launch.rendererOrigin}/plugins/fixture.js`)).status).toBe(200);
    expect(
      (
        await fetch(`${launch.rendererOrigin}/plugins/fixture.js`, {
          headers: { origin: "http://attacker.test" },
        })
      ).status,
    ).toBe(404);
    expect(
      (
        await fetch(`${launch.host.origin}/providers/alpha/health`, {
          headers: { origin: launch.rendererOrigin },
        })
      ).status,
    ).toBe(403);
    expect(beta.requests).toEqual([]);
  });
});
