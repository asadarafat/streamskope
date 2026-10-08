import { lstat, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { createServer, request } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { ProviderHostRegistry } from "../../src/platform/node/provider-host";
import {
  startWebGateway,
  type RunningWebGateway,
  type WebGatewayRuntime,
} from "../../src/platform/node/web-gateway";
import type { PluginRendererAsset } from "../../src/platform/node/plugins/runtime";
import { PassphraseVaultError } from "../../src/platform/node/vault/passphrase-vault";
import { WebGatewayCleanupUnconfirmedError } from "../../src/platform/node/web-gateway-errors";
import { createProviderFixture, type ProviderFixture } from "../support/provider-fixture";
import {
  OperationalDiagnosticError,
  type OperationalDiagnostic,
} from "../../src/platform/diagnostics";

const PASSPHRASE = "gateway independent test passphrase";
const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function fixture(
  options: {
    lifetime?: number;
    cleanupFails?: boolean;
    subscribeFails?: boolean;
    openDelay?: Promise<void>;
    openError?: Error;
    diagnosticSinkFails?: boolean;
  } = {},
): Promise<{
  gateway: RunningWebGateway;
  dataRoot: string;
  rendererRoot: string;
  opens: { mode: string; passphrase: string }[];
  providers: ProviderFixture[];
  keyLocks: number[];
  diagnostics: OperationalDiagnostic[];
  create: () => Promise<Response>;
}> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-web-gateway-"));
  cleanups.push(() => rm(root, { recursive: true, force: true }));
  const rendererRoot = join(root, "renderer");
  const dataRoot = join(root, "data");
  await mkdir(rendererRoot);
  await mkdir(join(rendererRoot, "assets"));
  await writeFile(
    join(rendererRoot, "index.html"),
    '<html><head><meta http-equiv="Content-Security-Policy" content="default-src \'self\'; connect-src http://127.0.0.1:*;"></head><body><div id="root">built application</div></body></html>',
  );
  await writeFile(join(rendererRoot, "assets", "app.js"), "window.builtApplication=true;");
  const opens: { mode: string; passphrase: string }[] = [];
  const providers: ProviderFixture[] = [];
  const keyLocks: number[] = [];
  const diagnostics: OperationalDiagnostic[] = [];
  let exists = false;
  const gateway = await startWebGateway({
    port: 0,
    hostname: "127.0.0.1",
    publicOrigin: "http://127.0.0.1:0",
    rendererRoot,
    dataRoot,
    onDiagnostic: (diagnostic): void => {
      diagnostics.push(diagnostic);
      if (options.diagnosticSinkFails) throw new Error("private logger failure");
    },
    ...(options.lifetime === undefined ? {} : { sessionLifetimeMs: options.lifetime }),
    inspectVault: () => Promise.resolve(exists ? "present" : "missing"),
    openRuntime: async (passphrase, mode): Promise<WebGatewayRuntime> => {
      opens.push({ mode, passphrase });
      await options.openDelay;
      if (options.openError !== undefined) throw options.openError;
      if (passphrase !== PASSPHRASE) throw new PassphraseVaultError("unlock-failed");
      exists = true;
      const provider = createProviderFixture({ id: "alpha", version: 7 });
      if (options.cleanupFails)
        provider.shutdownOperation = (): Promise<void> =>
          Promise.reject(new Error("private cleanup secret"));
      if (options.subscribeFails)
        provider.subscribeFailure = new Error("private subscription secret");
      providers.push(provider);
      return {
        providers: new ProviderHostRegistry([provider.endpoint]),
        pluginAsset: (path): Promise<PluginRendererAsset | undefined> =>
          Promise.resolve(
            path === "/plugins/example/renderer.js"
              ? {
                  content: Buffer.from("window.verifiedPlugin=true;"),
                  contentType: "text/javascript",
                }
              : undefined,
          ),
        lock: (): Promise<void> => {
          keyLocks.push(provider.shutdownCalls);
          return Promise.resolve();
        },
      };
    },
  });
  cleanups.push(async () => {
    if (options.cleanupFails || options.openError instanceof WebGatewayCleanupUnconfirmedError)
      await expect(gateway.close()).rejects.toThrow("Gateway cleanup");
    else await gateway.close();
  });
  return {
    gateway,
    dataRoot,
    rendererRoot,
    opens,
    providers,
    keyLocks,
    diagnostics,
    create: async () =>
      post(gateway, "/__streamskope_session/create", {
        setupCode: (await readFile(gateway.setupCodePath!, "utf8")).trim(),
        passphrase: PASSPHRASE,
      }),
  };
}

function post(
  gateway: RunningWebGateway,
  path: string,
  value: unknown,
  cookie?: string,
): Promise<Response> {
  return fetch(gateway.origin + path, {
    method: "POST",
    headers: {
      origin: gateway.origin,
      "content-type": "application/json",
      ...(cookie === undefined ? {} : { cookie }),
    },
    body: JSON.stringify(value),
  });
}

function session(response: Response): string {
  const cookie = response.headers.get("set-cookie");
  if (cookie === null) throw new Error("Expected an authenticated cookie.");
  return cookie.split(";", 1)[0]!;
}

function requestWithHost(gateway: RunningWebGateway, host: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const outgoing = request(
      { hostname: "127.0.0.1", port: gateway.port, path: "/health", headers: { host } },
      (response) => {
        response.resume();
        resolve(response.statusCode ?? 0);
      },
    );
    outgoing.once("error", reject);
    outgoing.end();
  });
}

async function readChunk(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Promise<ReadableStreamReadResult<Uint8Array>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      reader.read(),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error("Gateway SSE did not settle.")), 3_000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

describe("production browser gateway", () => {
  it("keeps setup authority private and refuses providers, plugins and the built app before create", async () => {
    const f = await fixture();
    const code = (await readFile(f.gateway.setupCodePath!, "utf8")).trim();
    expect((await lstat(f.gateway.setupCodePath!)).mode & 0o777).toBe(0o600);
    expect((await lstat(f.dataRoot)).mode & 0o777).toBe(0o700);
    const page = await fetch(f.gateway.origin + "/__streamskope_session/login");
    expect(page.status).toBe(200);
    expect(page.headers.get("set-cookie")).toBeNull();
    const html = await page.text();
    expect(html).toContain("Setup code");
    expect(html).not.toContain(code);
    expect(html).not.toContain(PASSPHRASE);
    for (const path of ["/", "/assets/app.js", "/plugins/example/renderer.js"]) {
      const result = await fetch(f.gateway.origin + path, { redirect: "manual" });
      expect(result.status).toBe(303);
      expect(result.headers.get("location")).toBe("/__streamskope_session/login");
    }
    expect(
      (await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health")).status,
    ).toBe(401);
    expect(
      (
        await post(f.gateway, "/__streamskope_session/create", {
          setupCode: "wrong",
          passphrase: PASSPHRASE,
        })
      ).status,
    ).toBe(401);
    expect(f.opens).toEqual([]);
  });

  it("requires exact Host and Origin, bounded JSON and a valid create authority", async () => {
    const f = await fixture();
    expect(await requestWithHost(f.gateway, "attacker.test")).toBe(403);
    for (const origin of [undefined, "http://attacker.test"]) {
      const result = await fetch(f.gateway.origin + "/__streamskope_session/create", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(origin === undefined ? {} : { origin }),
        },
        body: "{}",
      });
      expect(result.status).toBe(403);
    }
    const oversize = await post(f.gateway, "/__streamskope_session/create", {
      passphrase: "x".repeat(9_000),
    });
    expect(oversize.status).toBe(413);
    expect(
      (
        await post(f.gateway, "/__streamskope_session/create", {
          setupCode: "wrong",
          passphrase: "short",
        })
      ).status,
    ).toBe(400);
    expect(f.opens).toEqual([]);
  });

  it("creates a session without leaking authority and serves only authenticated static/plugin assets", async () => {
    const f = await fixture();
    const created = await f.create();
    expect(created.status).toBe(200);
    expect(created.headers.get("set-cookie")).toContain("HttpOnly; SameSite=Strict; Path=/");
    const auth = session(created);
    await expect(readFile(f.gateway.setupCodePath!)).rejects.toMatchObject({ code: "ENOENT" });
    const index = await fetch(f.gateway.origin + "/", { headers: { cookie: auth } });
    const html = await index.text();
    expect(html).toContain("built application");
    expect(html).toContain("connect-src 'self'");
    expect(html).toContain("/__streamskope_session/browser-runtime.js");
    expect(html).not.toContain("/__streamskope_session/lock.js");
    expect(html).not.toContain(PASSPHRASE);
    expect(html).not.toContain(auth.split("=")[1]);
    expect(index.headers.get("content-security-policy")).toContain("frame-ancestors 'none'");
    const plugin = await fetch(f.gateway.origin + "/plugins/example/renderer.js", {
      headers: { cookie: auth },
    });
    expect(await plugin.text()).toBe("window.verifiedPlugin=true;");
    const missing = await fetch(f.gateway.origin + "/assets/missing.js", {
      headers: { cookie: auth },
    });
    expect(missing.status).toBe(404);
    expect(
      (await post(f.gateway, "/__streamskope_session/unlock", { passphrase: PASSPHRASE })).status,
    ).toBe(409);
  });

  it("transports an independent provider through actual HTTP and SSE and rejects forged sessions", async () => {
    const f = await fixture();
    const auth = session(await f.create());
    const provider = f.providers[0]!;
    const events = await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/events", {
      headers: { cookie: auth },
    });
    expect(events.status).toBe(200);
    const reader = events.body!.getReader();
    expect(new TextDecoder().decode((await readChunk(reader)).value)).toContain("host ready");
    const command = provider.command("set", "real routed value");
    const reply = await post(
      f.gateway,
      "/__streamskope_host/providers/alpha/commands",
      command,
      auth,
    );
    expect(reply.status).toBe(200);
    await expect(reply.json()).resolves.toEqual(provider.response(command));
    provider.emit(provider.event("event survived proxy", 12));
    expect(new TextDecoder().decode((await readChunk(reader)).value)).toContain(
      "event survived proxy",
    );
    expect(
      (
        await post(
          f.gateway,
          "/__streamskope_host/providers/alpha/commands",
          provider.command(),
          auth + "forged",
        )
      ).status,
    ).toBe(401);
    expect(
      (await post(f.gateway, "/__streamskope_host/providers/missing/commands", command, auth))
        .status,
    ).toBe(404);
    expect(
      (
        await post(
          f.gateway,
          "/__streamskope_host/providers/alpha/commands",
          { ...command, version: 99 },
          auth,
        )
      ).status,
    ).toBe(400);
    expect(provider.requests).toEqual([command]);
    await reader.cancel();
    await expect.poll(() => provider.stopCalls).toBe(1);
  });

  it("rejects cross-site commands and ignores attacker token headers", async () => {
    const f = await fixture();
    const auth = session(await f.create());
    const provider = f.providers[0]!;
    const result = await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/commands", {
      method: "POST",
      headers: {
        cookie: auth,
        origin: "http://attacker.test",
        "content-type": "application/json",
        "x-streamskope-token": "attacker",
      },
      body: JSON.stringify(provider.command()),
    });
    expect(result.status).toBe(403);
    expect(provider.requests).toEqual([]);
    const health = await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health", {
      headers: { cookie: auth, "x-streamskope-token": "attacker" },
    });
    expect(health.status).toBe(200);
    expect(health.headers.has("x-streamskope-token")).toBe(false);
  });

  it("rejects symlink assets and encoded traversal without reading files outside the build", async () => {
    const f = await fixture();
    const auth = session(await f.create());
    const outside = join(f.dataRoot, "private.js");
    await writeFile(outside, "private outside material");
    await symlink(outside, join(f.rendererRoot, "assets", "leak.js"));
    await symlink(f.dataRoot, join(f.rendererRoot, "leak"));
    for (const path of [
      "/assets/leak.js",
      "/leak/private.js",
      "/assets/%2e%2e/private.js",
      "/assets/app.js?path=private",
      "/assets/missing",
    ]) {
      const response = await fetch(f.gateway.origin + path, { headers: { cookie: auth } });
      expect(response.status, path).toBe(404);
      expect(await response.text()).not.toContain("private outside material");
    }
  });

  it("locks after successful provider shutdown, revokes old cookies, and creates a fresh runtime on unlock", async () => {
    const f = await fixture();
    const original = session(await f.create());
    const result = await post(f.gateway, "/__streamskope_session/lock", {}, original);
    expect(result.status).toBe(200);
    expect(result.headers.get("set-cookie")).toContain("Max-Age=0");
    expect(f.providers[0]!.shutdownCalls).toBe(1);
    expect(f.keyLocks).toEqual([1]);
    expect(
      (
        await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health", {
          headers: { cookie: original },
        })
      ).status,
    ).toBe(401);
    const bad = await post(f.gateway, "/__streamskope_session/unlock", {
      passphrase: "incorrect passphrase",
    });
    expect(bad.status).toBe(401);
    expect(await bad.text()).not.toContain("sensitive low-level");
    const resumed = await post(f.gateway, "/__streamskope_session/unlock", {
      passphrase: PASSPHRASE,
    });
    expect(resumed.status).toBe(200);
    expect(session(resumed)).not.toBe(original);
    expect(f.providers).toHaveLength(2);
    expect(
      (
        await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health", {
          headers: { cookie: original },
        })
      ).status,
    ).toBe(401);
    expect(
      (
        await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health", {
          headers: { cookie: session(resumed) },
        })
      ).status,
    ).toBe(200);
  });

  it("does not release the vault key or admit a new owner after failed cleanup", async () => {
    const f = await fixture({ cleanupFails: true });
    const auth = session(await f.create());
    const result = await post(f.gateway, "/__streamskope_session/lock", {}, auth);
    expect(result.status).toBe(503);
    expect(await result.text()).not.toContain("private cleanup secret");
    expect(f.keyLocks).toEqual([]);
    expect(
      (await post(f.gateway, "/__streamskope_session/unlock", { passphrase: PASSPHRASE })).status,
    ).toBe(409);
    expect(
      (
        await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health", {
          headers: { cookie: auth },
        })
      ).status,
    ).toBe(401);
  });

  it("expires an authenticated owner and shuts down its providers before releasing the key", async () => {
    const f = await fixture({ lifetime: 150 });
    const auth = session(await f.create());
    await expect.poll(() => f.keyLocks.length).toBe(1);
    expect(f.keyLocks).toEqual([1]);
    expect(
      (
        await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health", {
          headers: { cookie: auth },
        })
      ).status,
    ).toBe(401);
  });

  it("rejects an old lock body that completes after another session has taken ownership", async () => {
    const f = await fixture();
    const original = session(await f.create());
    let settle = (_status: number): void => undefined;
    let fail = (_error: Error): void => undefined;
    const status = new Promise<number>((resolve, reject) => {
      settle = resolve;
      fail = reject;
    });
    const slow = request(
      {
        hostname: "127.0.0.1",
        port: f.gateway.port,
        path: "/__streamskope_session/lock",
        method: "POST",
        headers: {
          origin: f.gateway.origin,
          cookie: original,
          "content-type": "application/json",
          "content-length": "2",
        },
      },
      (response) => {
        response.resume();
        settle(response.statusCode ?? 0);
      },
    );
    slow.once("error", fail);
    slow.write("{");
    try {
      expect(
        (
          await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health", {
            headers: { cookie: original },
          })
        ).status,
      ).toBe(200);
      expect((await post(f.gateway, "/__streamskope_session/lock", {}, original)).status).toBe(200);
      const renewed = session(
        await post(f.gateway, "/__streamskope_session/unlock", { passphrase: PASSPHRASE }),
      );
      slow.end("}");
      expect(await status).toBe(401);
      expect(
        (
          await fetch(f.gateway.origin + "/__streamskope_host/providers/alpha/health", {
            headers: { cookie: renewed },
          })
        ).status,
      ).toBe(200);
      expect(f.providers[1]!.shutdownCalls).toBe(0);
      expect(f.keyLocks).toEqual([1]);
    } finally {
      slow.destroy();
    }
  });

  it("preserves safe vault diagnostics while masking arbitrary startup errors", async () => {
    const occupied = await fixture({ openError: new PassphraseVaultError("in-use") });
    const result = await occupied.create();
    expect(result.status).toBe(409);
    await expect(result.json()).resolves.toMatchObject({
      error: {
        code: "VAULT_IN_USE",
        recovery: "Stop the other StreamSkope host using this data directory, then retry.",
        diagnostic: { code: "VAULT_IN_USE", owner: "vault", stage: "unlock" },
      },
    });
    const broken = await fixture({ openError: new Error("secret/path/password runtime detail") });
    const failed = await broken.create();
    expect(failed.status).toBe(503);
    expect(await failed.text()).not.toContain("secret/path/password");
  });

  it("fences a composition whose partial startup cleanup is unconfirmed", async () => {
    const f = await fixture({
      openError: new WebGatewayCleanupUnconfirmedError({
        cause: new Error("private partial cleanup"),
      }),
    });
    const result = await f.create();
    expect(result.status).toBe(503);
    expect(await result.text()).not.toContain("private partial cleanup");
    expect((await f.create()).status).toBe(409);
    expect(f.opens).toHaveLength(1);
    expect(f.keyLocks).toEqual([]);
  });

  it("retains a returned runtime when private listener startup cleanup cannot be confirmed", async () => {
    const f = await fixture({ cleanupFails: true, subscribeFails: true });
    const result = await f.create();
    expect(result.status).toBe(503);
    const message = await result.text();
    expect(message).not.toContain("private subscription secret");
    expect(message).not.toContain("private cleanup secret");
    expect(f.providers[0]!.shutdownCalls).toBe(1);
    expect(f.keyLocks).toEqual([]);
    expect((await f.create()).status).toBe(409);
    expect(f.opens).toHaveLength(1);
  });

  it("bounds password/setup attempts before running another expensive unlock", async () => {
    const f = await fixture();
    for (let i = 0; i < 12; i++) {
      expect(
        (
          await post(f.gateway, "/__streamskope_session/create", {
            passphrase: PASSPHRASE,
            setupCode: "incorrect",
          })
        ).status,
      ).toBe(401);
    }
    expect((await f.create()).status).toBe(429);
    expect(f.opens).toEqual([]);
  });

  it("serializes concurrent create attempts and closes the public listener", async () => {
    let finish = (): void => undefined;
    const delay = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const f = await fixture({ openDelay: delay });
    const initial = f.create();
    try {
      await expect.poll(() => f.opens.length).toBe(1);
      expect((await f.create()).status).toBe(409);
    } finally {
      finish();
    }
    expect((await initial).status).toBe(200);
    await f.gateway.close();
    const server = createServer();
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(f.gateway.port, "127.0.0.1", resolve);
    });
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error === undefined ? resolve() : reject(error)));
    });
    expect(f.keyLocks).toEqual([1]);
  });
});

it("returns and records the same host-generated diagnostic without exposing private causes", async () => {
  const f = await fixture({
    openError: new OperationalDiagnosticError("NATS_RUNTIME_START_FAILED", {
      cause: new Error("private-password private-host.example /private/credential-path"),
    }),
    diagnosticSinkFails: true,
  });
  const response = await fetch(f.gateway.origin + "/__streamskope_session/create", {
    method: "POST",
    headers: {
      origin: f.gateway.origin,
      "content-type": "application/json",
      "x-correlation-id": "attacker-selected-reference",
    },
    body: JSON.stringify({
      setupCode: (await readFile(f.gateway.setupCodePath!, "utf8")).trim(),
      passphrase: PASSPHRASE,
    }),
  });
  expect(response.status).toBe(503);
  const result = (await response.json()) as {
    error: { code: string; diagnostic: OperationalDiagnostic };
  };
  expect(result.error.code).toBe("RUNTIME_START_FAILED");
  expect(result.error.diagnostic).toEqual(f.diagnostics[0]);
  expect(result.error.diagnostic).toMatchObject({
    code: "NATS_RUNTIME_START_FAILED",
    owner: "nats",
    stage: "startup",
  });
  expect(JSON.stringify(result)).not.toMatch(/private|attacker|password|credential-path/);
  expect(result.error.diagnostic.correlationId).toMatch(/^[a-f0-9-]{36}$/u);
});

it("identifies a missing required renderer without turning ordinary missing assets into host faults", async () => {
  const f = await fixture();
  const auth = session(await f.create());
  await rm(join(f.rendererRoot, "index.html"));
  const missing = await fetch(f.gateway.origin + "/assets/absent.js", {
    headers: { cookie: auth },
  });
  expect(missing.status).toBe(404);
  expect(f.diagnostics).toHaveLength(0);
  const root = await fetch(f.gateway.origin + "/", { headers: { cookie: auth } });
  expect(root.status).toBe(503);
  const result = (await root.json()) as { error: { diagnostic: OperationalDiagnostic } };
  expect(result.error.diagnostic).toEqual(f.diagnostics[0]);
  expect(result.error.diagnostic.code).toBe("RENDERER_ASSET_UNAVAILABLE");
  expect(JSON.stringify(result)).not.toContain(f.rendererRoot);
});
