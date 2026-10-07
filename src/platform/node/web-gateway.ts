import { randomBytes } from "node:crypto";
import { chmod, lstat, mkdir, open, unlink } from "node:fs/promises";
import {
  createServer,
  type ClientRequest,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { join } from "node:path";

import { browserDevelopmentSessionCookie } from "../providers/browser-development";

import { readBoundedFile } from "./bounded-file";
import type { PluginRendererAsset } from "./plugins/runtime";
import type { ProviderHostRegistry } from "./provider-host";
import { startProviderHttpHost, type RunningDevelopmentHost } from "./provider-http-host";
import { PassphraseVaultError } from "./vault/passphrase-vault";
import { GatewayProblem, WebGatewayCleanupUnconfirmedError } from "./web-gateway-errors";
import {
  cookie,
  header,
  json,
  matches,
  problem,
  readJson,
  securityHeaders,
} from "./web-gateway-http";
import { proxyWebGatewayProvider } from "./web-gateway-proxy";
import {
  prepareWebGatewayIndex,
  readWebGatewayAsset,
  WEB_GATEWAY_LOGIN_SCRIPT,
  webGatewayLoginPage,
} from "./web-gateway-assets";

const SESSION_PATH = "/__streamskope_session";
const PROVIDER_PATH = "/__streamskope_host";
const MAXIMUM_LOGIN_BYTES = 8 * 1_024;
const SESSION_LIFETIME_MS = 2 * 60 * 60 * 1_000;
const MAXIMUM_LOGIN_ATTEMPTS = 12;
const LOGIN_WINDOW_MS = 60_000;

export interface WebGatewayRuntime {
  readonly providers: ProviderHostRegistry;
  readonly pluginAsset: (pathname: string) => Promise<PluginRendererAsset | undefined>;
  /** Called only after provider shutdown has completed successfully. */
  readonly lock: () => Promise<void>;
  readonly handleAuthorizedRequest?: (
    request: IncomingMessage,
    response: ServerResponse,
  ) => Promise<boolean>;
  readonly discardPluginFile?: (commandId: string) => void;
}

export interface WebGatewayOptions {
  readonly port: number;
  readonly hostname: string;
  readonly publicOrigin: string;
  readonly rendererRoot: string;
  readonly dataRoot: string;
  readonly inspectVault: () => Promise<"missing" | "present">;
  readonly openRuntime: (
    passphrase: string,
    mode: "create" | "unlock",
  ) => Promise<WebGatewayRuntime>;
  readonly sessionLifetimeMs?: number;
}

export interface RunningWebGateway {
  readonly origin: string;
  readonly port: number;
  readonly setupCodePath: string | undefined;
  close(): Promise<void>;
}

type GatewayState = "locked" | "opening" | "unlocked" | "closing" | "cleanup-failed";

interface UnlockedRuntime {
  readonly runtime: WebGatewayRuntime;
  readonly host: RunningDevelopmentHost;
  readonly privateToken: string;
  readonly internalOrigin: string;
  readonly session: string;
  readonly expiresAt: number;
}

async function ensureSetupCode(dataRoot: string): Promise<{ path: string; code: string }> {
  await mkdir(dataRoot, { mode: 0o700, recursive: true });
  const directory = await lstat(dataRoot);
  if (!directory.isDirectory() || directory.isSymbolicLink())
    throw new Error("Gateway data must use a regular private directory.");
  await chmod(dataRoot, 0o700);
  const path = join(dataRoot, "setup-code");
  const code = randomBytes(32).toString("base64url");
  let file;
  try {
    file = await open(path, "wx", 0o600);
    await file.writeFile(`${code}\n`, "utf8");
    await file.sync();
    return { path, code };
  } catch (error) {
    if (
      error === null ||
      typeof error !== "object" ||
      !("code" in error) ||
      error.code !== "EEXIST"
    )
      throw error;
    const existing = (await readBoundedFile(path, 128, { rejectSymlinks: true }))
      .toString("utf8")
      .trim();
    const metadata = await lstat(path);
    if (
      !/^[A-Za-z0-9_-]{43}$/u.test(existing) ||
      (metadata.mode & 0o077) !== 0 ||
      metadata.nlink !== 1 ||
      (process.getuid !== undefined && metadata.uid !== process.getuid())
    )
      throw new Error("Gateway setup code must be a private regular file.", { cause: error });
    return { path, code: existing };
  } finally {
    await file?.close();
  }
}

function configuredOrigin(value: string): URL {
  const origin = new URL(value);
  if (
    origin.origin !== value ||
    !["http:", "https:"].includes(origin.protocol) ||
    origin.username !== "" ||
    origin.password !== "" ||
    origin.hostname.length === 0 ||
    ["0.0.0.0", "[::]"].includes(origin.hostname)
  )
    throw new Error(
      "Gateway public origin must be one exact browser-reachable HTTP or HTTPS origin.",
    );
  return origin;
}

/** One authenticated browser owner controls the same bounded provider hosts as the desktop. */
export async function startWebGateway(options: WebGatewayOptions): Promise<RunningWebGateway> {
  const publicUrl = configuredOrigin(options.publicOrigin);
  const lifetime = options.sessionLifetimeMs ?? SESSION_LIFETIME_MS;
  if (!Number.isSafeInteger(lifetime) || lifetime < 100 || lifetime > SESSION_LIFETIME_MS)
    throw new Error("Gateway session lifetime must be between 100 ms and two hours.");
  let vaultState = await options.inspectVault();
  const setup = vaultState === "missing" ? await ensureSetupCode(options.dataRoot) : undefined;
  let state: GatewayState = "locked";
  let active: UnlockedRuntime | undefined;
  let shuttingDown = false;
  let pendingOpen: Promise<void> | undefined;
  let pendingLock: Promise<void> | undefined;
  let expires: ReturnType<typeof setTimeout> | undefined;
  let closePromise: Promise<void> | undefined;
  let origin = publicUrl.origin;
  let cookieName = browserDevelopmentSessionCookie(origin);
  const upstreams = new Set<ClientRequest>();
  // Unconfirmed startup/shutdown retains the key and lease through process exit.
  const retainedRuntimes = new Set<WebGatewayRuntime>();
  let attemptWindow = Date.now();
  let attempts = 0;

  const clearSessionCookie = (response: ServerResponse): void => {
    response.setHeader(
      "Set-Cookie",
      `${cookieName}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0${publicUrl.protocol === "https:" ? "; Secure" : ""}`,
    );
  };
  const stopUpstreams = (): void => {
    for (const outgoing of upstreams) outgoing.destroy();
  };
  const lock = (): Promise<void> => {
    if (pendingLock !== undefined) return pendingLock;
    if (state === "cleanup-failed") return Promise.reject(new WebGatewayCleanupUnconfirmedError());
    clearTimeout(expires);
    state = "closing";
    stopUpstreams();
    const owner = active;
    pendingLock = (async (): Promise<void> => {
      try {
        if (owner !== undefined) {
          await owner.host.close();
          await owner.runtime.lock();
          retainedRuntimes.delete(owner.runtime);
        }
        active = undefined;
        state = "locked";
      } catch (error) {
        // Keep ownership and its unlocked key until process exit if shutdown is unconfirmed.
        state = "cleanup-failed";
        throw error;
      }
    })().finally(() => {
      pendingLock = undefined;
    });
    return pendingLock;
  };
  const authorized = (request: IncomingMessage): boolean =>
    !shuttingDown &&
    state === "unlocked" &&
    active !== undefined &&
    Date.now() < active.expiresAt &&
    matches(cookie(request, cookieName), active.session);

  const handle = async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    securityHeaders(response);
    if (header(request, "host") !== publicUrl.host)
      throw new GatewayProblem(403, "FORBIDDEN", "Gateway host is not authorized.");
    const requestOrigin = header(request, "origin");
    const unsafe = !["GET", "HEAD"].includes(request.method ?? "");
    if (
      (unsafe && requestOrigin !== origin) ||
      (requestOrigin !== undefined && requestOrigin !== origin)
    )
      throw new GatewayProblem(403, "FORBIDDEN", "Request origin is not authorized.");
    const raw = request.url ?? "/";
    if (
      !raw.startsWith("/") ||
      raw.includes("%") ||
      raw.includes("\\") ||
      raw.includes("?") ||
      raw.includes("#") ||
      raw.split("/").some((part) => part === "." || part === "..")
    )
      throw new GatewayProblem(404, "NOT_FOUND", "Route was not found.");
    const pathname = raw;
    if (shuttingDown) throw new GatewayProblem(503, "CLOSING", "StreamSkope is shutting down.");
    if (pathname === "/health" && request.method === "GET") {
      json(response, state === "cleanup-failed" ? 503 : 200, {
        status: state === "unlocked" ? "ready" : "locked",
      });
      return;
    }
    if (pathname === `${SESSION_PATH}/status` && request.method === "GET") {
      json(response, 200, {
        state: authorized(request) ? "unlocked" : "locked",
        setupRequired: vaultState === "missing",
      });
      return;
    }
    if (pathname === `${SESSION_PATH}/login` && request.method === "GET") {
      response.setHeader("Content-Type", "text/html; charset=utf-8");
      response.end(webGatewayLoginPage(vaultState === "missing" ? "create" : "unlock"));
      return;
    }
    if (pathname === `${SESSION_PATH}/login.js` && request.method === "GET") {
      response.setHeader("Content-Type", "text/javascript; charset=utf-8");
      response.end(WEB_GATEWAY_LOGIN_SCRIPT);
      return;
    }
    const mode =
      pathname === `${SESSION_PATH}/create`
        ? "create"
        : pathname === `${SESSION_PATH}/unlock`
          ? "unlock"
          : undefined;
    if (mode !== undefined && request.method === "POST") {
      if (state !== "locked")
        throw new GatewayProblem(
          409,
          "SESSION_BUSY",
          "Lock the active session before unlocking again. Restart the instance if cleanup could not be confirmed.",
        );
      if (Date.now() - attemptWindow >= LOGIN_WINDOW_MS) {
        attemptWindow = Date.now();
        attempts = 0;
      }
      if (++attempts > MAXIMUM_LOGIN_ATTEMPTS)
        throw new GatewayProblem(
          429,
          "TOO_MANY_ATTEMPTS",
          "Wait one minute before another unlock attempt.",
        );
      const input = await readJson(request, MAXIMUM_LOGIN_BYTES);
      if (input === null || typeof input !== "object" || Array.isArray(input))
        throw new GatewayProblem(400, "INVALID_REQUEST", "Supply a vault passphrase.");
      const record = input as Record<string, unknown>;
      const keys = mode === "create" ? ["passphrase", "setupCode"] : ["passphrase"];
      if (
        Object.keys(record).length !== keys.length ||
        keys.some((key) => !Object.hasOwn(record, key)) ||
        typeof record.passphrase !== "string" ||
        Buffer.byteLength(record.passphrase) < 12 ||
        Buffer.byteLength(record.passphrase) > 1024
      )
        throw new GatewayProblem(
          400,
          "INVALID_REQUEST",
          "Use a passphrase between 12 and 1024 UTF-8 bytes.",
        );
      if (
        (mode === "create") !== (vaultState === "missing") ||
        (mode === "create" &&
          (setup === undefined ||
            !matches(
              typeof record.setupCode === "string" ? record.setupCode : undefined,
              setup.code,
            )))
      )
        throw new GatewayProblem(
          401,
          "UNLOCK_FAILED",
          "The vault could not be unlocked. Check your passphrase or setup code.",
        );
      if (state !== "locked" || shuttingDown)
        throw new GatewayProblem(409, "SESSION_BUSY", "Another session operation is in progress.");
      state = "opening";
      const passphrase = record.passphrase;
      pendingOpen = (async (): Promise<void> => {
        let runtime: WebGatewayRuntime | undefined;
        try {
          runtime = await options.openRuntime(passphrase, mode);
          retainedRuntimes.add(runtime);
          const internalOrigin = `http://127.0.0.1:${String((server.address() as { port: number }).port)}`;
          const privateToken = randomBytes(32).toString("base64url");
          const host = await startProviderHttpHost({
            providers: runtime.providers,
            port: 0,
            rendererOrigin: internalOrigin,
            token: privateToken,
          });
          active = {
            runtime,
            host,
            privateToken,
            internalOrigin,
            session: randomBytes(32).toString("base64url"),
            expiresAt: Date.now() + lifetime,
          };
          vaultState = "present";
          state = "unlocked";
          if (mode === "create" && setup !== undefined) await unlink(setup.path);
          if (!shuttingDown) {
            expires = setTimeout(() => {
              void lock().catch(() => undefined);
            }, lifetime);
            expires.unref();
            response.setHeader(
              "Set-Cookie",
              `${cookieName}=${active.session}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${Math.ceil(lifetime / 1_000)}${publicUrl.protocol === "https:" ? "; Secure" : ""}`,
            );
            json(response, 200, { state: "unlocked" });
          }
        } catch (error) {
          if (error instanceof WebGatewayCleanupUnconfirmedError) {
            state = "cleanup-failed";
            throw new GatewayProblem(503, "CLEANUP_UNCONFIRMED", error.message);
          }
          if (active !== undefined) {
            await lock();
          } else if (runtime !== undefined) {
            try {
              await runtime.providers.shutdown();
              await runtime.lock();
              retainedRuntimes.delete(runtime);
            } catch {
              state = "cleanup-failed";
              throw new GatewayProblem(
                503,
                "CLEANUP_UNCONFIRMED",
                "Provider cleanup could not be confirmed. Restart the instance.",
              );
            }
          }
          if ((state as GatewayState) !== "cleanup-failed") {
            state = "locked";
            vaultState = await options.inspectVault();
          }
          if (error instanceof PassphraseVaultError) {
            const status =
              error.code === "unlock-failed"
                ? 401
                : error.code === "invalid-passphrase"
                  ? 400
                  : ["in-use", "already-exists", "not-created", "locked"].includes(error.code)
                    ? 409
                    : 503;
            throw new GatewayProblem(
              status,
              `VAULT_${error.code.toUpperCase().replaceAll("-", "_")}`,
              error.message,
              error.recovery,
            );
          }
          throw new GatewayProblem(
            503,
            "RUNTIME_START_FAILED",
            "The application could not start. Preserve the data directory and check the instance configuration.",
          );
        }
      })();
      try {
        await pendingOpen;
      } finally {
        pendingOpen = undefined;
      }
      return;
    }
    if (!authorized(request)) {
      if (
        request.method === "GET" &&
        !pathname.startsWith(SESSION_PATH) &&
        !pathname.startsWith(PROVIDER_PATH)
      ) {
        response.statusCode = 303;
        response.setHeader("Location", `${SESSION_PATH}/login`);
        response.end();
      } else throw new GatewayProblem(401, "LOCKED", "Unlock StreamSkope first.");
      return;
    }
    if (header(request, "sec-fetch-site") === "cross-site")
      throw new GatewayProblem(403, "FORBIDDEN", "Cross-site requests are not authorized.");
    if (pathname === `${SESSION_PATH}/lock` && request.method === "POST") {
      const lockingOwner = active;
      await readJson(request, MAXIMUM_LOGIN_BYTES);
      if (!authorized(request) || active !== lockingOwner)
        throw new GatewayProblem(401, "LOCKED", "The session has ended.");
      clearSessionCookie(response);
      try {
        await lock();
      } catch {
        throw new GatewayProblem(
          503,
          "CLEANUP_UNCONFIRMED",
          "Provider cleanup could not be confirmed. Restart the instance.",
        );
      }
      json(response, 200, { state: "locked" });
      return;
    }
    const owner = active!;
    if (await owner.runtime.handleAuthorizedRequest?.(request, response)) return;
    if (pathname === `${SESSION_PATH}/browser-runtime.js` && request.method === "GET") {
      response.setHeader("Content-Type", "text/javascript; charset=utf-8");
      response.end(
        `window.streamSkopeBrowserRuntime={pluginFileUpload:${owner.runtime.handleAuthorizedRequest !== undefined},lockVault:async()=>{const response=await fetch('/__streamskope_session/lock',{method:'POST',credentials:'same-origin',headers:{'content-type':'application/json'},body:'{}'});if(!response.ok){let summary='The vault could not be locked.';try{summary=(await response.json()).error?.summary||summary;}catch{}throw new Error(summary);}location.replace('/__streamskope_session/login');}};`,
      );
      return;
    }
    if (pathname.startsWith(`${PROVIDER_PATH}/`)) {
      await proxyWebGatewayProvider({
        request,
        response,
        path: pathname.slice(PROVIDER_PATH.length),
        port: owner.host.port,
        origin: owner.internalOrigin,
        token: owner.privateToken,
        upstreams,
        authorized: () => authorized(request) && active === owner,
        ...(owner.runtime.discardPluginFile === undefined
          ? {}
          : { discardPluginFile: owner.runtime.discardPluginFile }),
      });
      return;
    }
    if (request.method !== "GET" || pathname.startsWith(SESSION_PATH))
      throw new GatewayProblem(404, "NOT_FOUND", "Route was not found.");
    const asset = pathname.startsWith("/plugins/")
      ? await owner.runtime.pluginAsset(pathname)
      : await readWebGatewayAsset(options.rendererRoot, pathname);
    if (asset === undefined) throw new GatewayProblem(404, "NOT_FOUND", "Asset was not found.");
    if (!authorized(request) || active !== owner)
      throw new GatewayProblem(401, "LOCKED", "The session has ended.");
    response.setHeader("Content-Type", asset.contentType);
    response.end(
      pathname === "/" || pathname === "/index.html"
        ? prepareWebGatewayIndex(asset.content)
        : asset.content,
    );
  };

  const server = createServer((request, response) => {
    void handle(request, response).catch((error: unknown) => {
      if (response.headersSent) {
        response.destroy();
        return;
      }
      if (error instanceof GatewayProblem)
        problem(response, error.status, error.code, error.message, error.recovery);
      else problem(response, 500, "HOST_FAILURE", "StreamSkope could not complete the request.");
    });
  });
  server.requestTimeout = 15_000;
  server.headersTimeout = 5_000;
  server.on("clientError", (_error, socket) => socket.destroy());
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port, options.hostname, resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string")
    throw new Error("Gateway did not open a TCP listener.");
  if (options.port === 0 && publicUrl.port === "0") publicUrl.port = String(address.port);
  origin = publicUrl.origin;
  cookieName = browserDevelopmentSessionCookie(origin);
  return {
    origin,
    port: address.port,
    setupCodePath: setup?.path,
    close: (): Promise<void> => {
      closePromise ??= (async (): Promise<void> => {
        shuttingDown = true;
        clearTimeout(expires);
        stopUpstreams();
        const serverClosed = new Promise<void>((resolve, reject) => {
          server.close((error) => (error === undefined ? resolve() : reject(error)));
          server.closeAllConnections();
        });
        await pendingOpen?.catch(() => undefined);
        const results = await Promise.allSettled([serverClosed, lock()]);
        const errors = results
          .filter((result): result is PromiseRejectedResult => result.status === "rejected")
          .map((result) => result.reason as unknown);
        if (errors.length > 0)
          throw new AggregateError(errors, "Gateway cleanup could not be confirmed.");
      })();
      return closePromise;
    },
  };
}
