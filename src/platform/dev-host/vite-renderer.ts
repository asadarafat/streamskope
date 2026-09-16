import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { resolve } from "node:path";

import { createServer, type Plugin } from "vite";

import {
  BROWSER_DEVELOPMENT_GATEWAY_PATH,
  browserDevelopmentSessionCookie,
} from "../../features/kafka/contracts";

import {
  developmentOrigin,
  resolveDevelopmentNetwork,
  type DevelopmentNetworkOptions,
} from "./network";

export interface ViteRendererOptions extends DevelopmentNetworkOptions {
  readonly gatewayToken: string;
  readonly hostOrigin: string;
  readonly hostToken: string;
  readonly port: number;
  readonly root: string;
}

export interface RunningViteRenderer {
  readonly origin: string;
  close(): Promise<void>;
}

export class ViteRendererStartupError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ViteRendererStartupError";
  }
}

function header(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return typeof value === "string" ? value : undefined;
}

function cookieValue(request: IncomingMessage, name: string): string | undefined {
  return header(request, "cookie")
    ?.split(";")
    .map((part) => part.trim())
    .find((part) => part.startsWith(`${name}=`))
    ?.slice(name.length + 1);
}

function tokensMatch(actual: string | undefined, expected: string): boolean {
  if (actual === undefined) {
    return false;
  }
  const actualBytes = Buffer.from(actual);
  const expectedBytes = Buffer.from(expected);
  return actualBytes.length === expectedBytes.length && timingSafeEqual(actualBytes, expectedBytes);
}

function gatewayPath(url: string | undefined): boolean {
  if (url === undefined) {
    return false;
  }
  const pathname = new URL(url, "http://127.0.0.1").pathname;
  return (
    pathname === BROWSER_DEVELOPMENT_GATEWAY_PATH ||
    pathname.startsWith(`${BROWSER_DEVELOPMENT_GATEWAY_PATH}/`)
  );
}

function browserRequestMatchesRenderer(request: IncomingMessage, rendererOrigin: string): boolean {
  const origin = header(request, "origin");
  const fetchSite = header(request, "sec-fetch-site");
  return (
    (origin === undefined || origin === rendererOrigin) &&
    (fetchSite === undefined || fetchSite === "same-origin")
  );
}

function rendererDocumentRequest(request: IncomingMessage): boolean {
  if (request.method !== "GET") {
    return false;
  }
  const pathname = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
  return pathname === "/" || pathname === "/index.html";
}

function sendGatewayProblem(response: ServerResponse): void {
  response.statusCode = 403;
  response.setHeader("Cache-Control", "no-store");
  response.setHeader("Content-Type", "application/json; charset=utf-8");
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.end(
    JSON.stringify({
      error: {
        code: "FORBIDDEN",
        summary: "Development gateway request is not authorized.",
      },
    }),
  );
}

function developmentGateway(options: {
  readonly gatewayToken: string;
  readonly rendererOrigin: string;
}): Plugin {
  const cookieName = browserDevelopmentSessionCookie(options.rendererOrigin);
  const canonical = new URL(options.rendererOrigin);
  return {
    configureServer(server): void {
      server.middlewares.use((request, response, next) => {
        if (rendererDocumentRequest(request)) {
          const requestHost = header(request, "host");
          const loopbackAliases = ["localhost", "127.0.0.1", "[::1]"];
          if (
            requestHost !== canonical.host &&
            loopbackAliases.some((hostname) => requestHost === `${hostname}:${canonical.port}`)
          ) {
            response.statusCode = 307;
            response.setHeader("Location", `${options.rendererOrigin}/`);
            response.setHeader("Cache-Control", "no-store");
            response.end();
            return;
          }
          response.setHeader(
            "Set-Cookie",
            `${cookieName}=${options.gatewayToken}; HttpOnly; SameSite=Strict; Path=${BROWSER_DEVELOPMENT_GATEWAY_PATH}`,
          );
        }
        if (!gatewayPath(request.url)) {
          next();
          return;
        }
        if (
          !tokensMatch(cookieValue(request, cookieName), options.gatewayToken) ||
          !browserRequestMatchesRenderer(request, options.rendererOrigin)
        ) {
          sendGatewayProblem(response);
          return;
        }
        next();
      });
    },
    name: "streamskope-development-gateway-session",
  };
}

function configuredHostCsp(publicHostname: string): Plugin {
  return {
    name: "streamskope-configured-development-csp",
    transformIndexHtml: {
      handler(html: string): string {
        return html.replace(
          /connect-src [^;]*;/u,
          `connect-src http://${publicHostname}:* ws://${publicHostname}:*;`,
        );
      },
      order: "post" as const,
    },
  };
}

function validatePort(port: number): void {
  if (!Number.isSafeInteger(port) || port <= 0 || port > 65_535) {
    throw new ViteRendererStartupError("Renderer port must be between 1 and 65535.");
  }
}

export async function startViteRenderer(
  options: ViteRendererOptions,
): Promise<RunningViteRenderer> {
  const network = resolveDevelopmentNetwork(options);
  validatePort(options.port);
  const origin = developmentOrigin(network.publicHostname, options.port);
  const server = await createServer({
    appType: "spa",
    clearScreen: false,
    configFile: resolve(process.cwd(), "config/vite.config.ts"),
    logLevel: "silent",
    plugins: [
      developmentGateway({ gatewayToken: options.gatewayToken, rendererOrigin: origin }),
      configuredHostCsp(network.publicHostname),
    ],
    root: options.root,
    server: {
      allowedHosts: [network.publicHostname],
      host: network.listenHostname,
      port: options.port,
      proxy: {
        [BROWSER_DEVELOPMENT_GATEWAY_PATH]: {
          changeOrigin: false,
          configure(proxy): void {
            proxy.on("proxyReq", (proxyRequest) => {
              proxyRequest.removeHeader("cookie");
              proxyRequest.setHeader("origin", origin);
              proxyRequest.setHeader("x-streamskope-token", options.hostToken);
            });
          },
          rewrite(path): string {
            const rewritten = path.slice(BROWSER_DEVELOPMENT_GATEWAY_PATH.length);
            return rewritten.length === 0 ? "/" : rewritten;
          },
          target: options.hostOrigin,
          ws: false,
        },
      },
      strictPort: true,
    },
  });
  try {
    await server.listen();
  } catch (error) {
    try {
      await server.close();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        `Renderer endpoint ${origin} is unavailable and could not be stopped.`,
        { cause: cleanupError },
      );
    }
    throw new ViteRendererStartupError(`Renderer endpoint ${origin} is unavailable.`, {
      cause: error,
    });
  }

  let closePromise: Promise<void> | undefined;
  return {
    close: (): Promise<void> => {
      closePromise ??= server.close();
      return closePromise;
    },
    origin,
  };
}
