import { readFile } from "node:fs/promises";
import { extname, isAbsolute, relative, resolve } from "node:path";

import { protocol } from "electron";

import { PACKAGED_RENDERER_HOST, PACKAGED_RENDERER_SCHEME } from "./packaged-renderer-origin";

export { PACKAGED_RENDERER_SCHEME, PACKAGED_RENDERER_URL } from "./packaged-renderer-origin";

const contentSecurityPolicy = [
  "default-src 'self'",
  "base-uri 'none'",
  "connect-src 'none'",
  "font-src 'self' data:",
  "form-action 'none'",
  "frame-ancestors 'none'",
  "img-src 'self' data:",
  "object-src 'none'",
  "script-src 'self'",
  "style-src 'self' 'unsafe-inline'",
  "worker-src 'self' blob:",
].join("; ");

const contentTypes: Readonly<Record<string, string>> = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".jpeg": "image/jpeg",
  ".jpg": "image/jpeg",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".wasm": "application/wasm",
  ".webp": "image/webp",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
};

function notFound(): Response {
  return new Response("Not found", {
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "x-content-type-options": "nosniff",
    },
    status: 404,
  });
}

function rendererPath(rendererRoot: string, request: Request): string | undefined {
  if (request.method !== "GET" || request.url.includes("%")) {
    return undefined;
  }
  let parsed: URL;
  try {
    parsed = new URL(request.url);
  } catch {
    return undefined;
  }
  if (
    parsed.protocol !== `${PACKAGED_RENDERER_SCHEME}:` ||
    parsed.hostname !== PACKAGED_RENDERER_HOST ||
    parsed.port.length > 0 ||
    parsed.username.length > 0 ||
    parsed.password.length > 0 ||
    parsed.search.length > 0
  ) {
    return undefined;
  }
  const pathname = parsed.pathname === "/" ? "/index.html" : parsed.pathname;
  if (!pathname.startsWith("/") || pathname.includes("\\")) {
    return undefined;
  }
  const candidate = resolve(rendererRoot, `.${pathname}`);
  const relativePath = relative(rendererRoot, candidate);
  if (
    relativePath.length === 0 ||
    relativePath === ".." ||
    relativePath.startsWith(`..${process.platform === "win32" ? "\\" : "/"}`) ||
    isAbsolute(relativePath)
  ) {
    return undefined;
  }
  return candidate;
}

export function registerPackagedRendererScheme(): void {
  protocol.registerSchemesAsPrivileged([
    {
      privileges: {
        codeCache: true,
        secure: true,
        standard: true,
        supportFetchAPI: true,
      },
      scheme: PACKAGED_RENDERER_SCHEME,
    },
  ]);
}

export function installPackagedRendererProtocol(rendererRoot: string): void {
  if (!isAbsolute(rendererRoot)) {
    throw new Error("The packaged renderer root must be absolute.");
  }
  if (protocol.isProtocolHandled(PACKAGED_RENDERER_SCHEME)) {
    throw new Error("The packaged renderer protocol already has an owner.");
  }
  protocol.handle(PACKAGED_RENDERER_SCHEME, async (request) => {
    const path = rendererPath(rendererRoot, request);
    if (path === undefined) {
      return notFound();
    }
    try {
      const content = await readFile(path);
      return new Response(content, {
        headers: {
          "cache-control": path.endsWith("index.html")
            ? "no-cache"
            : "public, max-age=31536000, immutable",
          "content-security-policy": contentSecurityPolicy,
          "content-type": contentTypes[extname(path).toLowerCase()] ?? "application/octet-stream",
          "x-content-type-options": "nosniff",
        },
        status: 200,
      });
    } catch {
      return notFound();
    }
  });
}
