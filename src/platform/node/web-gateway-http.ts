import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

import { GatewayProblem } from "./web-gateway-errors";

export function header(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return typeof value === "string" ? value : undefined;
}

export function matches(actual: string | undefined, expected: string): boolean {
  if (actual === undefined) return false;
  const a = Buffer.from(actual);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

export function cookie(request: IncomingMessage, name: string): string | undefined {
  const entries = header(request, "cookie")
    ?.split(";")
    .map((value) => value.trim());
  const selected = entries?.filter((value) => value.startsWith(`${name}=`));
  return selected?.length === 1 ? selected[0]?.slice(name.length + 1) : undefined;
}

export function securityHeaders(response: ServerResponse): void {
  response.setHeader("Cache-Control", "no-store");
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.setHeader("Referrer-Policy", "no-referrer");
  response.setHeader("X-Frame-Options", "DENY");
  response.setHeader(
    "Content-Security-Policy",
    "default-src 'self'; connect-src 'self'; font-src 'self' data:; img-src 'self' data:; script-src 'self'; style-src 'self' 'unsafe-inline'; worker-src 'self' blob:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
  );
}

export function json(response: ServerResponse, status: number, value: unknown): void {
  if (response.destroyed || response.writableEnded) return;
  response.statusCode = status;
  response.setHeader("Content-Type", "application/json; charset=utf-8");
  response.end(JSON.stringify(value));
}

export function problem(
  response: ServerResponse,
  status: number,
  code: string,
  summary: string,
  recovery?: string,
): void {
  json(response, status, {
    error: { code, summary, ...(recovery === undefined ? {} : { recovery }) },
  });
}

async function body(request: IncomingMessage, limit: number): Promise<Buffer> {
  const length = header(request, "content-length");
  if (length !== undefined && (!/^\d+$/u.test(length) || Number(length) > limit)) {
    request.resume();
    throw new GatewayProblem(413, "BODY_TOO_LARGE", "Request exceeds its byte limit.");
  }
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const value of request) {
    const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value as Uint8Array);
    bytes += chunk.length;
    if (bytes > limit) {
      request.resume();
      throw new GatewayProblem(413, "BODY_TOO_LARGE", "Request exceeds its byte limit.");
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

export async function readJson(request: IncomingMessage, limit: number): Promise<unknown> {
  if (!header(request, "content-type")?.toLowerCase().startsWith("application/json")) {
    throw new GatewayProblem(415, "UNSUPPORTED_MEDIA_TYPE", "Use application/json.");
  }
  try {
    return JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(await body(request, limit)),
    ) as unknown;
  } catch (error) {
    if (error instanceof GatewayProblem) throw error;
    throw new GatewayProblem(400, "INVALID_REQUEST", "Request must contain valid JSON.");
  }
}
