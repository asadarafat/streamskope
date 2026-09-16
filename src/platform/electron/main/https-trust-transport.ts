import type { ClientRequest, IncomingMessage } from "node:http";
import { Agent, request } from "node:https";

import { HTTPS_TRUST_LIMITS as limits } from "../../../features/kafka/contracts/https-trust-types";
import {
  parseHttpsUrl,
  validateHttpsHeader,
} from "../../../features/kafka/contracts/https-trust-validation";
import {
  HttpsTrustTransportError,
  type HttpsTrustFailureCategory,
  type HttpsTrustPort,
  type HttpsTrustRequest,
} from "../../../features/kafka/application/https-trust-port";
import { canonicalCertificates } from "../../../features/kafka/engine/trust-material-shared";

function configurationError(): never {
  throw new HttpsTrustTransportError("configuration", "");
}

function validate(input: HttpsTrustRequest): {
  url: URL;
  headers: Record<string, string>;
  ca: string | undefined;
} {
  let url: URL;
  try {
    url = parseHttpsUrl(input.url, "request.url");
  } catch {
    configurationError();
  }
  if (
    !Number.isSafeInteger(input.maximumBytes) ||
    input.maximumBytes < 1 ||
    input.maximumBytes > limits.jsonWireBytes ||
    input.headers.length > limits.entries
  )
    configurationError();
  const headers: Record<string, string> = { "accept-encoding": "identity" };
  for (const field of input.headers) {
    const name = field.name.toLowerCase();
    try {
      validateHttpsHeader(field.name, field.value, "request.headers");
    } catch {
      configurationError();
    }
    if (Object.hasOwn(headers, name)) configurationError();
    Object.defineProperty(headers, name, { value: field.value, enumerable: true });
  }
  const auth = input.authentication;
  if (auth.mode === "bearer") {
    if (!/^[A-Za-z0-9._~+/-]+=*$/u.test(auth.token) || auth.token.length > limits.valueCharacters)
      configurationError();
    headers.authorization = `Bearer ${auth.token}`;
  } else if (auth.mode === "basic") {
    if (
      !auth.username ||
      auth.username.includes(":") ||
      auth.username.length > limits.valueCharacters ||
      auth.password.length > limits.valueCharacters ||
      /\p{Cc}/u.test(auth.username + auth.password)
    )
      configurationError();
    headers.authorization = `Basic ${Buffer.from(`${auth.username}:${auth.password}`, "utf8").toString("base64")}`;
  } else if (auth.mode !== "none") configurationError();
  let ca: string | undefined;
  if (input.tls.mode === "custom") {
    if (Buffer.byteLength(input.tls.caPem, "utf8") > limits.materialBytes) configurationError();
    try {
      ca = canonicalCertificates([input.tls.caPem]);
    } catch {
      configurationError();
    }
  } else if (input.tls.mode !== "system") configurationError();
  return { url, headers, ca };
}

function category(error: unknown, signal: AbortSignal): HttpsTrustFailureCategory {
  if (signal.aborted)
    return signal.reason instanceof Error && signal.reason.name === "TimeoutError"
      ? "timeout"
      : "cancelled";
  const code =
    typeof error === "object" && error !== null && "code" in error && typeof error.code === "string"
      ? error.code
      : "";
  if (/CERT|TLS|SSL|SELF_SIGNED|UNABLE_TO_VERIFY|UNABLE_TO_GET_ISSUER/u.test(code)) return "tls";
  return "network";
}

/** One verified GET; recipe resolution and the shared deadline remain application-owned. */
export class NodeHttpsTrustTransport implements HttpsTrustPort {
  async get(input: HttpsTrustRequest): Promise<Uint8Array> {
    const { url, headers, ca } = validate(input);
    if (input.signal.aborted)
      throw new HttpsTrustTransportError(category(undefined, input.signal), url.origin);
    const agent = new Agent({
      keepAlive: false,
      maxSockets: 1,
      maxCachedSessions: 0,
      rejectUnauthorized: true,
      ca,
    });
    return new Promise<Uint8Array>((resolve, reject) => {
      let outgoing: ClientRequest | undefined;
      let incoming: IncomingMessage | undefined;
      let settled = false;
      const chunks: Buffer[] = [];
      let received = 0;
      const finish = (failure?: HttpsTrustFailureCategory): void => {
        if (settled) return;
        settled = true;
        input.signal.removeEventListener("abort", abort);
        let result: Uint8Array | undefined;
        if (failure === undefined) result = Buffer.concat(chunks, received);
        chunks.length = 0;
        incoming?.destroy();
        outgoing?.destroy();
        agent.destroy();
        if (failure !== undefined) reject(new HttpsTrustTransportError(failure, url.origin));
        else if (result !== undefined) resolve(result);
      };
      const abort = (): void => finish(category(undefined, input.signal));
      input.signal.addEventListener("abort", abort, { once: true });
      try {
        outgoing = request(
          url,
          { agent, method: "GET", headers, ca, rejectUnauthorized: true, maxHeaderSize: 16384 },
          (response) => {
            incoming = response;
            response.on("error", () => finish("network"));
            response.on("aborted", () => finish("network"));
            const status = response.statusCode ?? 0;
            if (status !== 200) {
              finish(
                status === 401
                  ? "authentication"
                  : status === 403
                    ? "authorization"
                    : status >= 300 && status < 400
                      ? "redirect"
                      : "status",
              );
              return;
            }
            const encoding = response.headers["content-encoding"];
            if (encoding !== undefined && encoding.toLowerCase() !== "identity") {
              finish("encoding");
              return;
            }
            const length = response.headers["content-length"];
            if (
              length !== undefined &&
              (!/^[0-9]+$/u.test(length) ||
                !Number.isSafeInteger(Number(length)) ||
                Number(length) > input.maximumBytes)
            ) {
              finish("bounds");
              return;
            }
            response.on("data", (chunk: Buffer) => {
              if (settled) return;
              received += chunk.length;
              if (received > input.maximumBytes) {
                finish("bounds");
                return;
              }
              chunks.push(chunk);
            });
            response.on("end", () =>
              finish(
                !response.complete || (length !== undefined && received !== Number(length))
                  ? "network"
                  : undefined,
              ),
            );
          },
        );
        outgoing.on("error", (error) => finish(category(error, input.signal)));
        if (input.signal.aborted) abort();
        else outgoing.end();
      } catch (error) {
        finish(category(error, input.signal));
      }
    });
  }
}
