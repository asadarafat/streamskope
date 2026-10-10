import { request as requestHttp, type ClientRequest, type RequestOptions } from "node:http";
import { request as requestHttps } from "node:https";
import type { Socket } from "node:net";

import type { ConnectionClientIdentity } from "../contracts";

import { tlsClientIdentityOptions } from "./tls-client-identity";

export interface OwnedHttpRequestInput {
  readonly url: URL;
  readonly method: "DELETE" | "GET" | "PATCH" | "POST" | "PUT";
  readonly signal: AbortSignal;
  readonly encodedBody?: string;
  readonly authorization?: string;
  readonly contentType?: string;
  readonly caPem?: string;
  readonly clientIdentity?: ConnectionClientIdentity;
  readonly lookup?: RequestOptions["lookup"];
  readonly responseMode: "json" | "text" | "status";
  readonly maximumResponseBytes: number;
}
export interface OwnedHttpResponse {
  readonly status: number;
  readonly body: unknown;
}
/** A reply never establishes local resource cleanup. Only actual close events do. */
export interface OwnedHttpRequest {
  readonly response: Promise<OwnedHttpResponse>;
  readonly closed: Promise<void>;
  dispatched(): boolean;
  close(): Promise<void>;
}
export class OwnedHttpRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OwnedHttpRequestError";
  }
}

/** One dedicated socket per request; no other owner can borrow or close it. */
export function openOwnedHttpRequest(input: OwnedHttpRequestInput): OwnedHttpRequest {
  if (!["http:", "https:"].includes(input.url.protocol))
    throw new OwnedHttpRequestError("Service URL must use HTTP or HTTPS.");
  if (input.url.username || input.url.password)
    throw new OwnedHttpRequestError("Service URL must not include credentials.");
  input.signal.throwIfAborted();
  let request: ClientRequest | undefined;
  let socket: Socket | undefined;
  let requestClosed = false,
    socketClosed = false,
    sent = false;
  let confirmClose!: () => void;
  const closed = new Promise<void>((resolve) => {
    confirmClose = resolve;
  });
  const maybeClosed = (): void => {
    if (requestClosed && (socket === undefined || socketClosed)) confirmClose();
  };
  const response = new Promise<OwnedHttpResponse>((resolve, reject) => {
    try {
      request = (input.url.protocol === "https:" ? requestHttps : requestHttp)(
        input.url,
        {
          agent: false,
          ...(input.url.protocol === "https:" && input.caPem !== undefined
            ? { ca: [input.caPem] }
            : {}),
          ...(input.url.protocol === "https:"
            ? tlsClientIdentityOptions(input.clientIdentity)
            : {}),
          headers: {
            accept: "application/json",
            ...(input.authorization === undefined ? {} : { authorization: input.authorization }),
            ...(input.encodedBody === undefined
              ? {}
              : {
                  "content-length": Buffer.byteLength(input.encodedBody, "utf8"),
                  "content-type": input.contentType ?? "application/json",
                }),
          },
          ...(input.lookup === undefined ? {} : { lookup: input.lookup }),
          method: input.method,
          rejectUnauthorized: true,
          signal: input.signal,
        },
        (incoming) => {
          sent = true;
          incoming.once("error", reject);
          const status = incoming.statusCode ?? 0;
          if (status >= 300 && status < 400) {
            incoming.destroy();
            reject(new OwnedHttpRequestError("Service redirects are not allowed."));
            return;
          }
          // Connect mutation receipts depend on the actual status, not a body that can
          // be lost after a successful acknowledgement. Never parse remote error text.
          if (input.responseMode === "status") {
            resolve({ status, body: null });
            return;
          }
          const chunks: Buffer[] = [];
          let received = 0;
          incoming.on("data", (chunk: Buffer) => {
            received += chunk.length;
            if (received > input.maximumResponseBytes) {
              const error = new OwnedHttpRequestError("Service response exceeded its byte limit.");
              reject(error);
              incoming.destroy(error);
              return;
            }
            chunks.push(chunk);
          });
          incoming.once("aborted", () =>
            reject(new OwnedHttpRequestError("Service response was interrupted.")),
          );
          incoming.once("end", () => {
            const body = Buffer.concat(chunks).toString("utf8");
            if (input.responseMode === "text") {
              resolve({ status, body });
              return;
            }
            if (!body.length) {
              resolve({ status, body: null });
              return;
            }
            try {
              resolve({ status, body: JSON.parse(body) as unknown });
            } catch {
              if (status < 200 || status >= 300) resolve({ status, body: null });
              else reject(new OwnedHttpRequestError("Service returned invalid JSON."));
            }
          });
        },
      );
      request.once("socket", (value) => {
        socket = value;
        value.once("close", () => {
          socketClosed = true;
          maybeClosed();
        });
      });
      request.once("finish", () => {
        sent = true;
      });
      request.once("close", () => {
        requestClosed = true;
        maybeClosed();
      });
      request.once("error", reject);
      request.end(input.encodedBody);
    } catch (error) {
      if (request) request.destroy();
      else {
        requestClosed = true;
        maybeClosed();
      }
      reject(
        error instanceof Error
          ? error
          : new OwnedHttpRequestError("Service request could not be opened."),
      );
    }
  });
  // A revoked caller can stop waiting for a reply; its original lease still owns it.
  void response.catch(() => undefined);
  return {
    response,
    closed,
    dispatched: (): boolean => sent,
    close: (): Promise<void> => {
      request?.destroy();
      return closed;
    },
  };
}

/** A bounded attempt reports failure but retains the lease and its actual close proof. */
export function boundedHttpCleanup(request: OwnedHttpRequest, milliseconds = 2000): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new OwnedHttpRequestError("Original HTTP cleanup remains unresolved.")),
      milliseconds,
    );
    void request.close().then(
      () => {
        clearTimeout(timeout);
        resolve();
      },
      () => {
        clearTimeout(timeout);
        reject(new OwnedHttpRequestError("Original HTTP cleanup remains unresolved."));
      },
    );
  });
}
