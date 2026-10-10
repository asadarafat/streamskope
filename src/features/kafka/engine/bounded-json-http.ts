import type { ConnectionClientIdentity } from "../contracts";

import { openOwnedHttpRequest, type OwnedHttpRequest } from "./owned-http-request";

export interface BoundedJsonHttpRequest {
  readonly authorization?: string;
  readonly body?: unknown;
  readonly contentType?: string;
  readonly caPem?: string;
  readonly clientIdentity?: ConnectionClientIdentity;
  readonly method: "DELETE" | "GET" | "PATCH" | "POST" | "PUT";
  readonly responseMode?: "json" | "status";
  readonly signal: AbortSignal;
  readonly url: string;
}
export interface BoundedJsonHttpResponse {
  readonly body: unknown;
  readonly status: number;
}
export interface BoundedJsonHttpPort {
  request(input: BoundedJsonHttpRequest): Promise<BoundedJsonHttpResponse>;
}
export interface OwnedJsonHttpPort extends BoundedJsonHttpPort {
  open(input: BoundedJsonHttpRequest): OwnedHttpRequest;
}
export interface NodeBoundedJsonHttpOptions {
  readonly maximumRequestBytes?: number;
  readonly maximumResponseBytes?: number;
  readonly timeoutMs?: number;
}
const DEFAULT_MAXIMUM_BYTES = 4 * 1_048_576;
const DEFAULT_TIMEOUT_MS = 8_000;
export class BoundedJsonHttpError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BoundedJsonHttpError";
  }
}
export class NodeBoundedJsonHttp implements OwnedJsonHttpPort {
  private readonly maximumRequestBytes;
  private readonly maximumResponseBytes;
  private readonly timeoutMs;
  constructor(options: NodeBoundedJsonHttpOptions = {}) {
    this.maximumRequestBytes = options.maximumRequestBytes ?? DEFAULT_MAXIMUM_BYTES;
    this.maximumResponseBytes = options.maximumResponseBytes ?? DEFAULT_MAXIMUM_BYTES;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  }
  async request(input: BoundedJsonHttpRequest): Promise<BoundedJsonHttpResponse> {
    const request = this.open(input);
    try {
      return await request.response;
    } finally {
      await request.close();
    }
  }
  open(input: BoundedJsonHttpRequest): OwnedHttpRequest {
    const endpoint = new URL(input.url);
    const encodedBody = input.body === undefined ? undefined : JSON.stringify(input.body);
    if (
      encodedBody !== undefined &&
      Buffer.byteLength(encodedBody, "utf8") > this.maximumRequestBytes
    )
      throw new BoundedJsonHttpError("Service request exceeded its byte limit.");
    return openOwnedHttpRequest({
      url: endpoint,
      method: input.method,
      signal: AbortSignal.any([input.signal, AbortSignal.timeout(this.timeoutMs)]),
      ...(encodedBody === undefined ? {} : { encodedBody }),
      ...(input.authorization === undefined ? {} : { authorization: input.authorization }),
      contentType: input.contentType ?? "application/vnd.schemaregistry.v1+json",
      ...(input.caPem === undefined ? {} : { caPem: input.caPem }),
      ...(input.clientIdentity === undefined ? {} : { clientIdentity: input.clientIdentity }),
      responseMode: input.responseMode ?? "json",
      maximumResponseBytes: this.maximumResponseBytes,
    });
  }
}
