import { request as requestHttp } from "node:http";
import { request as requestHttps } from "node:https";

export interface BoundedJsonHttpRequest {
  readonly authorization?: string;
  readonly body?: unknown;
  readonly caPem: string;
  readonly method: "DELETE" | "GET" | "POST" | "PUT";
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

export class NodeBoundedJsonHttp implements BoundedJsonHttpPort {
  private readonly maximumRequestBytes;
  private readonly maximumResponseBytes;
  private readonly timeoutMs;

  constructor(options: NodeBoundedJsonHttpOptions = {}) {
    this.maximumRequestBytes = options.maximumRequestBytes ?? DEFAULT_MAXIMUM_BYTES;
    this.maximumResponseBytes = options.maximumResponseBytes ?? DEFAULT_MAXIMUM_BYTES;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  }

  request(input: BoundedJsonHttpRequest): Promise<BoundedJsonHttpResponse> {
    const endpoint = new URL(input.url);
    if (endpoint.protocol !== "http:" && endpoint.protocol !== "https:") {
      return Promise.reject(new BoundedJsonHttpError("Service URL must use HTTP or HTTPS."));
    }
    const encodedBody = input.body === undefined ? undefined : JSON.stringify(input.body);
    if (
      encodedBody !== undefined &&
      Buffer.byteLength(encodedBody, "utf8") > this.maximumRequestBytes
    ) {
      return Promise.reject(new BoundedJsonHttpError("Service request exceeded its byte limit."));
    }
    const timeoutSignal = AbortSignal.timeout(this.timeoutMs);
    const signal = AbortSignal.any([input.signal, timeoutSignal]);
    return new Promise((resolve, reject) => {
      const request = (endpoint.protocol === "https:" ? requestHttps : requestHttp)(
        endpoint,
        {
          ca: endpoint.protocol === "https:" ? [input.caPem] : undefined,
          headers: {
            accept: "application/json",
            ...(input.authorization === undefined ? {} : { authorization: input.authorization }),
            ...(encodedBody === undefined
              ? {}
              : {
                  "content-length": Buffer.byteLength(encodedBody, "utf8"),
                  "content-type": "application/vnd.schemaregistry.v1+json",
                }),
          },
          method: input.method,
          rejectUnauthorized: true,
          signal,
        },
        (response) => {
          const status = response.statusCode ?? 0;
          if (status >= 300 && status < 400) {
            response.resume();
            reject(new BoundedJsonHttpError("Service redirects are not allowed."));
            return;
          }
          const chunks: Buffer[] = [];
          let receivedBytes = 0;
          response.on("data", (chunk: Buffer) => {
            receivedBytes += chunk.length;
            if (receivedBytes > this.maximumResponseBytes) {
              response.destroy(
                new BoundedJsonHttpError("Service response exceeded its byte limit."),
              );
              return;
            }
            chunks.push(chunk);
          });
          response.once("error", reject);
          response.once("end", () => {
            const text = Buffer.concat(chunks).toString("utf8");
            if (text.length === 0) {
              resolve({ body: null, status });
              return;
            }
            try {
              resolve({ body: JSON.parse(text) as unknown, status });
            } catch {
              reject(new BoundedJsonHttpError("Service returned invalid JSON."));
            }
          });
        },
      );
      request.once("error", reject);
      request.end(encodedBody);
    });
  }
}
