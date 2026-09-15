import { lookup as lookupDns } from "node:dns";
import { request as requestHttp } from "node:http";
import type { RequestOptions } from "node:http";
import { request as requestHttps } from "node:https";

import type { OAuthToken, OAuthTokenRequest } from "./types";

const OAUTH_RESPONSE_BYTE_LIMIT = 65_536;

interface OAuthHttpResponse {
  readonly body: string;
  readonly status: number;
}

export class OAuthEndpointResponseError extends Error {
  constructor(
    readonly status: number,
    readonly responseBody: string,
  ) {
    super(`OAuth token endpoint returned HTTP ${status}.`);
    this.name = "OAuthEndpointResponseError";
  }
}

const lookupLocalhost: NonNullable<RequestOptions["lookup"]> = (
  hostname,
  options,
  callback,
): void => {
  if (options.all === true) {
    lookupDns(hostname, { ...options, all: true, order: "ipv4first" }, (error, addresses) => {
      callback(error, addresses);
    });
    return;
  }
  lookupDns(hostname, { ...options, all: false, order: "ipv4first" }, (error, address, family) => {
    callback(error, address, family);
  });
};

function endpointParameters(endpoint: URL): Readonly<Record<string, string>> {
  return Object.fromEntries(endpoint.searchParams);
}

function postForm(
  endpoint: URL,
  body: URLSearchParams,
  caPem: string,
  signal: AbortSignal,
  authorization?: string,
): Promise<OAuthHttpResponse> {
  return new Promise((resolve, reject) => {
    const encodedBody = body.toString();
    const request = (endpoint.protocol === "https:" ? requestHttps : requestHttp)(
      endpoint,
      {
        ca: endpoint.protocol === "https:" ? [caPem] : undefined,
        headers: {
          ...(authorization === undefined ? {} : { authorization }),
          "content-length": Buffer.byteLength(encodedBody),
          "content-type": "application/x-www-form-urlencoded",
        },
        ...(endpoint.hostname === "localhost" ? { lookup: lookupLocalhost } : {}),
        method: "POST",
        rejectUnauthorized: true,
        signal,
      },
      (response) => {
        const chunks: Buffer[] = [];
        let receivedBytes = 0;
        response.on("data", (chunk: Buffer) => {
          receivedBytes += chunk.length;
          if (receivedBytes > OAUTH_RESPONSE_BYTE_LIMIT) {
            response.destroy(new Error("OAuth token response exceeded the 64 KiB limit."));
            return;
          }
          chunks.push(chunk);
        });
        response.once("end", () => {
          resolve({
            body: Buffer.concat(chunks).toString("utf8"),
            status: response.statusCode ?? 0,
          });
        });
        response.once("error", reject);
      },
    );
    request.once("error", reject);
    request.end(encodedBody);
  });
}

function parseTokenResponse(body: string): OAuthToken {
  let value: string;
  let expiresIn: number | undefined;
  try {
    const parsed = JSON.parse(body) as {
      readonly access_token?: unknown;
      readonly expires_in?: unknown;
    };
    value = typeof parsed.access_token === "string" ? parsed.access_token : "";
    const parsedExpiresIn = Number(parsed.expires_in);
    expiresIn =
      Number.isFinite(parsedExpiresIn) && parsedExpiresIn > 0 ? parsedExpiresIn : undefined;
  } catch {
    const parameters = new URLSearchParams(body);
    value = parameters.get("access_token") ?? "";
    const parsedExpiresIn = Number(parameters.get("expires_in"));
    expiresIn =
      Number.isFinite(parsedExpiresIn) && parsedExpiresIn > 0 ? parsedExpiresIn : undefined;
  }
  if (value.length === 0) {
    throw new Error("OAuth token endpoint returned no access_token.");
  }
  return expiresIn === undefined
    ? { value }
    : {
        expiresAt: Date.now() + expiresIn * 1_000 - 5_000,
        value,
      };
}

export async function requestOAuthToken(request: OAuthTokenRequest): Promise<OAuthToken> {
  const endpoint = new URL(request.tokenEndpoint);
  if (endpoint.protocol !== "http:" && endpoint.protocol !== "https:") {
    throw new Error("OAuth token endpoint must use HTTP or HTTPS.");
  }
  const extras = endpointParameters(endpoint);
  const basicBody = new URLSearchParams({
    grant_type: "client_credentials",
    scope: request.scope,
    ...extras,
  });
  const authorization = `Basic ${Buffer.from(
    `${request.clientId}:${request.clientSecret}`,
    "utf8",
  ).toString("base64")}`;
  const basicResponse = await postForm(
    endpoint,
    basicBody,
    request.caPem,
    request.signal,
    authorization,
  );
  if (basicResponse.status >= 200 && basicResponse.status < 300) {
    return parseTokenResponse(basicResponse.body);
  }

  const postBody = new URLSearchParams({
    client_id: request.clientId,
    client_secret: request.clientSecret,
    grant_type: "client_credentials",
    scope: request.scope,
    ...extras,
  });
  const postResponse = await postForm(endpoint, postBody, request.caPem, request.signal);
  if (postResponse.status < 200 || postResponse.status >= 300) {
    throw new OAuthEndpointResponseError(postResponse.status, postResponse.body);
  }
  return parseTokenResponse(postResponse.body);
}
