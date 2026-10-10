import { lookup as lookupDns } from "node:dns";
import type { RequestOptions } from "node:http";

import type { OAuthToken, OAuthTokenRequest } from "./types";
import { openOwnedHttpRequest } from "./owned-http-request";

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

async function postForm(
  endpoint: URL,
  body: URLSearchParams,
  caPem: string | undefined,
  signal: AbortSignal,
  authorization?: string,
  clientIdentity?: OAuthTokenRequest["clientIdentity"],
): Promise<OAuthHttpResponse> {
  const request = openOwnedHttpRequest({
    url: endpoint,
    method: "POST",
    signal,
    encodedBody: body.toString(),
    contentType: "application/x-www-form-urlencoded",
    ...(caPem === undefined ? {} : { caPem }),
    ...(authorization === undefined ? {} : { authorization }),
    ...(clientIdentity === undefined ? {} : { clientIdentity }),
    ...(endpoint.hostname === "localhost" ? { lookup: lookupLocalhost } : {}),
    responseMode: "text",
    maximumResponseBytes: OAUTH_RESPONSE_BYTE_LIMIT,
  });
  try {
    const response = await request.response;
    if (typeof response.body !== "string") throw new Error("Invalid OAuth response.");
    return { status: response.status, body: response.body };
  } finally {
    await request.close();
  }
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
    ...(request.scope.trim() ? { scope: request.scope } : {}),
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
    request.clientIdentity,
  );
  if (basicResponse.status >= 200 && basicResponse.status < 300) {
    return parseTokenResponse(basicResponse.body);
  }

  const postBody = new URLSearchParams({
    client_id: request.clientId,
    client_secret: request.clientSecret,
    grant_type: "client_credentials",
    ...(request.scope.trim() ? { scope: request.scope } : {}),
    ...extras,
  });
  const postResponse = await postForm(
    endpoint,
    postBody,
    request.caPem,
    request.signal,
    undefined,
    request.clientIdentity,
  );
  if (postResponse.status < 200 || postResponse.status >= 300) {
    throw new OAuthEndpointResponseError(postResponse.status, postResponse.body);
  }
  return parseTokenResponse(postResponse.body);
}
