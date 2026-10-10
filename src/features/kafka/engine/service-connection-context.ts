import type { KafkaClusterServiceContext } from "../application";
import type { ResolvedClusterServiceEndpoint } from "../contracts";

import { abortableOperation } from "./abortable-operation";
import type { KafkaClientInput, OAuthToken, OAuthTokenRequester } from "./types";

interface ServiceConnectionOptions {
  readonly lifecycleSignal: AbortSignal;
  readonly operationTimeoutMs: number;
  readonly requestOAuthToken: OAuthTokenRequester;
  readonly ownWork?: <T>(start: () => Promise<T>) => Promise<T>;
}

/** One context owns one service's refresh cache and its connection lifetime. */
export function serviceConnectionContext(
  endpoint: ResolvedClusterServiceEndpoint,
  broker: KafkaClientInput,
  options: ServiceConnectionOptions,
): KafkaClusterServiceContext {
  endpoint = structuredClone(endpoint);
  const brokerTokenProvider = broker.oauthTokenProvider;
  const caPem = endpoint.tls === undefined ? broker.caPem : endpoint.tls.caPem;
  const clientIdentity =
    endpoint.tls?.clientIdentity === undefined
      ? undefined
      : Object.freeze({ ...endpoint.tls.clientIdentity });
  let token: OAuthToken | undefined;
  let refresh: Promise<OAuthToken> | undefined;
  const serviceToken = async (): Promise<OAuthToken> => {
    options.lifecycleSignal.throwIfAborted();
    if (token !== undefined && (token.expiresAt === undefined || token.expiresAt > Date.now()))
      return token;
    if (refresh !== undefined) return refresh;
    const oauth = endpoint.oauth;
    if (oauth === undefined) throw new Error("The service OAuth client credentials are missing.");
    const signal = AbortSignal.any([
      options.lifecycleSignal,
      AbortSignal.timeout(options.operationTimeoutMs),
    ]);
    const acquire = (): Promise<OAuthToken> =>
      options.requestOAuthToken({
        ...oauth,
        ...(caPem === undefined ? {} : { caPem }),
        ...(clientIdentity === undefined ? {} : { clientIdentity }),
        signal,
      });
    const original = options.ownWork ? options.ownWork(acquire) : acquire();
    refresh = abortableOperation(original, signal, () => signal.reason as Error)
      .then((value) => {
        options.lifecycleSignal.throwIfAborted();
        token = value;
        return value;
      })
      .finally(() => {
        refresh = undefined;
      });
    return refresh;
  };
  return Object.freeze({
    baseUrl: endpoint.baseUrl,
    ...(caPem === undefined ? {} : { caPem }),
    ...(clientIdentity === undefined ? {} : { clientIdentity }),
    signal: options.lifecycleSignal,
    async authorization(signal?: AbortSignal): Promise<string | undefined> {
      options.lifecycleSignal.throwIfAborted();
      signal?.throwIfAborted();
      switch (endpoint.authentication) {
        case "none":
          return undefined;
        case "basic": {
          if (endpoint.basic === undefined)
            throw new Error("The service Basic credentials are missing.");
          return `Basic ${Buffer.from(`${endpoint.basic.username}:${endpoint.basic.password}`, "utf8").toString("base64")}`;
        }
        case "bearer": {
          if (endpoint.bearer === undefined)
            throw new Error("The service bearer credential is missing.");
          return `Bearer ${endpoint.bearer}`;
        }
        case "oauth":
        case "oauth-client": {
          const operation =
            endpoint.authentication === "oauth-client" ? serviceToken() : brokerTokenProvider?.();
          if (operation === undefined)
            throw new Error("The service has no broker OAuth token provider.");
          const lifetime =
            signal === undefined
              ? options.lifecycleSignal
              : AbortSignal.any([signal, options.lifecycleSignal]);
          const current = await abortableOperation(
            operation,
            lifetime,
            () => lifetime.reason as Error,
          );
          lifetime.throwIfAborted();
          return `Bearer ${current.value}`;
        }
      }
    },
  });
}
