import { describe, expect, it, vi } from "vitest";

import { serviceConnectionContext } from "../../src/features/kafka/engine/service-connection-context";
import type {
  KafkaClientInput,
  OAuthToken,
  OAuthTokenRequester,
} from "../../src/features/kafka/engine/types";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import type { BoundedJsonHttpPort } from "../../src/features/kafka/engine/bounded-json-http";

const broker: KafkaClientInput = {
  brokers: ["broker.example:9093"],
  caPem: "broker-ca",
  clientIdentity: { certificatePem: "broker-cert", privateKeyPem: "broker-key" },
  operationTimeoutMs: 500,
  oauthTokenProvider: (): Promise<OAuthToken> => Promise.resolve({ value: "broker-token" }),
};
const oauth = {
  clientId: "service",
  clientSecret: "service-secret",
  scope: "service",
  tokenEndpoint: "https://identity.example/token",
};

function options(
  requestOAuthToken: OAuthTokenRequester = (): Promise<OAuthToken> =>
    Promise.resolve({ value: "service-token" }),
): {
  readonly controller: AbortController;
  readonly lifecycleSignal: AbortSignal;
  readonly operationTimeoutMs: number;
  readonly requestOAuthToken: OAuthTokenRequester;
} {
  const controller = new AbortController();
  return {
    controller,
    lifecycleSignal: controller.signal,
    operationTimeoutMs: 500,
    requestOAuthToken,
  };
}

describe("independent cluster service security", () => {
  it("keeps the admitted OAuth identity stable when a caller edits its profile object later", async () => {
    const requestOAuthToken = vi
      .fn<OAuthTokenRequester>()
      .mockResolvedValue({ value: "token", expiresAt: 0 });
    const endpoint = {
      baseUrl: "https://registry.example",
      authentication: "oauth-client" as const,
      oauth: { ...oauth },
      tls: {
        caPem: "original-ca",
        clientIdentity: { certificatePem: "original-cert", privateKeyPem: "original-key" },
      },
    };
    const context = serviceConnectionContext(endpoint, broker, options(requestOAuthToken));
    endpoint.oauth.clientSecret = "changed-secret";
    endpoint.oauth.tokenEndpoint = "https://changed.example/token";
    endpoint.tls.clientIdentity.privateKeyPem = "changed-key";
    expect(Object.isFrozen(context.clientIdentity)).toBe(true);
    await context.authorization();
    await context.authorization();
    expect(requestOAuthToken).toHaveBeenCalledTimes(2);
    for (const [request] of requestOAuthToken.mock.calls)
      expect(request).toMatchObject({
        clientSecret: "service-secret",
        tokenEndpoint: "https://identity.example/token",
        caPem: "original-ca",
        clientIdentity: { privateKeyPem: "original-key" },
      });
  });
  it("keeps legacy CA/OAuth inheritance without forwarding the broker client certificate", async () => {
    const context = serviceConnectionContext(
      { baseUrl: "https://registry.example", authentication: "oauth" },
      broker,
      options(),
    );
    expect(context.caPem).toBe("broker-ca");
    expect(context).not.toHaveProperty("clientIdentity");
    await expect(context.authorization()).resolves.toBe("Bearer broker-token");
    const system = serviceConnectionContext(
      { baseUrl: "https://registry.example", authentication: "none", tls: {} },
      broker,
      options(),
    );
    expect(system).not.toHaveProperty("caPem");
    expect(system).not.toHaveProperty("clientIdentity");
    await expect(system.authorization()).resolves.toBeUndefined();
  });

  it("uses distinct Basic/bearer credentials and only the explicitly selected service identity", async () => {
    const identity = {
      certificatePem: "registry-cert",
      privateKeyPem: "registry-key",
      passphrase: "registry-key-secret",
    };
    const basic = serviceConnectionContext(
      {
        baseUrl: "https://registry.example",
        authentication: "basic",
        basic: { username: "registry-user", password: "registry-password" },
        tls: { caPem: "registry-ca", clientIdentity: identity },
      },
      broker,
      options(),
    );
    expect(basic.caPem).toBe("registry-ca");
    expect(basic.clientIdentity).toEqual(identity);
    await expect(basic.authorization()).resolves.toBe(
      `Basic ${Buffer.from("registry-user:registry-password").toString("base64")}`,
    );
    const bearer = serviceConnectionContext(
      {
        baseUrl: "https://connect.example",
        authentication: "bearer",
        bearer: "connect-token",
        tls: {},
      },
      broker,
      options(),
    );
    await expect(bearer.authorization()).resolves.toBe("Bearer connect-token");
    expect(bearer.clientIdentity).toBeUndefined();
  });

  it("isolates service OAuth credentials/trust and shares concurrent refreshes without sharing tokens between services", async () => {
    let resolveToken!: (token: OAuthToken) => void;
    const requestOAuthToken = vi
      .fn<OAuthTokenRequester>()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveToken = resolve;
          }),
      )
      .mockResolvedValue({ value: "connect-token", expiresAt: Date.now() + 60_000 });
    const common = options(requestOAuthToken);
    const identity = { certificatePem: "registry-cert", privateKeyPem: "registry-key" };
    const registry = serviceConnectionContext(
      {
        baseUrl: "https://registry.example",
        authentication: "oauth-client",
        oauth,
        tls: { caPem: "registry-ca", clientIdentity: identity },
      },
      broker,
      common,
    );
    const first = registry.authorization();
    const second = registry.authorization();
    expect(requestOAuthToken).toHaveBeenCalledTimes(1);
    expect(requestOAuthToken).toHaveBeenCalledWith(
      expect.objectContaining({ ...oauth, caPem: "registry-ca", clientIdentity: identity }),
    );
    resolveToken({ value: "registry-token", expiresAt: Date.now() + 60_000 });
    await expect(Promise.all([first, second])).resolves.toEqual([
      "Bearer registry-token",
      "Bearer registry-token",
    ]);
    const connect = serviceConnectionContext(
      {
        baseUrl: "https://connect.example",
        authentication: "oauth-client",
        oauth: { ...oauth, clientId: "connect" },
        tls: {},
      },
      broker,
      common,
    );
    await expect(connect.authorization()).resolves.toBe("Bearer connect-token");
    expect(requestOAuthToken).toHaveBeenCalledTimes(2);
    expect(requestOAuthToken.mock.calls[1]?.[0]).not.toHaveProperty("caPem");
    expect(requestOAuthToken.mock.calls[1]?.[0]).not.toHaveProperty("clientIdentity");
    await expect(registry.authorization()).resolves.toBe("Bearer registry-token");
  });

  it("refreshes expired service tokens and revokes cached credentials on disconnect", async () => {
    const requestOAuthToken = vi
      .fn<OAuthTokenRequester>()
      .mockResolvedValueOnce({ value: "old", expiresAt: 0 })
      .mockResolvedValue({ value: "fresh", expiresAt: Date.now() + 60_000 });
    const common = options(requestOAuthToken);
    const context = serviceConnectionContext(
      { baseUrl: "https://registry.example", authentication: "oauth-client", oauth },
      broker,
      common,
    );
    await expect(context.authorization()).resolves.toBe("Bearer old");
    await expect(Promise.all([context.authorization(), context.authorization()])).resolves.toEqual([
      "Bearer fresh",
      "Bearer fresh",
    ]);
    expect(requestOAuthToken).toHaveBeenCalledTimes(2);
    common.controller.abort();
    await expect(context.authorization()).rejects.toMatchObject({ name: "AbortError" });
    expect(requestOAuthToken).toHaveBeenCalledTimes(2);
  });

  it("cancels one caller without cancelling another caller's shared token acquisition", async () => {
    let resolveToken!: (token: OAuthToken) => void;
    const requestOAuthToken = vi.fn<OAuthTokenRequester>().mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveToken = resolve;
        }),
    );
    const context = serviceConnectionContext(
      { baseUrl: "https://registry.example", authentication: "oauth-client", oauth },
      broker,
      options(requestOAuthToken),
    );
    const caller = new AbortController();
    const cancelled = context.authorization(caller.signal);
    const surviving = context.authorization();
    caller.abort();
    await expect(cancelled).rejects.toMatchObject({ name: "AbortError" });
    expect(requestOAuthToken.mock.calls[0]?.[0].signal.aborted).toBe(false);
    resolveToken({ value: "fresh" });
    await expect(surviving).resolves.toBe("Bearer fresh");
  });

  it("cancels in-flight HTTP work with its connection and refuses retained contexts after close", async () => {
    const common = options();
    const context = serviceConnectionContext(
      {
        baseUrl: "https://registry.example",
        authentication: "basic",
        basic: { username: "user", password: "secret" },
      },
      broker,
      common,
    );
    const request = vi.fn<BoundedJsonHttpPort["request"]>().mockImplementation(
      ({ signal }) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason as Error), { once: true });
        }),
    );
    const adapter = new SchemaRegistryHttpAdapter({ request });
    const running = adapter.listSubjects(context, new AbortController().signal);
    await vi.waitFor(() => expect(request).toHaveBeenCalledTimes(1));
    common.controller.abort();
    await expect(running).rejects.toMatchObject({ name: "AbortError" });
    await expect(adapter.listSubjects(context, new AbortController().signal)).rejects.toMatchObject(
      { name: "AbortError" },
    );
    expect(request).toHaveBeenCalledTimes(1);
  });
});
