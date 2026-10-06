import { EventEmitter } from "node:events";

import type { AuthInfo, ClientRequest, ClientRequestConstructorOptions } from "electron";
import { describe, expect, it, vi, type Mock } from "vitest";

import { createPluginNetworkTransport } from "../../src/platform/electron/main/plugin-network-transport";

class NativeRequest extends EventEmitter {
  readonly abort = vi.fn(() => this.emit("abort"));
  readonly end = vi.fn();
}

function fixture(): {
  transport: ReturnType<typeof createPluginNetworkTransport>;
  requests: NativeRequest[];
  session: {
    setProxy: Mock<() => Promise<undefined>>;
    closeAllConnections: Mock<() => Promise<undefined>>;
    clearAuthCache: Mock<() => Promise<undefined>>;
  };
  request: Mock<(options: ClientRequestConstructorOptions) => ClientRequest>;
} {
  const requests: NativeRequest[] = [];
  const session = {
    setProxy: vi.fn(() => Promise.resolve(undefined)),
    closeAllConnections: vi.fn(() => Promise.resolve(undefined)),
    clearAuthCache: vi.fn(() => Promise.resolve(undefined)),
  };
  const request = vi.fn((_options: ClientRequestConstructorOptions) => {
    const value = new NativeRequest();
    requests.push(value);
    return value as unknown as ClientRequest;
  });
  const transport = createPluginNetworkTransport({ session, request });
  return { transport, requests, session, request };
}

function respond(request: NativeRequest, status = 200): EventEmitter {
  const response = Object.assign(new EventEmitter(), {
    statusCode: status,
    headers: { "content-type": "application/octet-stream" },
  });
  request.emit("response", response);
  return response;
}

const authentication: AuthInfo = {
  isProxy: true,
  scheme: "basic",
  host: "proxy.example",
  port: 8080,
  realm: "fixture",
};
const proxy = {
  mode: "custom" as const,
  proxyUrl: "http://proxy.example:8080",
  credentials: { username: "operator", password: "protected-password" },
};

describe("dedicated native plugin network transport", () => {
  it("configures only its owned session and clears cached proxy authentication", async () => {
    const value = fixture();
    await value.transport.configure(proxy);
    expect(value.session.setProxy).toHaveBeenCalledWith({
      mode: "fixed_servers",
      proxyRules: "http://proxy.example:8080",
      proxyBypassRules: "<-loopback>",
    });
    const fetching = value.transport.fetch("https://api.github.com/fixture");
    const request = value.requests[0]!;
    const answer = vi.fn();
    request.emit("login", authentication, answer);
    expect(answer).toHaveBeenCalledWith("operator", "protected-password");
    const cancelled = expect(fetching).rejects.toMatchObject({ name: "AbortError" });
    await value.transport.configure({ mode: "system" });
    await cancelled;
    expect(request.abort).toHaveBeenCalledOnce();
    expect(value.session.clearAuthCache).toHaveBeenCalledTimes(2);
    expect(value.session.setProxy).toHaveBeenLastCalledWith({ mode: "system" });
    await value.transport.close();
  });

  it.each([
    { ...authentication, isProxy: false },
    { ...authentication, host: "different-proxy.example" },
    { ...authentication, port: 3128 },
  ])("never forwards custom credentials to an unrelated authentication challenge", async (info) => {
    const value = fixture();
    await value.transport.configure(proxy);
    const fetching = value.transport.fetch("https://api.github.com/fixture");
    const answer = vi.fn();
    value.requests[0]!.emit("login", info, answer);
    expect(answer).toHaveBeenCalledWith();
    expect(JSON.stringify(answer.mock.calls)).not.toContain("protected-password");
    const failure = expect(fetching).rejects.toBeInstanceOf(Error);
    await value.transport.close();
    await failure;
  });

  it("streams exact bytes, owns cancellation through the body and returns redirects for catalog validation", async () => {
    const value = fixture();
    const controller = new AbortController();
    const fetching = value.transport.fetch("https://api.github.com/fixture", {
      signal: controller.signal,
    });
    const native = respond(value.requests[0]!);
    const response = await fetching;
    const reading = response.arrayBuffer();
    native.emit("data", Buffer.from("verified"));
    const failure = expect(reading).rejects.toMatchObject({ name: "AbortError" });
    controller.abort();
    await failure;
    expect(value.requests[0]!.abort).toHaveBeenCalledOnce();
    const redirecting = value.transport.fetch("https://api.github.com/fixture");
    value.requests[1]!.emit("redirect", 302, "GET", "https://untrusted.example/file", {});
    const redirect = await redirecting;
    expect(redirect.status).toBe(302);
    expect(redirect.headers.get("location")).toBe("https://untrusted.example/file");
    expect(value.requests[1]!.abort).toHaveBeenCalledOnce();
    expect(value.request.mock.calls[0]![0].redirect).toBe("manual");
    await value.transport.close();
  });

  it("hides raw native diagnostics and rejects credentials embedded in proxy URLs", async () => {
    const value = fixture();
    await expect(
      value.transport.configure({
        mode: "custom",
        proxyUrl: "http://secret:password@proxy.example",
      }),
    ).rejects.toThrow("without embedded credentials");
    const fetching = value.transport.fetch("https://api.github.com/fixture");
    value.requests[0]!.emit(
      "error",
      new Error("net::ERR_CERT_AUTHORITY_INVALID https://internal.secret password"),
    );
    await expect(fetching).rejects.toThrow("trusted CA");
    await value.transport.close();
  });

  it("completes bytes without falsely treating normal close as truncated and clears secrets at shutdown", async () => {
    const value = fixture();
    const fetching = value.transport.fetch("https://api.github.com/fixture");
    const native = respond(value.requests[0]!);
    const response = await fetching;
    native.emit("data", Buffer.from("complete"));
    native.emit("end");
    value.requests[0]!.emit("close");
    expect(await response.text()).toBe("complete");
    await value.transport.close();
    await expect(value.transport.fetch("https://api.github.com/fixture")).rejects.toThrow(
      "reconfigured",
    );
  });

  it("does not restore credentials when shutdown interrupts proxy configuration", async () => {
    const value = fixture();
    let finish: (() => void) | undefined;
    value.session.setProxy.mockImplementationOnce(
      () =>
        new Promise<undefined>((resolve) => {
          finish = (): void => resolve(undefined);
        }),
    );
    const configuring = value.transport.configure(proxy);
    await vi.waitFor(() => expect(finish).toBeDefined());
    const failure = expect(configuring).rejects.toThrow("could not be applied");
    await value.transport.close();
    finish?.();
    await failure;
    await expect(value.transport.fetch("https://api.github.com/fixture")).rejects.toThrow(
      "reconfigured",
    );
    expect(value.requests).toHaveLength(0);
  });
});
