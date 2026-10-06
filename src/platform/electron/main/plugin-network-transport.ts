import type {
  ClientRequest,
  ClientRequestConstructorOptions,
  IncomingMessage,
  Session,
} from "electron";

import { MAX_PLUGIN_ARCHIVE_BYTES } from "../../node/plugins/package";
import type {
  PluginNetworkTransport,
  PluginNetworkTransportConfiguration,
} from "../../node/plugins/network-transport";

export interface PluginNativeNetworkPort {
  readonly session: Pick<Session, "setProxy" | "closeAllConnections" | "clearAuthCache">;
  readonly request: (options: ClientRequestConstructorOptions) => ClientRequest;
}

/** Public diagnostics deliberately exclude Chromium's raw URLs and proxy credentials. */
function networkFailure(error: unknown): Error {
  const message = error instanceof Error ? error.message : "";
  if (/PROXY_AUTH|INVALID_AUTH_CREDENTIALS|HTTP.*407/iu.test(message))
    return new Error("Plugin proxy authentication failed. Check the proxy username and password.");
  if (/PROXY_CONNECTION|TUNNEL_CONNECTION|NO_SUPPORTED_PROXIES/iu.test(message))
    return new Error(
      "The plugin proxy could not connect. Check its address, port and tunnel policy.",
    );
  if (/CERT_/iu.test(message))
    return new Error(
      "Plugin download certificate validation failed. Install your organization's trusted CA in the operating system.",
    );
  if (/SSL_|TLS/iu.test(message))
    return new Error(
      "Plugin TLS negotiation failed. Check the proxy's protocol and supported TLS configuration.",
    );
  if (/TIMED_OUT|TimeoutError/iu.test(message))
    return new Error(
      "The plugin connection timed out. Check your proxy and network, or install a signed file.",
    );
  return new Error(
    "Plugin download connection failed. Check system or custom proxy settings, or install a signed file.",
  );
}

function headersFromNative(values: Record<string, string | string[]>): Headers {
  const headers = new Headers();
  for (const [name, value] of Object.entries(values)) {
    for (const item of Array.isArray(value) ? value : [value]) headers.append(name, item);
  }
  return headers;
}

function proxyEndpoint(configuration: PluginNetworkTransportConfiguration): URL | undefined {
  if (configuration.mode === "system") return undefined;
  const endpoint = new URL(configuration.proxyUrl ?? "");
  if (
    !["http:", "https:"].includes(endpoint.protocol) ||
    endpoint.username ||
    endpoint.password ||
    endpoint.pathname !== "/" ||
    endpoint.search ||
    endpoint.hash
  )
    throw new Error("Use an HTTP or HTTPS proxy address without embedded credentials or a path.");
  return endpoint;
}

function cancelledDownload(signal: AbortSignal | null | undefined): Error {
  return signal?.reason instanceof Error
    ? signal.reason
    : new DOMException("Plugin download cancelled.", "AbortError");
}

/** Only the dedicated, nonpersistent plugin-download session is configured here. */
export function createPluginNetworkTransport(
  port: PluginNativeNetworkPort,
): PluginNetworkTransport {
  const owned = new Map<ClientRequest, () => void>();
  let configuration: PluginNetworkTransportConfiguration = { mode: "system" };
  let ready = true;
  let closed = false;

  function abortAll(): void {
    for (const cancel of [...owned.values()]) cancel();
  }

  const fetcher: typeof fetch = (input, init): Promise<Response> => {
    if (closed || !ready)
      return Promise.reject(
        new Error("Plugin networking is being reconfigured. Retry when settings have been saved."),
      );
    const url = input instanceof Request ? input.url : String(input);
    const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
    if (signal?.aborted) return Promise.reject(cancelledDownload(signal));
    const endpoint = proxyEndpoint(configuration);
    const credentials = configuration.credentials;
    const method = init?.method ?? (input instanceof Request ? input.method : "GET");
    if (!["GET", "HEAD"].includes(method))
      return Promise.reject(
        new Error("Plugin networking only supports catalog and package reads."),
      );
    const requestHeaders = new Headers(
      init?.headers ?? (input instanceof Request ? input.headers : undefined),
    );
    if (requestHeaders.has("authorization") || requestHeaders.has("proxy-authorization"))
      return Promise.reject(
        new Error("Plugin requests cannot supply origin or proxy authorization headers."),
      );
    return new Promise<Response>((resolve, reject) => {
      let request: ClientRequest;
      let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
      let settled = false;
      let finished = false;
      let received = 0;
      let challenged = false;
      const abortError = (): Error => cancelledDownload(signal);
      const finish = (): void => {
        if (finished) return;
        finished = true;
        signal?.removeEventListener("abort", abort);
        owned.delete(request);
      };
      const fail = (error: Error): void => {
        if (finished) return;
        finish();
        if (!settled) {
          settled = true;
          reject(error);
        } else if (controller !== undefined) controller.error(error);
      };
      const abort = (): void => {
        fail(abortError());
        request.abort();
      };
      try {
        request = port.request({
          url,
          method,
          session: port.session as Session,
          redirect: "manual",
          cache: "no-store",
          headers: Object.fromEntries(requestHeaders),
          useSessionCookies: false,
        });
      } catch (error) {
        reject(networkFailure(error));
        return;
      }
      owned.set(request, abort);
      signal?.addEventListener("abort", abort, { once: true });
      request.on("login", (info, answer) => {
        const host = endpoint?.hostname.replace(/^\[|\]$/gu, "").toLowerCase();
        const portNumber =
          endpoint === undefined
            ? undefined
            : Number(endpoint.port || (endpoint.protocol === "https:" ? "443" : "80"));
        if (
          !challenged &&
          !finished &&
          info.isProxy &&
          endpoint !== undefined &&
          credentials !== undefined &&
          info.host.toLowerCase() === host &&
          info.port === portNumber
        ) {
          challenged = true;
          answer(credentials.username, credentials.password);
        } else {
          answer();
          if (info.isProxy) {
            fail(
              new Error(
                "Plugin proxy authentication failed. Check the proxy credentials or authentication method.",
              ),
            );
            request.abort();
          }
        }
      });
      request.on("error", (error) => fail(networkFailure(error)));
      request.on("abort", () => fail(abortError()));
      // Chromium can publish close before its actionable error. Response end/error,
      // request error/abort and the acquisition owner's deadline settle this request.
      request.on("redirect", (status, _method, destination, values) => {
        if (finished || settled) return;
        try {
          const headers = headersFromNative(values);
          headers.set("location", destination);
          const response = new Response(null, { status, headers });
          settled = true;
          finish();
          resolve(response);
        } catch {
          fail(new Error("The plugin server returned an invalid redirect."));
        }
        request.abort();
      });
      request.on("response", (response: IncomingMessage) => {
        if (finished) return;
        const status = response.statusCode;
        let headers: Headers;
        try {
          headers = headersFromNative(response.headers);
        } catch {
          fail(new Error("The plugin server returned invalid response headers."));
          request.abort();
          return;
        }
        const noBody = method === "HEAD" || [204, 205, 304].includes(status);
        // IncomingMessage is an event emitter, not a pausable Node readable. Bound all
        // received bytes even if the consumer is slow; catalog enforces smaller limits.
        const body = noBody
          ? null
          : new ReadableStream<Uint8Array>({
              start(value): void {
                controller = value;
              },
              cancel(): void {
                finish();
                request.abort();
              },
            });
        response.on("error", (error) => fail(networkFailure(error)));
        response.on("aborted", () => fail(abortError()));
        response.on("end", () => {
          if (finished) return;
          finish();
          controller?.close();
        });
        response.on("data", (chunk: Buffer) => {
          if (finished) return;
          received += chunk.byteLength;
          if (received > MAX_PLUGIN_ARCHIVE_BYTES) {
            fail(new Error("Plugin download exceeds its size limit."));
            request.abort();
          } else if (!noBody) controller?.enqueue(new Uint8Array(chunk));
        });
        try {
          const result = new Response(body, { status, headers });
          settled = true;
          resolve(result);
        } catch {
          fail(new Error("The plugin server returned an invalid response."));
          request.abort();
        }
      });
      if (signal?.aborted) abort();
      else request.end();
    });
  };
  return {
    nativeAvailable: true,
    supportedProxyProtocols: ["http", "https"],
    fetch: fetcher,
    async configure(next): Promise<void> {
      if (closed) throw new Error("Plugin networking has shut down.");
      const endpoint = proxyEndpoint(next);
      ready = false;
      abortAll();
      configuration = { mode: "system" };
      try {
        await port.session.closeAllConnections();
        if (closed) throw new Error("Plugin networking has shut down.");
        await port.session.clearAuthCache();
        if (closed) throw new Error("Plugin networking has shut down.");
        await port.session.setProxy(
          endpoint === undefined
            ? { mode: "system" }
            : {
                mode: "fixed_servers",
                proxyRules: endpoint.origin,
                proxyBypassRules: "<-loopback>",
              },
        );
        if (closed) throw new Error("Plugin networking has shut down.");
        configuration = {
          ...next,
          ...(next.credentials === undefined ? {} : { credentials: { ...next.credentials } }),
        };
        ready = true;
      } catch {
        throw new Error(
          "Plugin proxy settings could not be applied. Restore system settings or install a signed file.",
        );
      }
    },
    async close(): Promise<void> {
      closed = true;
      ready = false;
      abortAll();
      configuration = { mode: "system" };
      await port.session.closeAllConnections();
      await port.session.clearAuthCache();
    },
  };
}
