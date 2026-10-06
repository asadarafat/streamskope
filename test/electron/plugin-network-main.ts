import { X509Certificate } from "node:crypto";
import { readFile } from "node:fs/promises";

import { app, net, session } from "electron";

import { createPluginNetworkTransport } from "../../src/platform/electron/main/plugin-network-transport";
import type { PluginNetworkTransportConfiguration } from "../../src/platform/node/plugins/network-transport";

void app.whenReady().then(async () => {
  const certificatePath = process.env.STREAMSKOPE_PLUGIN_PROXY_TEST_CERTIFICATE;
  if (certificatePath === undefined)
    throw new Error("Native network fixture certificate is required.");
  const certificate = new X509Certificate(await readFile(certificatePath));
  const downloads = session.fromPartition("streamskope-plugin-proxy-fixture", { cache: false });
  // Only this test session accepts the exact generated loopback certificate.
  // Production never installs a certificate callback or disables TLS validation.
  downloads.setCertificateVerifyProc((request, answer) => {
    answer(
      request.hostname === "127.0.0.1" &&
        new X509Certificate(request.certificate.data).fingerprint256 === certificate.fingerprint256
        ? 0
        : -3,
    );
  });
  const transport = createPluginNetworkTransport({
    session: downloads,
    request: (options) => {
      const request = net.request(options);
      const log = (event: string): void => {
        process.stderr.write(`plugin-proxy-fixture: ${event}\n`);
      };
      request.on("login", (details) =>
        log(details.isProxy ? "proxy challenge" : "origin challenge"),
      );
      request.on("error", (error) =>
        log(`request error ${error.message.match(/net::[A-Z_]+/u)?.[0] ?? "unspecified"}`),
      );
      request.on("close", () => log("request close"));
      request.on("abort", () => log("request abort"));
      request.on("finish", () => log("request finish"));
      request.on("response", (response) => {
        log(`response ${response.statusCode}`);
        response.on("end", () => log("response end"));
        response.on("aborted", () => log("response aborted"));
        response.on("error", () => log("response error"));
      });
      return request;
    },
  });
  Object.assign(globalThis, {
    pluginNetworkFixture: {
      configure: (input: PluginNetworkTransportConfiguration): Promise<void> =>
        transport.configure(input),
      close: (): Promise<void> => transport.close(),
      async fetch(
        url: string,
        cancelAfterResponse = false,
      ): Promise<{
        status?: number;
        body?: string;
        location?: string | null;
        error?: string;
        name?: string;
      }> {
        const controller = new AbortController();
        try {
          const response = await transport.fetch(url, {
            signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10_000)]),
          });
          if (cancelAfterResponse) controller.abort();
          return {
            status: response.status,
            body: await response.text(),
            location: response.headers.get("location"),
          };
        } catch (error) {
          return {
            error: error instanceof Error ? error.message : "Unknown fixture failure.",
            name: error instanceof Error ? error.name : "Error",
          };
        }
      },
      async defaultProxy(url: string): Promise<string> {
        return session.defaultSession.resolveProxy(url);
      },
    },
  });
});
