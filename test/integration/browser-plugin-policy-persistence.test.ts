import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseCorrelatedHostResponse,
  type HostCommand,
  type HostCommandResponse,
} from "../../src/features/kafka/contracts";
import type { PluginNetworkSnapshot } from "../../src/plugins/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import type { WebGatewayRuntime } from "../../src/platform/node/web-gateway";

const passphrase = "independent browser offline fixture passphrase";
const roots: string[] = [];
const runtimes: WebGatewayRuntime[] = [];
let nextId = 0;

afterEach(async () => {
  await Promise.all(runtimes.splice(0).map((runtime) => runtime.lock()));
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function runtime(dataRoot: string, mode: "create" | "unlock"): Promise<WebGatewayRuntime> {
  const opened = await openBrowserRuntime(dataRoot, passphrase, mode);
  runtimes.push(opened);
  return opened;
}

async function invoke<Command extends HostCommand>(
  owner: WebGatewayRuntime,
  command: Command,
): Promise<HostCommandResponse<Command["command"]>> {
  const endpoint = owner.providers.get("kafka");
  if (endpoint === undefined) throw new Error("Expected the real production Kafka provider.");
  return parseCorrelatedHostResponse(await endpoint.dispatch(command), command);
}

function identity(): { id: string; version: typeof HOST_PROTOCOL_VERSION } {
  return { id: `offline-policy-${++nextId}`, version: HOST_PROTOCOL_VERSION };
}

async function network(owner: WebGatewayRuntime): Promise<PluginNetworkSnapshot> {
  const response = await invoke(owner, {
    ...identity(),
    command: "plugins.network.get",
    payload: {},
  });
  if (!response.ok) throw new Error("The real host did not return download settings.");
  return response.result.pluginNetwork;
}

async function saveOffline(owner: WebGatewayRuntime): Promise<PluginNetworkSnapshot> {
  const response = await invoke(owner, {
    ...identity(),
    command: "plugins.network.update",
    payload: {
      configuration: { mode: "system", proxyUrl: null, offline: true },
      credentials: { action: "clear" },
    },
  });
  if (!response.ok) throw new Error("The real host did not save Offline mode.");
  return response.result.pluginNetwork;
}

async function expectRemoteBlocked(owner: WebGatewayRuntime, reason: RegExp): Promise<void> {
  const catalog = await invoke(owner, {
    ...identity(),
    command: "plugins.catalog",
    payload: { refresh: true },
  });
  if (!catalog.ok) throw new Error("Expected the preserved catalog/failure response.");
  expect(catalog.result.pluginCatalog.error).toMatch(reason);
  const requests: HostCommand[] = [
    { ...identity(), command: "plugins.network.test", payload: {} },
    {
      ...identity(),
      command: "plugins.package.inspect",
      payload: {
        source: "catalog",
        pluginId: "streamskope.eda",
        version: "0.1.1",
        sha256: "a".repeat(64),
      },
    },
    { ...identity(), command: "plugins.install", payload: { pluginId: "streamskope.eda" } },
  ];
  for (const request of requests) {
    const response = await invoke(owner, request);
    if (response.ok) throw new Error("A remote plugin path bypassed saved download policy.");
    expect(response.error.summary).toMatch(reason);
  }
  const local = await invoke(owner, {
    ...identity(),
    command: "plugins.delivery",
    payload: {},
  });
  expect(local).toMatchObject({
    ok: true,
    result: { pluginDelivery: { fileInstallationAvailable: true } },
  });
}

describe.skipIf(process.platform !== "linux")(
  "production browser's durable plugin download policy",
  () => {
    it("restores Offline mode after a real vault/provider restart without inventing native proxy support", async () => {
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockRejectedValue(new Error("Unexpected public network request"));
      const root = await mkdtemp(join(tmpdir(), "streamskope-browser-offline-"));
      roots.push(root);
      const first = await runtime(root, "create");
      const saved = await saveOffline(first);
      expect(saved).toMatchObject({
        configuration: { mode: "system", proxyUrl: null, offline: true },
        nativeAvailable: false,
        supportedProxyProtocols: [],
        credentialStorage: "unavailable",
      });
      const path = join(root, "plugins", "network.json");
      const bytes = await readFile(path);
      expect((await stat(path)).mode & 0o777).toBe(0o600);
      expect((await stat(join(path, ".."))).mode & 0o777).toBe(0o700);
      expect(bytes.toString("utf8")).not.toContain(passphrase);
      await first.lock();
      const reopened = await runtime(root, "unlock");
      expect(await network(reopened)).toEqual(saved);
      await expectRemoteBlocked(reopened, /offline/u);
      expect(await readFile(path)).toEqual(bytes);
      expect(fetch).not.toHaveBeenCalled();
    });

    it.each([
      ["truncated", '{"configuration":'],
      [
        "unsupported-version",
        JSON.stringify({
          formatVersion: 2,
          revision: 1,
          configuration: { mode: "system", proxyUrl: null, offline: true },
        }),
      ],
      [
        "unknown-field",
        JSON.stringify({
          formatVersion: 1,
          revision: 1,
          configuration: { mode: "system", proxyUrl: null, offline: true },
          unrecognized: true,
        }),
      ],
      [
        "unsupported-proxy",
        JSON.stringify({
          formatVersion: 1,
          revision: 1,
          configuration: {
            mode: "custom",
            proxyUrl: "https://proxy.example.test:8443",
            offline: false,
          },
        }),
      ],
    ])(
      "preserves %s saved policy and blocks all remote paths until an explicit valid reset",
      async (_kind, bad) => {
        const fetch = vi
          .spyOn(globalThis, "fetch")
          .mockRejectedValue(new Error("Unexpected public network request"));
        const root = await mkdtemp(join(tmpdir(), "streamskope-browser-policy-corrupt-"));
        roots.push(root);
        const first = await runtime(root, "create");
        await saveOffline(first);
        await first.lock();
        const path = join(root, "plugins", "network.json");
        await writeFile(path, bad, { mode: 0o600 });
        const reopened = await runtime(root, "unlock");
        expect(await network(reopened)).toMatchObject({
          configuration: null,
          error: expect.stringContaining("could not be read or applied") as unknown,
          nativeAvailable: false,
          supportedProxyProtocols: [],
        });
        await expectRemoteBlocked(reopened, /need attention/u);
        expect(await readFile(path, "utf8")).toBe(bad);
        expect(fetch).not.toHaveBeenCalled();
        const reset = await saveOffline(reopened);
        expect(reset.configuration?.offline).toBe(true);
        expect(reset.error).toBeUndefined();
        expect(await readFile(path, "utf8")).not.toBe(bad);
        await expectRemoteBlocked(reopened, /offline/u);
        expect(fetch).not.toHaveBeenCalled();
      },
    );
  },
);
