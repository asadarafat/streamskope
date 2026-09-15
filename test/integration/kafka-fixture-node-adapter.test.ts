import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { FileFixtureOwnershipStore } from "../../tools/kafka-fixture/file-ownership-store";
import { NodeFixtureRuntime } from "../../tools/kafka-fixture/node-runtime";
import type { OwnedFixtureRecord } from "../../tools/kafka-fixture/lifecycle";

const temporaryDirectories: string[] = [];
const openServers: Server[] = [];

async function createTemporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-fixture-test-"));
  temporaryDirectories.push(directory);
  return directory;
}

async function listenOnRandomPort(): Promise<{ readonly port: number; readonly server: Server }> {
  const server = createServer();
  openServers.push(server);

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });

  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Expected a TCP address.");
  }

  return { port: address.port, server };
}

afterEach(async () => {
  vi.unstubAllEnvs();
  for (const server of openServers.splice(0)) {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  }

  for (const directory of temporaryDirectories.splice(0)) {
    await rm(directory, { force: true, recursive: true });
  }
});

describe("file fixture ownership store", () => {
  it("round-trips a private record and removes it explicitly", async () => {
    const directory = await createTemporaryDirectory();
    const store = new FileFixtureOwnershipStore(directory);
    const record: OwnedFixtureRecord = {
      caPath: "/fixture/ca.pem",
      kafkaPort: 19_093,
      name: "streamskope-owned-test",
      oauthImage: "streamskope-aio-kafka-oauth:1.0.0",
      oauthPort: 15_000,
      ownership: "owned",
      schemaRegistryImage:
        "ghcr.io/aiven-open/karapace:5.0.3@sha256:4cf3dbea61eebb6c85a5198ea8f0b6c32ca3e7fe330cf3b9843cf3837fcdb868",
      schemaRegistryPort: 18_081,
      topologyPath: "/fixture/topology.clab.yml",
    };

    await store.save(record);

    expect(await store.load(record.name)).toEqual(record);
    const recordPath = join(directory, `${record.name}.json`);
    expect((await stat(recordPath)).mode & 0o777).toBe(0o600);
    expect(await readFile(recordPath, "utf8")).not.toContain("clientSecret");

    await store.remove(record.name);
    expect(await store.load(record.name)).toBeUndefined();
  });
});

describe("Node fixture port inspection", () => {
  it.skipIf(process.platform === "win32")(
    "resumes only stopped containers with verified ownership",
    async () => {
      const root = await createTemporaryDirectory();
      const bin = join(root, "bin");
      await mkdir(bin);
      const request: OwnedFixtureRecord = {
        name: "streamskope-owned-test",
        ownership: "owned",
        caPath: join(root, "ca.pem"),
        topologyPath: join(root, "aio-kafka", "topology.clab.yml"),
        kafkaPort: 19093,
        oauthPort: 15000,
        oauthImage: "fixture-oauth",
        schemaRegistryPort: 18081,
        schemaRegistryImage: "fixture-registry",
      };
      const lines = ["oauth", "broker", "schema-registry"].map(
        (role, index) =>
          `${String(index + 1).repeat(64)}|/clab-${request.name}-${role}|${index === 1 ? "exited" : "running"}|${request.name}|${request.topologyPath}`,
      );
      const inspection = join(root, "inspection");
      const calls = join(root, "calls");
      await writeFile(inspection, lines.join("\n"));
      await writeFile(
        join(bin, "docker"),
        `#!/usr/bin/env node
const fs = require("node:fs");
if (process.argv[2] === "inspect") process.stdout.write(fs.readFileSync(${JSON.stringify(inspection)}));
else fs.appendFileSync(${JSON.stringify(calls)}, JSON.stringify(process.argv.slice(2)) + "\\n");
`,
        { mode: 0o700 },
      );
      vi.stubEnv("PATH", `${bin}:${process.env.PATH ?? ""}`);
      const runtime = new NodeFixtureRuntime(root);
      await runtime.resumeOwned(request);
      expect(await readFile(calls, "utf8")).toBe(`${JSON.stringify(["start", "2".repeat(64)])}\n`);
      await writeFile(calls, "");
      await writeFile(inspection, lines.join("\n").replace("exited", "running"));
      await runtime.resumeOwned(request);
      expect(await readFile(calls, "utf8")).toBe("");
      await writeFile(
        inspection,
        lines.join("\n").replaceAll(request.topologyPath, "/foreign/topology.clab.yml"),
      );
      await expect(runtime.resumeOwned(request)).rejects.toThrow("ownership");
      expect(await readFile(calls, "utf8")).toBe("");
    },
  );
  it("reports a bound loopback port and releases its own probes", async () => {
    const repositoryRoot = await createTemporaryDirectory();
    const runtime = new NodeFixtureRuntime(repositoryRoot);
    const { port, server } = await listenOnRandomPort();

    expect(await runtime.findUnavailablePorts([port])).toEqual([port]);

    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }
        resolve();
      });
    });
    openServers.splice(openServers.indexOf(server), 1);

    expect(await runtime.findUnavailablePorts([port])).toEqual([]);
  });
});
