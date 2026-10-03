import { mkdtemp, writeFile, chmod, symlink, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { once } from "node:events";

import { afterEach, expect, it, vi } from "vitest";

import {
  readCliJson,
  parseCliConfiguration,
  parseCliQuery,
} from "../../src/platform/node/read-only-cli";
import { cliMain } from "../../tools/cli";

const config = {
  connection: { name: "CLI", brokers: ["127.0.0.1:1"], tls: { enabled: false } },
  protection: { readOnly: false, maskKey: true, maskHeaders: ["secret"], valuePaths: ["/private"] },
};
afterEach(() => vi.restoreAllMocks());
it("requires bounded private non-symlink configuration and finite shared query contracts", async () => {
  const folder = await mkdtemp(join(tmpdir(), "streamskope-cli-input-"));
  try {
    const path = join(folder, "private.json");
    await writeFile(path, JSON.stringify(config), { mode: 0o600 });
    expect(parseCliConfiguration(await readCliJson(path, true)).protection.readOnly).toBe(true);
    expect(() => parseCliConfiguration({ ...config, password: "unexpected" })).toThrow();
    expect(() =>
      parseCliQuery("query", { topic: "events", mode: "tail", maxMessages: 1 }, config.protection),
    ).toThrow();
    expect(() =>
      parseCliQuery(
        "query",
        { topic: "events", mode: "earliest", maxMessages: 1001 },
        config.protection,
      ),
    ).toThrow();
    expect(() =>
      parseCliQuery(
        "query",
        {
          topic: "events",
          mode: "earliest",
          maxMessages: 10,
          search: { key: "", value: "hidden", offset: "", timestamp: "", partition: null },
        },
        config.protection,
      ),
    ).toThrow("masking");
    await symlink(path, join(folder, "link.json"));
    await expect(readCliJson(join(folder, "link.json"), true)).rejects.toThrow();
    if (process.platform !== "win32") {
      await chmod(path, 0o644);
      await expect(readCliJson(path, true)).rejects.toThrow();
    }
    await writeFile(path, "x".repeat(1048577), { mode: 0o600 });
    await expect(readCliJson(path)).rejects.toThrow();
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
it("returns machine-readable usage errors and never overwrites an existing export", async () => {
  const folder = await mkdtemp(join(tmpdir(), "streamskope-cli-output-"));
  const errors: string[] = [];
  vi.spyOn(process.stderr, "write").mockImplementation((chunk) => {
    errors.push(String(chunk));
    return true;
  });
  try {
    const privatePath = join(folder, "config.json"),
      query = join(folder, "query.json"),
      output = join(folder, "records.ndjson");
    await writeFile(privatePath, JSON.stringify(config), { mode: 0o600 });
    await writeFile(query, JSON.stringify({ topic: "events", mode: "earliest", maxMessages: 10 }));
    await writeFile(output, "owned by user");
    expect(await cliMain(["delete", "--config", privatePath])).toBe(2);
    expect(await cliMain(["inspect", "--config", privatePath, "--query", query])).toBe(2);
    expect(
      await cliMain(["export", "--config", privatePath, "--query", query, "--output", output]),
    ).toBe(2);
    expect(await readFile(output, "utf8")).toBe("owned by user");
    expect(errors.map((line) => JSON.parse(line) as unknown)).toEqual(
      Array.from({ length: 3 }, (): unknown =>
        expect.objectContaining({
          format: "streamskope.cli/v1",
          kind: "error",
          code: "INVALID_INPUT",
        }),
      ),
    );
    await writeFile(
      query,
      JSON.stringify({
        topic: "events",
        mode: "earliest",
        maxMessages: 10,
        search: { key: "", value: "do-not-echo", offset: "", timestamp: "", partition: null },
      }),
    );
    expect(await cliMain(["query", "--config", privatePath, "--query", query])).toBe(2);
    expect(errors.at(-1)).toContain("masking");
    await writeFile(privatePath, JSON.stringify({ secret: "do-not-echo" }), { mode: 0o600 });
    expect(await cliMain(["inspect", "--config", privatePath])).toBe(2);
    expect(errors.join("")).not.toContain("do-not-echo");
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
it("cancels an in-flight broker handshake with exit 130 and closes its socket", async () => {
  const folder = await mkdtemp(join(tmpdir(), "streamskope-cli-cancel-"));
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("No port");
  let child: ReturnType<typeof spawn> | undefined;
  let socket: import("node:net").Socket | undefined;
  try {
    const path = join(folder, "config.json");
    await writeFile(
      path,
      JSON.stringify({
        ...config,
        connection: { ...config.connection, brokers: [`127.0.0.1:${address.port}`] },
      }),
      { mode: 0o600 },
    );
    const accepted = once(server, "connection");
    child = spawn(
      process.execPath,
      ["--import", "tsx", "tools/cli.ts", "inspect", "--config", path],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    let stderr = "";
    child.stderr!.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    const exited = once(child, "exit");
    [socket] = (await accepted) as [import("node:net").Socket];
    socket.resume();
    const closed = once(socket, "close");
    child.kill("SIGINT");
    expect((await exited)[0]).toBe(130);
    await closed;
    expect(JSON.parse(stderr) as unknown).toMatchObject({ code: "CANCELLED" });
  } finally {
    child?.kill("SIGKILL");
    socket?.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(folder, { recursive: true, force: true });
  }
});
