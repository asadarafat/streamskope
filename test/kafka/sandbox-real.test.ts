import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { randomUUID } from "node:crypto";

import { expect, it } from "vitest";

import { sandboxMain } from "../../tools/sandbox";
import { readCliJson, runReadOnlyCli } from "../../src/platform/node/read-only-cli";

const execute = promisify(execFile);
async function docker(args: string[]): Promise<string> {
  return (await execute("docker", args, { timeout: 180000 })).stdout.trim();
}
it("reuses deterministic seeds, transforms bounded records, resets only owned data and refuses a foreign network", async () => {
  const project = "streamskope-sandbox";
  if (await docker(["network", "ls", "-q", "--filter", `name=^${project}_default$`]))
    throw new Error("Existing sandbox network; test refuses to change it.");
  for (const args of [
    ["ps", "-aq"],
    ["network", "ls", "-q"],
    ["volume", "ls", "-q"],
  ]) {
    if (await docker([...args, "--filter", `label=com.docker.compose.project=${project}`]))
      throw new Error("Existing sandbox resources; test refuses to change them.");
  }
  let foreign: string | undefined;
  const previous = process.env.STREAMSKOPE_SANDBOX_INSTANCE;
  const query = async (topic: string): Promise<unknown[]> => {
    const results: unknown[] = [];
    await runReadOnlyCli(
      "query",
      await readCliJson(".artifacts/sandbox/connection.json", true),
      { topic, mode: "earliest", maxMessages: 100 },
      {
        write: (value): Promise<void> => {
          results.push(value);
          return Promise.resolve();
        },
      },
      AbortSignal.timeout(15000),
    );
    return results;
  };
  try {
    await sandboxMain(["up"]);
    const first = await query("sandbox.events");
    expect(first).toHaveLength(11);
    expect(first[0]).toMatchObject({
      kind: "record",
      record: {
        offset: "0",
        key: "event-1",
        payload: '{"eventId":1,"seed":1,"kind":"sandbox","value":10}',
      },
    });
    await sandboxMain(["up"]);
    expect(await query("sandbox.events")).toEqual(first);
    const ids = (
      await docker(["ps", "-q", "--filter", `label=com.docker.compose.project=${project}`])
    ).split(/\s+/u);
    expect(ids).toHaveLength(2);
    for (const id of ids) {
      const info = JSON.parse(await docker(["inspect", id])) as {
        HostConfig: {
          Memory: number;
          NanoCpus: number;
          PortBindings: Record<string, { HostIp: string }[]>;
        };
      }[];
      expect(info[0]!.HostConfig.Memory).toBeLessThanOrEqual(768 * 1024 * 1024);
      expect(info[0]!.HostConfig.NanoCpus).toBe(1_000_000_000);
      expect(
        Object.values(info[0]!.HostConfig.PortBindings)
          .flat()
          .every((port) => port.HostIp === "127.0.0.1"),
      ).toBe(true);
    }
    await sandboxMain(["consume"]);
    await sandboxMain(["transform"]);
    expect(await query("sandbox.processed")).toHaveLength(11);
    await sandboxMain(["transform"]);
    expect(await query("sandbox.processed")).toHaveLength(21);
    await sandboxMain(["down"]);
    expect(
      await docker(["ps", "-aq", "--filter", `label=com.docker.compose.project=${project}`]),
    ).toBe("");
    await sandboxMain(["up"]);
    expect(await query("sandbox.events")).toHaveLength(11);
    expect(await query("sandbox.processed")).toHaveLength(1);
    await sandboxMain(["down"]);
    const foreignName = `streamskope-foreign-${randomUUID()}`;
    foreign = await docker([
      "network",
      "create",
      "--label",
      `com.docker.compose.project=${project}`,
      foreignName,
    ]);
    await expect(sandboxMain(["down"])).rejects.toThrow("unowned resources");
    expect(await docker(["network", "inspect", "--format", "{{.Name}}", foreign])).toBe(
      foreignName,
    );
    await docker(["network", "rm", foreign]);
    foreign = undefined;
    foreign = await docker(["network", "create", `${project}_default`]);
    await expect(sandboxMain(["up"])).rejects.toThrow("unowned resources");
    await expect(sandboxMain(["down"])).rejects.toThrow("unowned resources");
    expect(await docker(["network", "inspect", "--format", "{{.Name}}", foreign])).toBe(
      `${project}_default`,
    );
  } finally {
    // Only the network ID returned by this test's successful creation is removable here.
    if (foreign) await docker(["network", "rm", foreign]);
    await sandboxMain(["down"]);
    if (previous === undefined) delete process.env.STREAMSKOPE_SANDBOX_INSTANCE;
    else process.env.STREAMSKOPE_SANDBOX_INSTANCE = previous;
  }
}, 360000);
