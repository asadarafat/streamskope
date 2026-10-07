import { createServer, request as httpRequest, type Server } from "node:http";

import { afterEach, describe, expect, it } from "vitest";

import { BrowserPluginFiles } from "../../src/platform/node/browser-plugin-files";

const cleanups: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function listen(
  onRequest?: (path: string) => void,
): Promise<{ readonly files: BrowserPluginFiles; readonly origin: string }> {
  const files = new BrowserPluginFiles();
  const server = createServer((request, response) => {
    void files.handleRequest(request, response).then((handled) => {
      if (!handled) {
        response.writeHead(404);
        response.end();
      }
    });
    onRequest?.(request.url ?? "");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(async () => {
    files.close();
    await close(server);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("Missing test listener.");
  return { files, origin: `http://127.0.0.1:${address.port}` };
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error === undefined ? resolve() : reject(error)));
    server.closeAllConnections();
  });
}

describe("bounded production browser plugin upload", () => {
  it("stages actual HTTP bytes for the matching typed command and supports explicit cleanup", async () => {
    const { files, origin } = await listen();
    const url = `${origin}/__streamskope_session/plugin-file/request-1`;
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: Buffer.from("signed package bytes"),
    });
    expect(response.status).toBe(204);
    await expect(
      files.run("request-1", async () =>
        Buffer.from((await files.chooseFile(new AbortController().signal))!).toString(),
      ),
    ).resolves.toBe("signed package bytes");
    expect(
      (
        await fetch(url, {
          method: "POST",
          headers: { "content-type": "application/octet-stream" },
          body: Buffer.from("second package"),
        })
      ).status,
    ).toBe(204);
    expect((await fetch(url, { method: "DELETE" })).status).toBe(204);
    await expect(
      files.run("request-1", () => files.chooseFile(new AbortController().signal)),
    ).rejects.toThrow("Select a plugin file");
    expect((await fetch(`${origin}/unrelated`)).status).toBe(404);
  });

  it("rejects wrong types, empty bodies, unsafe IDs, overwrites, and a locked runtime", async () => {
    const { files, origin } = await listen();
    const url = `${origin}/__streamskope_session/plugin-file/request`;
    const post = (body: string, contentType = "application/octet-stream"): Promise<Response> =>
      fetch(url, { method: "POST", headers: { "content-type": contentType }, body });
    expect((await post("package", "application/json")).status).toBe(415);
    expect((await post("")).status).toBe(413);
    expect((await fetch(`${url}?other=id`, { method: "DELETE" })).status).toBe(400);
    expect((await fetch(url)).status).toBe(405);
    expect((await post("original")).status).toBe(204);
    const conflict = await post("replacement");
    expect(conflict.status).toBe(409);
    expect(await conflict.json()).toEqual({
      error: { code: "PLUGIN_FILE_BUSY", summary: "This plugin file selection is already in use." },
    });
    await expect(
      files.run("request", async () =>
        Buffer.from((await files.chooseFile(new AbortController().signal))!).toString(),
      ),
    ).resolves.toBe("original");
    files.close();
    const locked = await post("package");
    expect(locked.status).toBe(503);
    expect(await locked.text()).not.toContain("package");
  });

  it("allows one in-flight upload and aborts its partial body when the vault locks", async () => {
    let started = (): void => undefined;
    const arrived = new Promise<void>((resolve) => {
      started = resolve;
    });
    const { files, origin } = await listen((path) => {
      if (path.endsWith("/slow-request")) started();
    });
    const upload = httpRequest(`${origin}/__streamskope_session/plugin-file/slow-request`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
    });
    const interrupted = new Promise<Error>((resolve) => upload.once("error", resolve));
    upload.write("partial archive");
    await arrived;
    const second = await fetch(`${origin}/__streamskope_session/plugin-file/other-request`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: "other archive",
    });
    expect(second.status).toBe(409);
    files.close();
    expect(await interrupted).toBeInstanceOf(Error);
    await expect(
      files.run("slow-request", () => files.chooseFile(new AbortController().signal)),
    ).rejects.toThrow("Unlock");
  });
});
