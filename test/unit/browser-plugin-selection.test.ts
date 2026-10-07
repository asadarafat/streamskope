// @vitest-environment jsdom
// @vitest-environment-options {"url":"http://127.0.0.1/"}

import { afterEach, describe, expect, it, vi, type MockInstance } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostCommand } from "../../src/features/kafka/contracts";
import {
  createBrowserDevelopmentHost,
  resolveStreamSkopeHost,
} from "../../src/platform/electron/renderer/host";

const command = {
  command: "plugins.package.inspect",
  id: "portable-request",
  payload: { source: "file" },
  version: HOST_PROTOCOL_VERSION,
} satisfies HostCommand;

function response(): Response {
  return new Response(
    JSON.stringify({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: "test-inspection", pluginPackage: null },
      version: command.version,
    }),
    { headers: { "content-type": "application/json" } },
  );
}

function select(file: File | null): MockInstance<() => void> {
  return vi.spyOn(HTMLInputElement.prototype, "click").mockImplementation(function (
    this: HTMLInputElement,
  ): void {
    if (file === null) {
      this.dispatchEvent(new Event("cancel"));
      return;
    }
    Object.defineProperty(this, "files", { value: [file] });
    this.dispatchEvent(new Event("change"));
  });
}

function url(input: RequestInfo | URL): string {
  return typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
}

afterEach(() => {
  vi.unstubAllGlobals();
  delete window.streamSkopeBrowserRuntime;
  delete window.streamSkopeHost;
  document.querySelectorAll('input[type="file"]').forEach((input) => input.remove());
});

describe("production browser portable plugin selection", () => {
  it("uploads a chosen archive, dispatches its existing typed command, and discards staged bytes", async () => {
    window.streamSkopeBrowserRuntime = { pluginFileUpload: true };
    const file = new File(["portable archive"], "eda.skope-plugin", {
      type: "application/octet-stream",
    });
    const click = select(file);
    const fetchMock = vi.fn<typeof fetch>((input) =>
      Promise.resolve(
        url(input).endsWith("/commands") ? response() : new Response(null, { status: 204 }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserDevelopmentHost(window);
    expect(await host.execute(command)).toMatchObject({
      ok: true,
      result: { pluginPackage: null },
    });
    expect(click).toHaveBeenCalledOnce();
    expect(fetchMock.mock.calls.map(([input, init]) => [url(input), init?.method])).toEqual([
      ["http://127.0.0.1/__streamskope_session/plugin-file/portable-request", "POST"],
      ["http://127.0.0.1/__streamskope_host/commands", "POST"],
      ["http://127.0.0.1/__streamskope_session/plugin-file/portable-request", "DELETE"],
    ]);
    expect(fetchMock.mock.calls[0]?.[1]).toMatchObject({
      body: file,
      credentials: "same-origin",
      headers: { "content-type": "application/octet-stream" },
      mode: "same-origin",
    });
    expect(JSON.parse(fetchMock.mock.calls[1]![1]!.body as string)).toEqual(command);
    expect(document.querySelector('input[type="file"]')).toBeNull();
  });

  it("cancels without uploading or executing a host command", async () => {
    window.streamSkopeBrowserRuntime = { pluginFileUpload: true };
    select(null);
    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);
    expect(await createBrowserDevelopmentHost(window).execute(command)).toMatchObject({
      command: command.command,
      id: command.id,
      ok: true,
      result: { pluginPackage: null },
    });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(document.querySelector('input[type="file"]')).toBeNull();
  });

  it("keeps development browser commands and native host selection unchanged", async () => {
    const click = vi.spyOn(HTMLInputElement.prototype, "click");
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(response());
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserDevelopmentHost(window);
    await host.execute(command);
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith(
      "http://127.0.0.1/__streamskope_host/commands",
      expect.objectContaining({ method: "POST" }),
    );
    expect(JSON.parse(fetchMock.mock.calls[0]![1]!.body as string)).toEqual(command);
    expect(click).not.toHaveBeenCalled();
    window.streamSkopeBrowserRuntime = { pluginFileUpload: true };
    window.streamSkopeHost = host;
    expect(resolveStreamSkopeHost(window)).toBe(host);
  });

  it("rejects empty or oversized selections before transmitting them", async () => {
    window.streamSkopeBrowserRuntime = { pluginFileUpload: true };
    const file = new File([], "empty.skope-plugin");
    select(file);
    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);
    await expect(createBrowserDevelopmentHost(window).execute(command)).rejects.toThrow(
      "between 1 byte and 48 MiB",
    );
    Object.defineProperty(file, "size", { value: 48 * 1024 * 1024 + 1 });
    await expect(createBrowserDevelopmentHost(window).execute(command)).rejects.toThrow(
      "between 1 byte and 48 MiB",
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("cleans up a staged file when command dispatch fails and never dispatches a rejected upload", async () => {
    window.streamSkopeBrowserRuntime = { pluginFileUpload: true };
    select(new File(["archive"], "nsp.skope-plugin"));
    const fetchMock = vi.fn<typeof fetch>((input) =>
      Promise.resolve(new Response(null, { status: url(input).endsWith("/commands") ? 503 : 204 })),
    );
    vi.stubGlobal("fetch", fetchMock);
    await expect(createBrowserDevelopmentHost(window).execute(command)).rejects.toThrow("HTTP 503");
    expect(fetchMock.mock.calls.at(-1)?.[1]?.method).toBe("DELETE");
    fetchMock.mockReset().mockResolvedValue(new Response(null, { status: 413 }));
    await expect(createBrowserDevelopmentHost(window).execute(command)).rejects.toThrow("HTTP 413");
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(url(fetchMock.mock.calls[0]![0])).not.toContain("/commands");
    expect(fetchMock.mock.calls[1]?.[1]?.method).toBe("DELETE");
  });

  it("cancels an in-flight upload and prevents a late inspection from reaching the host", async () => {
    window.streamSkopeBrowserRuntime = { pluginFileUpload: true };
    select(new File(["archive"], "eda.skope-plugin"));
    let uploadStarted = (): void => undefined;
    const uploaded = new Promise<void>((resolve) => {
      uploadStarted = resolve;
    });
    const fetchMock = vi.fn<typeof fetch>((input, init) => {
      if (url(input).includes("/plugin-file/") && init?.method === "POST") {
        uploadStarted();
        return new Promise<Response>((_resolve, reject) => {
          init.signal!.addEventListener("abort", () => reject(new Error("upload canceled")), {
            once: true,
          });
        });
      }
      if (url(input).endsWith("/commands")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              command: "plugins.network.cancel",
              id: "cancel-request",
              ok: true,
              result: { correlationId: "test-cancel" },
              version: HOST_PROTOCOL_VERSION,
            }),
          ),
        );
      }
      return Promise.resolve(new Response(null, { status: 204 }));
    });
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserDevelopmentHost(window);
    const inspection = host.execute(command);
    await uploaded;
    await host.execute({
      command: "plugins.network.cancel",
      id: "cancel-request",
      payload: { requestId: command.id },
      version: HOST_PROTOCOL_VERSION,
    });
    await expect(inspection).resolves.toMatchObject({ result: { pluginPackage: null } });
    expect(fetchMock.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
    expect(fetchMock.mock.calls.filter(([input]) => url(input).endsWith("/commands"))).toHaveLength(
      1,
    );
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === "DELETE")).toBe(true);
  });

  it("uses the exact HTTPS renderer origin for upload and commands", async () => {
    const file = new File(["archive"], "nsp.skope-plugin");
    select(file);
    const fetchMock = vi.fn<typeof fetch>((input) =>
      Promise.resolve(
        url(input).endsWith("/commands") ? response() : new Response(null, { status: 204 }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const host = createBrowserDevelopmentHost({
      document,
      location: { origin: "https://streamskope.example.test" } as Location,
      open: window.open.bind(window),
      streamSkopeBrowserRuntime: { pluginFileUpload: true },
    });
    await host.execute(command);
    expect(
      fetchMock.mock.calls.every(([input]) =>
        url(input).startsWith("https://streamskope.example.test/"),
      ),
    ).toBe(true);
    expect(() =>
      createBrowserDevelopmentHost({
        location: { origin: "https://streamskope.example.test/path" } as Location,
        open: window.open.bind(window),
      }),
    ).toThrow("one exact HTTP or HTTPS origin");
  });
});
