import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

const electronMock = vi.hoisted(() => {
  let handler: ((request: Request) => Promise<Response> | Response) | undefined;
  return {
    handled: false,
    privileges: vi.fn(),
    protocol: {
      handle(
        _scheme: string,
        nextHandler: (request: Request) => Promise<Response> | Response,
      ): void {
        handler = nextHandler;
      },
      isProtocolHandled(): boolean {
        return electronMock.handled;
      },
    },
    request(request: Request): Promise<Response> {
      if (handler === undefined) {
        return Promise.reject(new Error("Expected a packaged renderer handler."));
      }
      return Promise.resolve(handler(request));
    },
    reset(): void {
      handler = undefined;
      electronMock.handled = false;
      electronMock.privileges.mockReset();
    },
  };
});

vi.mock("electron", () => ({
  protocol: {
    ...electronMock.protocol,
    registerSchemesAsPrivileged: electronMock.privileges,
  },
}));

import {
  PACKAGED_RENDERER_SCHEME,
  PACKAGED_RENDERER_URL,
  installPackagedRendererProtocol,
  registerPackagedRendererScheme,
} from "../../src/platform/electron/main/packaged-renderer-protocol";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  electronMock.reset();
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("packaged renderer protocol", () => {
  it("registers one standard secure application scheme without bypassing CSP", () => {
    registerPackagedRendererScheme();

    expect(PACKAGED_RENDERER_SCHEME).toBe("streamskope");
    expect(PACKAGED_RENDERER_URL).toBe("streamskope://app/");
    expect(electronMock.privileges).toHaveBeenCalledWith([
      {
        privileges: {
          codeCache: true,
          secure: true,
          standard: true,
          supportFetchAPI: true,
        },
        scheme: "streamskope",
      },
    ]);
  });

  it("serves only GET requests for files beneath the exact renderer root", async () => {
    const rendererRoot = await mkdtemp(join(tmpdir(), "streamskope-renderer-"));
    temporaryDirectories.push(rendererRoot);
    await mkdir(join(rendererRoot, "assets"));
    await writeFile(join(rendererRoot, "index.html"), "<main>StreamSkope</main>", "utf8");
    await writeFile(join(rendererRoot, "assets", "index.js"), "export {};", "utf8");

    installPackagedRendererProtocol(rendererRoot);

    const documentResponse = await electronMock.request(new Request("streamskope://app/"));
    expect(documentResponse.status).toBe(200);
    expect(documentResponse.headers.get("content-type")).toBe("text/html; charset=utf-8");
    expect(documentResponse.headers.get("content-security-policy")).toContain("default-src 'self'");
    await expect(documentResponse.text()).resolves.toBe("<main>StreamSkope</main>");

    const scriptResponse = await electronMock.request(
      new Request("streamskope://app/assets/index.js"),
    );
    expect(scriptResponse.status).toBe(200);
    expect(scriptResponse.headers.get("content-type")).toBe("text/javascript; charset=utf-8");

    for (const request of [
      new Request("streamskope://other/index.html"),
      new Request("streamskope://app/%2e%2e/secret.txt"),
      new Request("streamskope://app/%252e%252e/secret.txt"),
      new Request("streamskope://app/missing.js"),
      new Request("streamskope://app/index.html?source=other"),
      new Request("streamskope://app/index.html", { method: "POST" }),
    ]) {
      const response = await electronMock.request(request);
      expect(response.status).toBe(404);
      expect(await response.text()).toBe("Not found");
    }
  });

  it("fails closed when another owner already handles the scheme", () => {
    electronMock.handled = true;

    expect(() => installPackagedRendererProtocol("/opt/StreamSkope/renderer")).toThrow(
      "already has an owner",
    );
  });
});
