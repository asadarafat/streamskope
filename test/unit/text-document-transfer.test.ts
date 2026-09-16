// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import type { HostTextDocument } from "../../src/features/kafka/contracts";
import type { StreamSkopeDesktop } from "../../src/platform/desktop";
import {
  browserTextDocumentTransfer,
  createTextDocumentTransfer,
} from "../../src/features/kafka/ui";

const documentValue: HostTextDocument = {
  byteSize: 3,
  content: "{}\n",
  fileName: "evidence.json",
  mediaType: "application/json",
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("browser text document transfer", () => {
  it("copies exact text through the standards clipboard", async () => {
    const writeText = vi.fn<(content: string) => Promise<void>>(() => Promise.resolve());
    Object.defineProperty(globalThis.navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    await browserTextDocumentTransfer.copy("  exact value  ");

    expect(writeText).toHaveBeenCalledWith("  exact value  ");
  });

  it("downloads through one temporary anchor and always revokes the object URL", async () => {
    const createObjectURL = vi.fn(() => "blob:message-export");
    const revokeObjectURL = vi.fn();
    Object.defineProperties(globalThis.URL, {
      createObjectURL: { configurable: true, value: createObjectURL },
      revokeObjectURL: { configurable: true, value: revokeObjectURL },
    });
    const click = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => undefined);

    await browserTextDocumentTransfer.download(documentValue);

    expect(createObjectURL).toHaveBeenCalledOnce();
    expect(click).toHaveBeenCalledOnce();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:message-export");
    expect(document.querySelector("a[download='evidence.json']")).toBeNull();
  });

  it("uses the native Save dialog when the Electron desktop port is present", async () => {
    const saveTextDocument = vi.fn<StreamSkopeDesktop["saveTextDocument"]>(() =>
      Promise.resolve({ state: "cancelled", version: 1 }),
    );
    const transfer = createTextDocumentTransfer({
      saveTextDocument,
      subscribeActions: (): (() => void) => () => undefined,
    });
    const createObjectURL = vi.fn();
    Object.defineProperty(globalThis.URL, "createObjectURL", {
      configurable: true,
      value: createObjectURL,
    });

    await expect(transfer.download(documentValue)).resolves.toBe("cancelled");

    expect(saveTextDocument).toHaveBeenCalledWith(documentValue);
    expect(createObjectURL).not.toHaveBeenCalled();
  });

  it("revokes the object URL and removes the anchor when browser activation fails", async () => {
    const revokeObjectURL = vi.fn();
    Object.defineProperties(globalThis.URL, {
      createObjectURL: { configurable: true, value: () => "blob:failed-export" },
      revokeObjectURL: { configurable: true, value: revokeObjectURL },
    });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {
      throw new Error("activation denied");
    });

    await expect(browserTextDocumentTransfer.download(documentValue)).rejects.toThrow(
      "activation denied",
    );
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:failed-export");
    expect(document.querySelector("a[download='evidence.json']")).toBeNull();
  });

  it("revokes the object URL when the temporary anchor cannot be constructed", async () => {
    const revokeObjectURL = vi.fn();
    Object.defineProperties(globalThis.URL, {
      createObjectURL: { configurable: true, value: () => "blob:construction-failed" },
      revokeObjectURL: { configurable: true, value: revokeObjectURL },
    });
    vi.spyOn(globalThis.document, "createElement").mockImplementationOnce(() => {
      throw new Error("DOM construction denied");
    });

    await expect(browserTextDocumentTransfer.download(documentValue)).rejects.toThrow(
      "DOM construction denied",
    );
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:construction-failed");
  });
});
