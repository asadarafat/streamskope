import { afterEach, describe, expect, it, vi } from "vitest";

import { BrowserPluginFiles } from "../../src/platform/node/browser-plugin-files";

const signal = (): AbortSignal => new AbortController().signal;
const files = new Set<BrowserPluginFiles>();

function bridge(): BrowserPluginFiles {
  const value = new BrowserPluginFiles();
  files.add(value);
  return value;
}

afterEach(() => {
  for (const value of files) value.close();
  files.clear();
  vi.useRealTimers();
});

describe("browser plugin file selection ownership", () => {
  it("binds a selection to its command, consumes it once, and wipes it after dispatch", async () => {
    const value = bridge();
    const original = Buffer.from("signed portable package");
    value.stage("selected-request", original);
    original.fill(0);
    await expect(value.chooseFile(signal())).rejects.toThrow("Select a plugin file");
    await expect(value.run("different-request", () => value.chooseFile(signal()))).rejects.toThrow(
      "Select a plugin file",
    );
    let selected: Uint8Array | null = null;
    await value.run("selected-request", async () => {
      selected = await value.chooseFile(signal());
      expect(Buffer.from(selected!).toString()).toBe("signed portable package");
      await expect(value.chooseFile(signal())).rejects.toThrow("Select a plugin file");
    });
    expect(selected).toEqual(Buffer.alloc("signed portable package".length));
    await expect(value.run("selected-request", () => value.chooseFile(signal()))).rejects.toThrow(
      "Select a plugin file",
    );
  });

  it("keeps concurrent commands independent and rejects replacement and excess staging", async () => {
    const value = bridge();
    value.stage("first", Buffer.from("first package"));
    value.stage("second", Buffer.from("second package"));
    expect(() => value.stage("first", Buffer.from("replacement"))).toThrow("already in use");
    expect(() => value.stage("third", Buffer.from("third"))).toThrow("current plugin file");
    let release = (): void => undefined;
    const waiting = new Promise<void>((resolve) => {
      release = resolve;
    });
    const first = value.run("first", async () => {
      await waiting;
      return Buffer.from((await value.chooseFile(signal()))!).toString();
    });
    const second = value.run("second", async () =>
      Buffer.from((await value.chooseFile(signal()))!).toString(),
    );
    await expect(value.run("first", () => Promise.resolve("unexpected"))).rejects.toThrow(
      "already in progress",
    );
    expect(() => value.stage("first", Buffer.from("replacement"))).toThrow("already in use");
    await expect(second).resolves.toBe("second package");
    release();
    await expect(first).resolves.toBe("first package");
  });

  it("expires, discards, and locks selections instead of allowing later reuse", async () => {
    vi.useFakeTimers();
    const value = bridge();
    value.stage("expires", Buffer.from("package"));
    await vi.advanceTimersByTimeAsync(60_000);
    await expect(value.run("expires", () => value.chooseFile(signal()))).rejects.toThrow(
      "Select a plugin file",
    );
    value.stage("discard", Buffer.from("package"));
    value.discard("discard");
    await expect(value.run("discard", () => value.chooseFile(signal()))).rejects.toThrow(
      "Select a plugin file",
    );
    value.stage("lock", Buffer.from("package"));
    value.close();
    expect(() => value.stage("new", Buffer.from("package"))).toThrow("Unlock");
    await expect(value.run("lock", () => value.chooseFile(signal()))).rejects.toThrow("Unlock");
    const replacement = bridge();
    await expect(replacement.run("lock", () => replacement.chooseFile(signal()))).rejects.toThrow(
      "Select a plugin file",
    );
  });

  it("removes aborted selections and failed commands without leaking buffers", async () => {
    const value = bridge();
    value.stage("aborted", Buffer.from("package"));
    const controller = new AbortController();
    controller.abort(new Error("request canceled"));
    await expect(value.run("aborted", () => value.chooseFile(controller.signal))).rejects.toThrow(
      "request canceled",
    );
    await expect(value.run("aborted", () => value.chooseFile(signal()))).rejects.toThrow(
      "Select a plugin file",
    );
    value.stage("failed", Buffer.from("package"));
    let selected: Uint8Array | null = null;
    await expect(
      value.run("failed", async () => {
        selected = await value.chooseFile(signal());
        throw new Error("verification failed");
      }),
    ).rejects.toThrow("verification failed");
    expect(selected).toEqual(Buffer.alloc(7));
  });

  it("rejects path-like IDs and bounds individual and combined archive size", () => {
    const value = bridge();
    for (const id of ["", "../file", "request?token=secret", "x".repeat(129)]) {
      expect(() => value.stage(id, Buffer.from("package"))).toThrow("identifier is invalid");
    }
    expect(() => value.stage("empty", Buffer.alloc(0))).toThrow("between 1 byte");
    expect(() => value.stage("large", new Uint8Array(48 * 1024 * 1024 + 1))).toThrow(
      "between 1 byte",
    );
    value.stage("maximum", new Uint8Array(48 * 1024 * 1024));
    expect(() => value.stage("extra", Buffer.from("x"))).toThrow("current plugin file");
  });
});
