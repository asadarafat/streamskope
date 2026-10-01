import { appendFile, mkdtemp, open, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { readBoundedFile } from "../../src/platform/node/bounded-file";

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return { ...actual, open: vi.fn(actual.open) };
});

const directories: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function filePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-bounded-read-"));
  directories.push(directory);
  return join(directory, "data.json");
}

describe("bounded file reads", () => {
  it("accepts exactly the bound and rejects excess bytes without modifying the file", async () => {
    const path = await filePath();
    await writeFile(path, "abcd");
    expect((await readBoundedFile(path, 4)).toString()).toBe("abcd");
    await appendFile(path, "e");
    await expect(readBoundedFile(path, 4)).rejects.toThrow("storage bound");
    expect(await readFile(path, "utf8")).toBe("abcde");
  });

  it("rejects growth between descriptor inspection and reading", async () => {
    const path = await filePath();
    await writeFile(path, "abcd");
    const handle = await open(path, "r");
    const metadata = await handle.stat();
    vi.spyOn(handle, "stat").mockResolvedValueOnce(metadata);
    vi.mocked(open).mockImplementationOnce(async () => {
      await appendFile(path, "x".repeat(1024));
      return handle;
    });
    await expect(readBoundedFile(path, 4)).rejects.toThrow("changed during read");
    // The descriptor closes even when a raced read is rejected.
    await expect(handle.stat()).rejects.toMatchObject({ code: "EBADF" });
    expect((await readFile(path)).length).toBe(1028);
  });

  it("preserves the caller's symlink policy", async () => {
    const path = await filePath();
    const target = `${path}.target`;
    await writeFile(target, "abcd");
    await symlink(target, path);
    expect((await readBoundedFile(path, 4)).toString()).toBe("abcd");
    await expect(readBoundedFile(path, 4, { rejectSymlinks: true })).rejects.toThrow(
      "regular file",
    );
    expect(await readFile(target, "utf8")).toBe("abcd");
  });

  it("propagates missing-file and cancellation errors", async () => {
    const path = await filePath();
    await expect(readBoundedFile(path, 4)).rejects.toMatchObject({ code: "ENOENT" });
    const signal = AbortSignal.abort();
    await expect(readBoundedFile(path, 4, { signal })).rejects.toMatchObject({
      name: "AbortError",
    });
  });
});
