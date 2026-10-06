import { mkdtemp, rm, symlink, truncate, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it, vi } from "vitest";

import { createPluginPackageFilePicker } from "../../src/platform/electron/main/plugin-file-picker";
import { MAX_PLUGIN_ARCHIVE_BYTES } from "../../src/platform/node/plugins/package";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function file(bytes: Uint8Array = Buffer.from("original package")): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-plugin-picker-"));
  roots.push(root);
  const path = join(root, "portable.skope-plugin");
  await writeFile(path, bytes);
  return path;
}

it("returns the original bounded bytes from the selected local file without forwarding its path", async () => {
  const path = await file();
  const dialog = vi.fn(() => Promise.resolve({ canceled: false, filePaths: [path] }));
  const bytes = await createPluginPackageFilePicker(dialog)(new AbortController().signal);
  expect(Buffer.from(bytes!)).toEqual(Buffer.from("original package"));
  expect(dialog).toHaveBeenCalledWith(expect.objectContaining({ properties: ["openFile"] }));
});

it("cancels before reading when the dialog is cancelled or its owner has closed", async () => {
  const cancelled = createPluginPackageFilePicker(() =>
    Promise.resolve({ canceled: true, filePaths: [] }),
  );
  await expect(cancelled(new AbortController().signal)).resolves.toBeNull();
  let finish: (value: { canceled: boolean; filePaths: string[] }) => void = () => undefined;
  const controller = new AbortController();
  const picker = createPluginPackageFilePicker(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const pending = picker(controller.signal);
  controller.abort(new Error("Plugin dialog owner closed."));
  finish({ canceled: false, filePaths: ["/missing/should-never-be-read.skope-plugin"] });
  await expect(pending).rejects.toThrow("Plugin dialog owner closed.");
});

it("rejects symlink selections and oversized regular files before allocating their contents", async () => {
  const path = await file();
  const link = join(roots[0]!, "linked.skope-plugin");
  await symlink(path, link);
  await expect(
    createPluginPackageFilePicker(() => Promise.resolve({ canceled: false, filePaths: [link] }))(
      new AbortController().signal,
    ),
  ).rejects.toThrow(/regular plugin file/u);
  await truncate(path, MAX_PLUGIN_ARCHIVE_BYTES + 1);
  await expect(
    createPluginPackageFilePicker(() => Promise.resolve({ canceled: false, filePaths: [path] }))(
      new AbortController().signal,
    ),
  ).rejects.toThrow(/48 MiB/u);
});

it("keeps local filesystem paths out of file-selection errors returned to the runtime", async () => {
  const path = "/private/user-directory/missing.skope-plugin";
  const result = createPluginPackageFilePicker(() =>
    Promise.resolve({
      canceled: false,
      filePaths: [path],
    }),
  )(new AbortController().signal);
  await expect(result).rejects.toMatchObject({
    message: "The selected plugin file could not be read.",
    recovery: "Choose a readable .skope-plugin file and review it again.",
  });
  await expect(result).rejects.not.toThrow(path);
});
