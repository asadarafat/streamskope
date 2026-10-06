import { MAX_PLUGIN_ARCHIVE_BYTES } from "../../node/plugins/package";
import { pluginProblem } from "../../node/plugins/problem";
import { readBoundedFile } from "../../node/bounded-file";

export type PluginPackageFilePicker = (signal: AbortSignal) => Promise<Uint8Array | null>;

export interface PluginFileDialogOptions {
  readonly title: string;
  readonly buttonLabel: string;
  readonly filters: { name: string; extensions: string[] }[];
  readonly properties: ["openFile"];
}

/** Only the desktop composition receives paths; the runtime receives bounded original bytes. */
export function createPluginPackageFilePicker(
  openDialog: (
    options: PluginFileDialogOptions,
  ) => Promise<{ readonly canceled: boolean; readonly filePaths: readonly string[] }>,
): PluginPackageFilePicker {
  return async (signal) => {
    signal.throwIfAborted();
    const selection = await openDialog({
      title: "Install StreamSkope plugin from file",
      buttonLabel: "Review plugin",
      filters: [{ name: "StreamSkope plugin", extensions: ["skope-plugin"] }],
      properties: ["openFile"],
    });
    signal.throwIfAborted();
    if (selection.canceled || selection.filePaths.length === 0) return null;
    if (selection.filePaths.length !== 1) throw new Error("Choose one StreamSkope plugin file.");
    try {
      return await readBoundedFile(selection.filePaths[0]!, MAX_PLUGIN_ARCHIVE_BYTES, {
        rejectSymlinks: true,
        signal,
      });
    } catch (error) {
      signal.throwIfAborted();
      const known = error instanceof Error ? error.message : "";
      const reason =
        known === "Expected a regular file"
          ? "Choose a regular plugin file; symbolic links are not accepted."
          : known === "File exceeds its storage bound" ||
              known === "File exceeds its storage bound or is not a regular file"
            ? "The selected plugin file exceeds the 48 MiB limit or is not a regular file."
            : known === "File changed during read"
              ? "The selected plugin file changed while being read."
              : "The selected plugin file could not be read.";
      // Native filesystem errors contain local paths; only safe recovery text crosses the bridge.
      throw pluginProblem(reason, "Choose a readable .skope-plugin file and review it again.");
    }
  };
}
