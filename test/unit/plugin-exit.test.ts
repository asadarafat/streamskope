import { describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { confirmPluginExit } from "../../src/platform/electron/main/plugin-exit";
import { testHostExecute } from "../support/host-response";

describe("desktop plugin exit coordination", () => {
  it("waits for the plugin cleanup decision before allowing application exit", async () => {
    let completed = false;
    const host = {
      execute: testHostExecute((command) => {
        const result =
          command.command === "plugins.exit.prepare"
            ? {
                pluginExit: completed
                  ? null
                  : {
                      pluginId: "example.capture",
                      title: "Pending work",
                      message: "Clean up?",
                      detail: "The session is temporary.",
                      actions: [
                        { id: "cleanup", label: "Clean up" },
                        { id: "cancel", label: "Cancel" },
                      ],
                      cancelAction: "cancel",
                    },
              }
            : {
                allowed:
                  command.command === "plugins.exit.resolve" &&
                  command.payload.action === "cleanup",
              };
        if ("allowed" in result) completed = result.allowed;
        return Promise.resolve({
          command: command.command,
          id: command.id,
          ok: true,
          version: HOST_PROTOCOL_VERSION,
          result: { correlationId: command.id, ...result },
        });
      }),
    };
    const report = vi.fn(() => Promise.resolve());
    expect(await confirmPluginExit(host, () => Promise.resolve("cancel"), report)).toBe(false);
    expect(completed).toBe(false);
    expect(await confirmPluginExit(host, () => Promise.resolve("cleanup"), report)).toBe(true);
    expect(report).not.toHaveBeenCalled();
  });

  it("keeps the application open when a plugin's cleanup request fails", async () => {
    const host = { execute: testHostExecute(() => Promise.reject(new Error("Cleanup failed"))) };
    const report = vi.fn(() => Promise.resolve());
    expect(await confirmPluginExit(host, () => Promise.resolve("cleanup"), report)).toBe(false);
    expect(report).toHaveBeenCalledWith(
      expect.stringContaining("recovery information is retained"),
    );
  });

  it.each([32, 33])(
    "bounds exit prompts while allowing all 32 installed plugins (%i prompts)",
    async (total) => {
      let completed = 0;
      const host = {
        execute: testHostExecute((command) => {
          if (command.command === "plugins.exit.resolve") completed += 1;
          return Promise.resolve({
            command: command.command,
            id: command.id,
            ok: true,
            version: HOST_PROTOCOL_VERSION,
            result: {
              correlationId: command.id,
              ...(command.command === "plugins.exit.resolve"
                ? { allowed: true }
                : {
                    pluginExit:
                      completed === total
                        ? null
                        : {
                            pluginId: `example.plugin${completed}`,
                            title: "Pending work",
                            message: "Clean up?",
                            detail: "Temporary session",
                            actions: [{ id: "cleanup", label: "Clean up" }],
                            cancelAction: "cleanup",
                          },
                  }),
            },
          });
        }),
      };
      const report = vi.fn(() => Promise.resolve());
      expect(await confirmPluginExit(host, () => Promise.resolve("cleanup"), report)).toBe(
        total === 32,
      );
      expect(completed).toBe(32);
      expect(report).toHaveBeenCalledTimes(total === 32 ? 0 : 1);
    },
  );
});
