import { spawn } from "node:child_process";
import { once } from "node:events";

import { describe, expect, it } from "vitest";

import { stopWebDevelopmentOwner } from "../../tools/stop-web-development-owner";

describe("development process ownership", () => {
  it("refuses to stop the current process", async () => {
    await expect(stopWebDevelopmentOwner(process.pid, process.cwd())).rejects.toThrow(
      "current launcher",
    );
  });

  it.skipIf(process.platform !== "linux")(
    "does not stop an unrelated Node process in the same repository",
    async () => {
      const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
        stdio: "ignore",
      });
      await once(child, "spawn");
      try {
        if (child.pid === undefined) throw new Error("Child has no PID.");
        await expect(stopWebDevelopmentOwner(child.pid, process.cwd())).rejects.toThrow(
          "No process was stopped",
        );
        expect(child.exitCode).toBeNull();
      } finally {
        const exited = once(child, "exit");
        child.kill("SIGTERM");
        await exited;
      }
    },
  );
});
