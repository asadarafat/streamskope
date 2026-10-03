import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import { describe, expect, it } from "vitest";

import {
  installerChecksum,
  installerName,
  loadCandidateInstaller,
  nativeCommand,
} from "../../tools/check/native-installers";

describe("native recovery release identity", () => {
  it("refuses to label an arbitrary local installer as a published release", async () => {
    await expect(loadCandidateInstaller("untrusted-installer", "0.7.0", "/unused")).rejects.toThrow(
      "development version 0.0.0-dev",
    );
  });
  it("selects the actual published installer on each supported native runner", () => {
    expect(installerName("0.6.0", "darwin", "arm64")).toBe("StreamSkope-0.6.0-darwin-arm64.dmg");
    expect(installerName("0.7.0", "win32", "x64")).toBe("StreamSkope-0.7.0-win32-x64-Setup.exe");
    expect(installerName("0.7.0", "linux", "x64")).toBe("StreamSkope-0.7.0-linux-x64.AppImage");
  });

  it("rejects floating, path-like and cross-architecture release identities", () => {
    for (const version of ["main", "v0.7.0", "../0.7.0", "0.7.0/other"]) {
      expect(() => installerName(version, "linux", "x64")).toThrow("explicit release versions");
    }
    expect(() => installerName("0.7.0", "linux", "arm64")).toThrow("supported native");
  });

  it("requires an exact filename match instead of trusting a similarly named checksum", () => {
    const digest = "a".repeat(64);
    const name = "StreamSkope-0.7.0-linux-x64.AppImage";
    expect(installerChecksum(`${digest}  ${name}\n`, name)).toBe(digest);
    expect(installerChecksum(`${digest} *${name}\r\n`, name)).toBe(digest);
    expect(() => installerChecksum(`${digest}  ${name}.old\n`, name)).toThrow("exactly one");
  });

  it("rejects ambiguous or malformed checksum records", () => {
    const name = "StreamSkope-0.7.0-linux-x64.AppImage";
    const entry = `${"a".repeat(64)}  ${name}\n`;
    expect(() => installerChecksum(entry + entry, name)).toThrow("exactly one");
    expect(() => installerChecksum(`not-a-digest  ${name}`, name)).toThrow("exactly one");
  });
});

it("a timed-out launcher cannot leave its owned worker writing after cleanup", async () => {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-native-process-test-"));
  const workerPid = join(directory, "worker.pid");
  const heartbeat = join(directory, "heartbeat");
  let pid: number | undefined;
  const worker = `
    const fs = require("node:fs");
    fs.writeFileSync(process.argv[1], String(process.pid));
    process.on("SIGTERM", () => {});
    setInterval(() => fs.appendFileSync(process.argv[2], "."), 20);
  `;
  const launcher = `
    require("node:child_process").spawn(process.execPath,
      ["-e", process.argv[1], process.argv[2], process.argv[3]], {
        stdio: "ignore", detached: process.platform !== "win32"
      });
    setInterval(() => {}, 1000);
  `;
  const outcome = nativeCommand(process.execPath, ["-e", launcher, worker, workerPid, heartbeat], {
    timeoutMs: 1_500,
    quiet: true,
  }).catch((error: unknown) => error);
  try {
    await expect.poll(() => readFile(workerPid, "utf8").catch(() => "")).not.toBe("");
    pid = Number(await readFile(workerPid, "utf8"));
    expect(Number.isSafeInteger(pid) && pid > 0).toBe(true);
    await expect
      .poll(() =>
        stat(heartbeat)
          .then((value) => value.size)
          .catch(() => 0),
      )
      .toBeGreaterThan(0);
    expect(await outcome).toMatchObject({ code: "ETIMEDOUT" });
    const finalSize = (await stat(heartbeat)).size;
    await delay(200);
    expect((await stat(heartbeat)).size).toBe(finalSize);
  } finally {
    await outcome;
    if (pid !== undefined) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {
        // A passing command already removed the worker; fallback cleanup is best effort.
      }
    }
    await rm(directory, { recursive: true, force: true, maxRetries: 3 });
  }
});
