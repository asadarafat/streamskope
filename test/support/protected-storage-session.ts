import { execFile, spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

const run = promisify(execFile);
export const protectedStorageSessionAvailable =
  process.platform !== "linux" ||
  ["/usr/bin/dbus-daemon", "/usr/bin/dbus-send", "/usr/bin/gnome-keyring-daemon"].every(existsSync);

/** Isolate a real Linux Secret Service without changing the desktop's credential identity. */
export async function startProtectedStorageSession(root: string): Promise<{
  environment: Record<string, string>;
  electronArguments: readonly string[];
  dispose(): Promise<void>;
}> {
  if (process.platform !== "linux")
    return { environment: {}, electronArguments: [], dispose: () => Promise.resolve() };
  if (!protectedStorageSessionAvailable)
    throw new Error("Protected storage requires D-Bus and GNOME Keyring.");
  const environment: Record<string, string> = {
    XDG_DATA_HOME: join(root, "data"),
    XDG_CONFIG_HOME: join(root, "config"),
    GNOME_KEYRING_CONTROL: join(root, "keyring"),
  };
  for (const path of Object.values(environment))
    await mkdir(path, { recursive: true, mode: 0o700 });
  let busPid: number | undefined;
  let keyring: ChildProcess | undefined;
  let disposed = false;
  let keyringFailure: Error | undefined;
  async function dispose(): Promise<void> {
    if (disposed) return;
    disposed = true;
    let failure: unknown;
    try {
      if (keyring?.pid !== undefined && keyring.exitCode === null && keyring.signalCode === null) {
        const stopped = once(keyring, "exit", { signal: AbortSignal.timeout(5_000) });
        keyring.kill("SIGTERM");
        await stopped;
      }
    } catch (error) {
      failure = error;
    }
    if (busPid !== undefined) {
      try {
        process.kill(busPid, "SIGTERM");
      } catch (error) {
        if (!(error instanceof Error && "code" in error && error.code === "ESRCH"))
          failure ??= error;
      }
    }
    if (failure !== undefined)
      throw failure instanceof Error ? failure : new Error("Protected storage cleanup failed.");
  }
  try {
    const bus = await run(
      "dbus-daemon",
      ["--session", "--fork", "--print-address=1", "--print-pid=1"],
      { timeout: 10_000 },
    );
    const [address, pid] = bus.stdout.trim().split("\n");
    if (address === undefined || pid === undefined || !/^\d+$/u.test(pid))
      throw new Error("D-Bus did not report an isolated session.");
    environment.DBUS_SESSION_BUS_ADDRESS = address;
    busPid = Number(pid);
    const env = { ...process.env, ...environment };
    keyring = spawn(
      "gnome-keyring-daemon",
      [
        "--foreground",
        "--unlock",
        "--components=secrets",
        `--control-directory=${environment.GNOME_KEYRING_CONTROL}`,
      ],
      { env, stdio: ["pipe", "ignore", "ignore"] },
    );
    keyring.on("error", (error) => {
      keyringFailure = error;
    });
    keyring.stdin?.on("error", (error) => {
      keyringFailure = error;
    });
    keyring.stdin?.end(`${randomBytes(32).toString("hex")}\n`);
    const deadline = Date.now() + 10_000;
    while (true) {
      if (keyringFailure !== undefined)
        throw new Error("The isolated keyring failed to start.", { cause: keyringFailure });
      const { stdout } = await run(
        "dbus-send",
        [
          "--session",
          "--print-reply",
          "--dest=org.freedesktop.DBus",
          "/org/freedesktop/DBus",
          "org.freedesktop.DBus.NameHasOwner",
          "string:org.freedesktop.secrets",
        ],
        { env, timeout: 5_000 },
      );
      if (stdout.includes("boolean true")) break;
      if (Date.now() >= deadline) throw new Error("The isolated Secret Service did not start.");
      await delay(100);
    }
    return { environment, electronArguments: ["--password-store=gnome-libsecret"], dispose };
  } catch (error) {
    await dispose();
    throw error;
  }
}
