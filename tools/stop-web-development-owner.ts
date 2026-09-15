import { execFile } from "node:child_process";
import { readFile, realpath } from "node:fs/promises";
import { basename } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

const execute = promisify(execFile);

export async function stopWebDevelopmentOwner(pid: number, repositoryRoot: string): Promise<void> {
  if (pid <= 0 || pid === process.pid) throw new Error("Refusing to stop the current launcher.");
  let argumentsList: string[];
  let cwd: string;
  if (process.platform === "linux") {
    argumentsList = (await readFile(`/proc/${pid}/cmdline`, "utf8")).split("\0").filter(Boolean);
    cwd = await realpath(`/proc/${pid}/cwd`);
  } else if (process.platform === "darwin") {
    const result = await execute("ps", ["-p", String(pid), "-o", "command="], { timeout: 5000 });
    argumentsList = result.stdout.trim().split(/\s+/u);
    const files = await execute("lsof", ["-a", "-p", String(pid), "-d", "cwd", "-Fn"], {
      timeout: 5000,
    });
    const directory = files.stdout
      .split("\n")
      .find((line) => line.startsWith("n"))
      ?.slice(1);
    if (directory === undefined)
      throw new Error("Cannot verify the development process directory.");
    cwd = await realpath(directory);
  } else {
    throw new Error(
      "Automatic takeover cannot verify process ownership on this platform. Stop the previous StreamSkope launcher first.",
    );
  }
  if (
    cwd !== repositoryRoot ||
    basename(argumentsList[0] ?? "") !== "node" ||
    argumentsList.length !== 4 ||
    argumentsList.slice(1).join(" ") !== "--import tsx tools/start-web-development.ts"
  ) {
    throw new Error(
      "Occupied ports do not belong to a verified StreamSkope launcher from this checkout. No process was stopped.",
    );
  }
  process.kill(pid, "SIGTERM");
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      process.kill(pid, 0);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ESRCH") return;
      throw error;
    }
    await delay(100);
  }
  throw new Error(
    "The previous StreamSkope launcher did not stop within 10 seconds. No force kill was attempted.",
  );
}
