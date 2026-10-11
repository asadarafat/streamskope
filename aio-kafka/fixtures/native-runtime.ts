import { spawn, execFile, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream, createWriteStream, type WriteStream } from "node:fs";
import { mkdir, mkdtemp, readFile, rename, rm, stat } from "node:fs/promises";
import { join, resolve, win32 } from "node:path";
import { Readable } from "node:stream";
import { finished, pipeline } from "node:stream/promises";
import type { ReadableStream } from "node:stream/web";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import { KafkaFixtureError } from "./error";
import { kafkaPin, NATIVE_ARCHIVE_SHA512 } from "./pins";
import { disposeFixtureResources } from "./resources";
import type { BrokerRuntime } from "./runtime";

const run = promisify(execFile);
async function digest(path: string): Promise<string> {
  const hash = createHash("sha512");
  for await (const chunk of createReadStream(path)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}

export async function fetchNativeKafkaArchive(fetchArchive = fetch): Promise<Response> {
  const { version: KAFKA_VERSION } = await kafkaPin();
  const ARCHIVE = `kafka_2.13-${KAFKA_VERSION}.tgz`;
  // Prefer the active mirror, retaining old pins after archival. The caller still
  // verifies downloaded and cached bytes against the original release checksum.
  let failure: unknown = new Error("Unable to download the pinned Kafka fixture.");
  for (const origin of ["https://downloads.apache.org", "https://archive.apache.org/dist"]) {
    try {
      const response = await fetchArchive(`${origin}/kafka/${KAFKA_VERSION}/${ARCHIVE}`, {
        signal: AbortSignal.timeout(180_000),
      });
      if (response.ok && response.body !== null) return response;
      await response.body?.cancel();
      failure = new Error("Unable to download the pinned Kafka fixture.");
    } catch (error) {
      failure = error;
    }
  }
  throw failure;
}

export function nativeArchiveExtractor(
  platform = process.platform,
  systemRoot = process.env.SystemRoot,
): string {
  if (platform !== "win32") return "tar";
  if (systemRoot === undefined || !win32.isAbsolute(systemRoot))
    throw new Error("Windows native fixture requires an absolute SystemRoot.");
  // The PATH can contain Git/MSYS GNU tar, which treats native drive-letter
  // archive arguments as remote paths. Windows ships a native bsdtar binary.
  return win32.join(systemRoot, "System32", "tar.exe");
}

export async function kafkaDistribution(): Promise<string> {
  const { version } = await kafkaPin();
  const ARCHIVE = `kafka_2.13-${version}.tgz`;
  const ARCHIVE_SHA512 = NATIVE_ARCHIVE_SHA512[version];
  if (ARCHIVE_SHA512 === undefined) throw new Error("Native Kafka archive pin is not reviewed.");
  const cache = resolve(".cache/native-kafka");
  await mkdir(cache, { recursive: true });
  const archive = join(cache, ARCHIVE);
  if (!(await stat(archive).catch(() => undefined))) {
    const response = await fetchNativeKafkaArchive();
    const temporary = `${archive}.${process.pid}.partial`;
    try {
      await pipeline(
        Readable.fromWeb(response.body as ReadableStream<Uint8Array>),
        createWriteStream(temporary, { flags: "wx" }),
      );
      if ((await digest(temporary)) !== ARCHIVE_SHA512)
        throw new Error("Kafka fixture checksum mismatch.");
      await rename(temporary, archive);
    } finally {
      await rm(temporary, { force: true });
    }
  }
  if ((await digest(archive)) !== ARCHIVE_SHA512)
    throw new Error("Cached Kafka fixture checksum mismatch.");
  // Extract into a new owned directory; do not trust a previously extracted executable tree.
  const extracted = await mkdtemp(join(cache, "distribution-"));
  try {
    await run(nativeArchiveExtractor(), ["-xzf", archive, "-C", extracted], { timeout: 60_000 });
    return extracted;
  } catch (error) {
    let cleanup: "passed" | "failed" = "passed";
    try {
      await rm(extracted, { recursive: true, force: true });
    } catch {
      cleanup = "failed";
    }
    throw new KafkaFixtureError("extract Kafka distribution", error, "", cleanup);
  }
}

async function stop(child: ChildProcess | undefined): Promise<void> {
  if (!child || child.pid === undefined || child.exitCode !== null || child.signalCode !== null)
    return;
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  child.kill("SIGTERM");
  await Promise.race([exited, delay(10_000, undefined, { ref: false })]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await exited;
  }
}

/** Explicit exception for native Windows/macOS installer runners without Docker. */
export async function createNativeBrokerRuntime(
  distribution = kafkaDistribution,
): Promise<BrokerRuntime> {
  const pin = await kafkaPin();
  const archiveSha512 = NATIVE_ARCHIVE_SHA512[pin.version];
  if (archiveSha512 === undefined) throw new Error("Native Kafka archive pin is not reviewed.");
  const extracted = await distribution();
  let broker: ChildProcess | undefined;
  let log: WriteStream | undefined;
  let logFailure: Error | undefined;
  let directory: string | undefined;
  return {
    metadata: {
      backend: "native",
      kafkaVersion: pin.version,
      archiveSha512,
    },
    tool: (command, arguments_, options) => run(command, [...arguments_], options),
    async start(options): Promise<void> {
      directory = options.directory;
      const home = join(extracted, `kafka_2.13-${pin.version}`);
      const java = [
        "-Xms128m",
        "-Xmx384m",
        `-Dkafka.logs.dir=${join(directory, "logs")}`,
        `-Dlog4j2.configurationFile=${join(home, "config/log4j2.yaml")}`,
        `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=${options.oauthRoot}/.well-known/jwks.json`,
        "-cp",
        join(home, "libs/*"),
      ];
      const settings = join(directory, "server.properties");
      await run(
        "java",
        [
          ...java,
          "kafka.tools.StorageTool",
          "format",
          "--standalone",
          `--cluster-id=${options.clusterId}`,
          "--config",
          settings,
          ...options.storageArguments,
        ],
        { cwd: directory, timeout: 30_000 },
      );
      log = createWriteStream(join(directory, "broker.log"), { mode: 0o600 });
      log.on("error", (error: Error) => {
        logFailure = error;
      });
      broker = spawn("java", [...java, "kafka.Kafka", settings], {
        cwd: directory,
        stdio: ["ignore", "pipe", "pipe"],
      });
      broker.stdout?.pipe(log, { end: false });
      broker.stderr?.pipe(log, { end: false });
      broker.once("error", (error: Error) => {
        logFailure = error;
      });
    },
    exited(): Promise<boolean> {
      if (logFailure !== undefined) throw logFailure;
      return Promise.resolve(
        broker === undefined || broker.exitCode !== null || broker.signalCode !== null,
      );
    },
    logs: (): Promise<string> =>
      directory === undefined
        ? Promise.resolve("")
        : readFile(join(directory, "broker.log"), "utf8").catch(() => ""),
    dispose: (): Promise<void> =>
      disposeFixtureResources([
        (): Promise<void> => stop(broker),
        async (): Promise<void> => {
          if (log !== undefined) {
            log.end();
            await finished(log);
          }
        },
        (): Promise<void> =>
          rm(extracted, { recursive: true, force: true, maxRetries: 5, retryDelay: 500 }),
      ]),
  };
}
