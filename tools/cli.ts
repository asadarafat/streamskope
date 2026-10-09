import { open, unlink } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  readCliJson,
  runReadOnlyCli,
  parseCliConfiguration,
  parseCliQuery,
  CliInputError,
} from "../src/platform/node/read-only-cli";

function usageError(): number {
  process.stderr.write(
    JSON.stringify({
      format: "streamskope.cli/v1",
      kind: "error",
      code: "INVALID_INPUT",
      message: "Use inspect, query or export with the options shown by --help.",
    }) + "\n",
  );
  return 2;
}

export async function cliMain(args: readonly string[]): Promise<number> {
  if (args.length === 0 || args[0] === "--help") {
    process.stdout.write(
      'StreamSkope read-only CLI (Node 24)\ninspect --config PRIVATE.json\nquery --config PRIVATE.json --query QUERY.json\nexport --config PRIVATE.json --query QUERY.json --output NEW.ndjson\nConfiguration: {connection: <desktop connection input>, protection: {readOnly:true,maskKey:false,maskHeaders:[],valuePaths:[]}, codecs: {key:"auto",value:"auto"}} (codecs optional)\nExit: 0 success, 1 operation failed, 2 usage/config, 130 cancelled. No mutation commands.\n',
    );
    return 0;
  }
  const [operation, ...rest] = args;
  const options = new Map<string, string>();
  for (let i = 0; i < rest.length; i += 2) {
    const key = rest[i],
      value = rest[i + 1];
    if (!key || !value || !["--config", "--query", "--output"].includes(key) || options.has(key))
      return usageError();
    options.set(key, value);
  }
  if (
    !["inspect", "query", "export"].includes(operation!) ||
    !options.has("--config") ||
    (operation !== "inspect" && !options.has("--query")) ||
    (operation === "export" && !options.has("--output")) ||
    (operation === "inspect" && options.has("--query")) ||
    (operation !== "export" && options.has("--output"))
  )
    return usageError();
  const controller = new AbortController();
  const cancel = (): void => controller.abort();
  process.once("SIGINT", cancel);
  process.once("SIGTERM", cancel);
  let output: Awaited<ReturnType<typeof open>> | undefined;
  let path: string | undefined;
  let configured = false;
  try {
    const config = await readCliJson(options.get("--config")!, true);
    const query = options.has("--query") ? await readCliJson(options.get("--query")!) : undefined;
    parseCliConfiguration(config);
    parseCliQuery(operation as "inspect" | "query" | "export", query);
    if (operation === "export") {
      path = options.get("--output")!;
      output = await open(path, "wx", 0o600);
    }
    configured = true;
    const file = output;
    await runReadOnlyCli(
      operation as "inspect" | "query" | "export",
      config,
      query,
      {
        write: async (value) => {
          const line = JSON.stringify(value) + "\n";
          if (file) await file.write(line);
          else
            await new Promise<void>((resolve, reject) =>
              process.stdout.write(line, (error) => (error ? reject(error) : resolve())),
            );
        },
      },
      AbortSignal.any([controller.signal, AbortSignal.timeout(60000)]),
    );
    return 0;
  } catch (error) {
    if (output && path) {
      await output.close();
      output = undefined;
      await unlink(path).catch(() => undefined);
    }
    process.stderr.write(
      JSON.stringify({
        format: "streamskope.cli/v1",
        kind: "error",
        code: controller.signal.aborted
          ? "CANCELLED"
          : configured
            ? "OPERATION_FAILED"
            : "INVALID_INPUT",
        message:
          error instanceof CliInputError
            ? error.message
            : "Check private configuration, query bounds, connectivity and permissions. Secrets and raw diagnostics are withheld.",
      }) + "\n",
    );
    return controller.signal.aborted ? 130 : configured ? 1 : 2;
  } finally {
    await output?.close();
    process.removeListener("SIGINT", cancel);
    process.removeListener("SIGTERM", cancel);
  }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void cliMain(process.argv.slice(2)).then((code) => {
    process.exitCode = code;
  });
