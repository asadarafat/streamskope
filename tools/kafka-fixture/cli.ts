import { fileURLToPath } from "node:url";
import { join } from "node:path";

import { FileFixtureOwnershipStore } from "./file-ownership-store";
import { DEFAULT_OWNED_FIXTURE_NAME } from "./defaults";
import { defaultOwnedFixtureRequest } from "./owned-request";
import {
  FixtureLifecycleError,
  KafkaFixtureLifecycle,
  type ExternalFixtureRequest,
  type OwnedFixtureRequest,
} from "./lifecycle";
import { NodeFixtureRuntime } from "./node-runtime";

const repositoryRoot = fileURLToPath(new URL("../..", import.meta.url));
const fixtureRoot = join(repositoryRoot, "aio-kafka");

function parseOptions(values: readonly string[]): ReadonlyMap<string, string> {
  const options = new Map<string, string>();

  for (let index = 0; index < values.length; index += 2) {
    const option = values[index];
    const value = values[index + 1];
    if (option === undefined || !option.startsWith("--") || value === undefined) {
      throw new Error("Options must use --name value pairs.");
    }

    const name = option.slice(2);
    if (options.has(name)) {
      throw new Error(`Option --${name} was provided more than once.`);
    }
    options.set(name, value);
  }

  return options;
}

function rejectUnknownOptions(
  options: ReadonlyMap<string, string>,
  allowed: ReadonlySet<string>,
): void {
  for (const name of options.keys()) {
    if (!allowed.has(name)) {
      throw new Error(`Unknown option --${name}.`);
    }
  }
}

function requiredOption(options: ReadonlyMap<string, string>, name: string): string {
  const value = options.get(name);
  if (value === undefined || value.length === 0) {
    throw new Error(`Option --${name} is required.`);
  }
  return value;
}

function parsePort(value: string, name: string): number {
  const port = Number(value);
  if (!Number.isInteger(port)) {
    throw new Error(`Option --${name} must be an integer.`);
  }
  return port;
}

async function startOwned(
  lifecycle: KafkaFixtureLifecycle,
  options: ReadonlyMap<string, string>,
): Promise<void> {
  rejectUnknownOptions(
    options,
    new Set(["kafka-port", "name", "oauth-port", "schema-registry-port"]),
  );
  const name = options.get("name") ?? DEFAULT_OWNED_FIXTURE_NAME;
  const defaults = await defaultOwnedFixtureRequest(repositoryRoot, name);
  const request: OwnedFixtureRequest = {
    ...defaults,
    kafkaPort: parsePort(options.get("kafka-port") ?? String(defaults.kafkaPort), "kafka-port"),
    oauthPort: parsePort(options.get("oauth-port") ?? String(defaults.oauthPort), "oauth-port"),
    schemaRegistryPort: parsePort(
      options.get("schema-registry-port") ?? String(defaults.schemaRegistryPort),
      "schema-registry-port",
    ),
  };

  const connection = await lifecycle.startOwned(request);
  process.stdout.write(`${JSON.stringify(connection, undefined, 2)}\n`);
}

async function stopOwned(
  lifecycle: KafkaFixtureLifecycle,
  options: ReadonlyMap<string, string>,
): Promise<void> {
  rejectUnknownOptions(options, new Set(["name"]));
  const name = requiredOption(options, "name");
  await lifecycle.stopOwned(name);
  process.stdout.write(`${JSON.stringify({ name, stopped: true })}\n`);
}

async function attachExternal(
  lifecycle: KafkaFixtureLifecycle,
  options: ReadonlyMap<string, string>,
): Promise<void> {
  rejectUnknownOptions(options, new Set(["ca", "kafka", "oauth"]));
  const request: ExternalFixtureRequest = {
    caPath: requiredOption(options, "ca"),
    kafkaEndpoint: requiredOption(options, "kafka"),
    oauthEndpoint: requiredOption(options, "oauth"),
  };
  const connection = await lifecycle.attachExternal(request);
  process.stdout.write(`${JSON.stringify(connection, undefined, 2)}\n`);
}

async function main(): Promise<void> {
  const [command, ...rawOptions] = process.argv.slice(2);
  const options = parseOptions(rawOptions);
  const runtime = new NodeFixtureRuntime(repositoryRoot);
  const ownershipStore = new FileFixtureOwnershipStore(join(fixtureRoot, "ownership", "records"));
  const lifecycle = new KafkaFixtureLifecycle(runtime, ownershipStore);

  switch (command) {
    case "attach":
      await attachExternal(lifecycle, options);
      return;
    case "start":
      await startOwned(lifecycle, options);
      return;
    case "stop":
      await stopOwned(lifecycle, options);
      return;
    default:
      throw new Error("Usage: kafka-fixture <start|stop|attach> [--name value ...]");
  }
}

main().catch((error: unknown) => {
  if (error instanceof FixtureLifecycleError) {
    process.stderr.write(
      `${JSON.stringify({
        code: error.code,
        diagnostic: error.diagnostic,
        message: error.message,
        unavailablePorts: error.unavailablePorts,
      })}\n`,
    );
  } else {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  }
  process.exitCode = 1;
});
