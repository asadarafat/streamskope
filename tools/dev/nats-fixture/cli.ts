import { fileURLToPath } from "node:url";

import { NatsFixtureLifecycle } from "./lifecycle";
import { NatsFixtureError } from "./ownership";

const cancellation = new AbortController();
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    process.exitCode = signal === "SIGINT" ? 130 : 143;
    cancellation.abort();
  });
}

async function main(): Promise<void> {
  const [command, ...options] = process.argv.slice(2);
  const lifecycle = new NatsFixtureLifecycle(fileURLToPath(new URL("../../..", import.meta.url)));
  if (command === "publish") {
    const parsed = new Map<string, string>();
    for (let index = 0; index < options.length; index += 2) {
      const name = options[index];
      const value = options[index + 1];
      if ((name !== "--seconds" && name !== "--rate") || value === undefined || parsed.has(name))
        throw new NatsFixtureError("Publish accepts --seconds 1–600 and --rate 1–100.");
      parsed.set(name, value);
    }
    process.stdout.write(
      "Publishing live samples; subscribe to streamskope.fixture.> in the workbench first.\n",
    );
    process.stdout.write(
      `${JSON.stringify(await lifecycle.publish(Number(parsed.get("--seconds") ?? 60), Number(parsed.get("--rate") ?? 2), cancellation.signal))}\n`,
    );
    return;
  }
  if (options.length > 0)
    throw new NatsFixtureError(
      "This command accepts no options; edit aio-nats/fixture.config.json to change its port.",
    );
  if (command === "start") {
    process.stdout.write(
      `${JSON.stringify(await lifecycle.ensure(cancellation.signal), undefined, 2)}\n`,
    );
  } else if (command === "status") {
    process.stdout.write(`${JSON.stringify(await lifecycle.status(), undefined, 2)}\n`);
  } else if (command === "stop") {
    await lifecycle.stop();
    process.stdout.write("Local AIO NATS stopped; owned material removed.\n");
  } else {
    throw new NatsFixtureError(
      "Usage: npm run dev -- nats <start|status|publish|stop> [--seconds value --rate value]",
    );
  }
}

void main().catch((error: unknown) => {
  // Native command/SDK errors may contain private material; print only a fixed recovery.
  if (cancellation.signal.aborted) {
    process.stderr.write(
      "Local AIO NATS cancellation was requested. Check fixture status and recovery instructions; retained ownership evidence is preserved if cleanup could not be confirmed.\n",
    );
    return;
  }
  process.stderr.write(
    `${error instanceof NatsFixtureError ? error.message : "Local AIO NATS command failed. Check Docker/OpenSSL, the configured port, private ownership files and aio-nats/README.md recovery instructions."}\n`,
  );
  process.exitCode = 1;
});
