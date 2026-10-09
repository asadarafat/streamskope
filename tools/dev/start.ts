import { join, resolve } from "node:path";

import {
  createBrowserKafkaProfileStore,
  createKafkaBackend,
} from "../../src/platform/node/kafka-backend";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { DEVELOPMENT_VERSION } from "../../src/plugins/compatibility";
import { STREAMSKOPE_RELEASE } from "../../src/plugins/host-release";
import { createNatsBackend } from "../../src/platform/node/nats-backend";
import { createKafkaProviderEndpoint } from "../../src/platform/node/kafka-provider";
import { createNatsProviderEndpoint } from "../../src/platform/node/nats-provider";
import { ProviderHostRegistry } from "../../src/platform/node/provider-host";
import { InMemoryNatsProfileStore } from "../../src/features/nats/application";

import { FileFixtureOwnershipStore } from "./kafka-fixture/file-ownership-store";
import { KafkaFixtureLifecycle } from "./kafka-fixture/lifecycle";
import { NodeFixtureRuntime, defaultOwnedFixtureRequest } from "./kafka-fixture/node-runtime";
import { openDevelopmentBrowser, webDevelopmentOptions } from "./launch";
import {
  prepareLocalAioDevelopmentProfile,
  type LocalAioDevelopmentProfilePreparation,
} from "./kafka-fixture/development-profile";
import { startWebDevelopmentCommand, type RunningWebDevelopmentCommand } from "./session";
import { DevelopmentPluginCatalog } from "./plugin-catalog";
import { NatsFixtureLifecycle } from "./nats-fixture/lifecycle";
import { prepareLocalNatsDevelopmentProfile } from "./nats-fixture/development-profile";

let launch: RunningWebDevelopmentCommand | undefined;
let closing: Promise<void> | undefined;

function close(): Promise<void> {
  closing ??= launch?.close() ?? Promise.resolve();
  return closing;
}

function stop(): void {
  void close().catch((error: unknown) => {
    const summary = error instanceof Error ? error.message : "Unknown shutdown failure.";
    process.stderr.write(`StreamSkope web development shutdown failed: ${summary}\n`);
    process.exitCode = 1;
  });
}

async function start(): Promise<void> {
  const repositoryRoot = resolve(process.cwd());
  let profilePreparation: LocalAioDevelopmentProfilePreparation | undefined;
  let natsProfilePreparation: "seeded" | "unchanged" | "unavailable" | undefined;
  launch = await startWebDevelopmentCommand(webDevelopmentOptions(process.env, repositoryRoot), {
    prepare: async () => {
      process.stdout.write(
        "Checking owned Local AIO Kafka; starting stopped services if needed...\n",
      );
      const lifecycle = new KafkaFixtureLifecycle(
        new NodeFixtureRuntime(repositoryRoot),
        new FileFixtureOwnershipStore(join(repositoryRoot, "aio-kafka", "ownership", "records")),
      );
      await lifecycle.ensureOwned(await defaultOwnedFixtureRequest(repositoryRoot));
      process.stdout.write("Local AIO Kafka, OAuth and Schema Registry are ready on loopback.\n");
      await new NatsFixtureLifecycle(repositoryRoot).ensure();
      process.stdout.write(
        "Local AIO NATS is ready with token authentication and verified TLS on loopback.\n",
      );
    },
    createProviders: async () => {
      const profileStore = createBrowserKafkaProfileStore();
      profilePreparation = await prepareLocalAioDevelopmentProfile(profileStore, {
        repositoryRoot,
      });
      const plugins = new PluginRuntime({
        store: new PluginStore(join(repositoryRoot, ".cache", "development-plugins")),
        ...(STREAMSKOPE_RELEASE === `v${DEVELOPMENT_VERSION}`
          ? {
              catalog: new DevelopmentPluginCatalog(join(repositoryRoot, "dist", "plugin-package")),
              persistCatalog: false,
            }
          : {}),
      });
      const backend = createKafkaBackend({ profileStore, plugins });
      let nats: ReturnType<typeof createNatsBackend> | undefined;
      try {
        await plugins.start();
        const natsProfileStore = new InMemoryNatsProfileStore();
        natsProfilePreparation = await prepareLocalNatsDevelopmentProfile(
          natsProfileStore,
          repositoryRoot,
        );
        nats = createNatsBackend({ profileStore: natsProfileStore });
        return {
          providers: new ProviderHostRegistry([
            createKafkaProviderEndpoint(backend),
            createNatsProviderEndpoint(nats),
          ]),
          pluginAsset: plugins.rendererAsset.bind(plugins),
          exportFiles: backend.exportFiles,
        };
      } catch (error) {
        const results = await Promise.allSettled(
          [backend, nats]
            .filter((owner) => owner !== undefined)
            .map((owner) => Promise.resolve().then(() => owner.shutdown())),
        );
        const failures = results.flatMap((result) =>
          result.status === "rejected" ? [result.reason as unknown] : [],
        );
        if (failures.length > 0)
          throw new AggregateError(
            [error, ...failures],
            "Development provider startup cleanup failed.",
            { cause: error },
          );
        throw error;
      }
    },
    openBrowser: openDevelopmentBrowser,
  });
  const profileStatus =
    profilePreparation?.status === "seeded"
      ? `${profilePreparation.profileName} is ready as a session-only default.\n`
      : profilePreparation?.status === "unavailable"
        ? `Local AIO Kafka default is unavailable. ${profilePreparation.recovery}\n`
        : "";
  process.stdout.write(
    `${
      launch.reused
        ? "StreamSkope web development is already running."
        : "StreamSkope web development is ready."
    }\n${
      launch.browserOpenError === null
        ? "The authorized browser launch was requested."
        : `The browser did not open automatically: ${launch.browserOpenError}`
    }\n${profileStatus}Manual launch URL:\n${launch.browserUrl}\n`,
  );
  if (STREAMSKOPE_RELEASE === `v${DEVELOPMENT_VERSION}`) {
    process.stdout.write(
      "Development plugins use local packages. Run npm run package -- plugin, then Preferences > Plugins > Refresh plugins > Install or Update.\n",
    );
  }
  if (natsProfilePreparation === "seeded")
    process.stdout.write(
      "Local AIO NATS is ready as a session-only default. Subscribe to streamskope.fixture.>, then run npm run dev -- nats publish in another terminal.\n",
    );
  if (launch.reused) {
    return;
  }
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}

void start().catch((error: unknown) => {
  const summary = error instanceof Error ? error.message : "Unknown startup failure.";
  process.stderr.write(`StreamSkope web development startup failed: ${summary}\n`);
  process.exitCode = 1;
});
