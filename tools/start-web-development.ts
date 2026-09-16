import { join, resolve } from "node:path";

import { createBrowserKafkaProfileStore, createKafkaBackend } from "../src/platform/electron/main";

import { FileFixtureOwnershipStore } from "./kafka-fixture/file-ownership-store";
import { KafkaFixtureLifecycle } from "./kafka-fixture/lifecycle";
import { NodeFixtureRuntime } from "./kafka-fixture/node-runtime";
import { defaultOwnedFixtureRequest } from "./kafka-fixture/owned-request";
import { webDevelopmentOptions } from "./web-development-options";
import {
  prepareLocalAioDevelopmentProfile,
  type LocalAioDevelopmentProfilePreparation,
} from "./kafka-fixture/development-profile";
import {
  openDevelopmentBrowser,
  startWebDevelopmentCommand,
  type RunningWebDevelopmentCommand,
} from "./web-development-command";

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
    },
    createBackend: async () => {
      const profileStore = createBrowserKafkaProfileStore();
      profilePreparation = await prepareLocalAioDevelopmentProfile(profileStore, {
        repositoryRoot,
      });
      return createKafkaBackend(profileStore);
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
