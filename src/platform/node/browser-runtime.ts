import { join } from "node:path";

import { parseHostCommand } from "../../features/kafka/contracts";
import {
  operationalDiagnostic,
  OperationalDiagnosticError,
  type OperationalDiagnosticCode,
} from "../diagnostics";

import { BrowserPluginFiles } from "./browser-plugin-files";
import { createKafkaBackend } from "./kafka-backend";
import { createKafkaProviderEndpoint } from "./kafka-provider";
import { AtomicObservationFileStore } from "./kafka-observation-file-store";
import { AtomicKafkaOperationalPreferenceFileStore } from "./kafka-operational-preference-file-store";
import { AtomicKafkaProfileFileStore } from "./kafka-profile-file-store";
import { AtomicKafkaQueryFileStore } from "./kafka-query-file-store";
import { AtomicKafkaRuleFileStore } from "./kafka-rule-file-store";
import { AtomicKafkaTopicConfigurationHistoryFileStore } from "./kafka-topic-configuration-history-file-store";
import { AtomicKafkaTrustRecipeFileStore } from "./kafka-trust-recipe-file-store";
import { createNatsBackend } from "./nats-backend";
import { AtomicNatsProfileFileStore } from "./nats-profile-file-store";
import { createNatsProviderEndpoint } from "./nats-provider";
import { PluginRuntime } from "./plugins/runtime";
import { PluginStore } from "./plugins/store";
import { ProviderHostRegistry, ProviderWireValidationError } from "./provider-host";
import { WebGatewayCleanupUnconfirmedError } from "./web-gateway-errors";
import { openPassphraseVault, type PassphraseVault } from "./vault/passphrase-vault";
import type { WebGatewayRuntime } from "./web-gateway";

// A failed cleanup must retain the kernel lease until this process exits.
const blockedVaults = new Set<PassphraseVault>();

/** Production composition has no fixture setup, Vite, Electron, or implicit profile. */
export async function openBrowserRuntime(
  dataRoot: string,
  passphrase: string,
  mode: "create" | "unlock",
): Promise<WebGatewayRuntime> {
  const vault = await openPassphraseVault({ dataRoot, passphrase, mode });
  const files = new BrowserPluginFiles();
  let ownedPlugins: PluginRuntime | undefined;
  let ownedKafka: ReturnType<typeof createKafkaBackend> | undefined;
  let ownedNats: ReturnType<typeof createNatsBackend> | undefined;
  let ownedProviders: ProviderHostRegistry | undefined;
  let startupCode: OperationalDiagnosticCode = "PLUGIN_RUNTIME_START_FAILED";
  try {
    const plugins = (ownedPlugins = new PluginRuntime({
      store: new PluginStore(join(dataRoot, "plugins")),
      networkProtector: vault.protector,
      durableSettings: true,
      choosePackageFile: (signal): Promise<Uint8Array | null> => files.chooseFile(signal),
    }));
    startupCode = "KAFKA_RUNTIME_START_FAILED";
    const kafka = (ownedKafka = createKafkaBackend({
      profileStore: new AtomicKafkaProfileFileStore(
        vault.paths.kafkaProfiles,
        vault.protector,
        vault.capability,
      ),
      plugins,
      ruleStore: new AtomicKafkaRuleFileStore(join(dataRoot, "rules", "kafka-rules.json")),
      preferenceStore: new AtomicKafkaOperationalPreferenceFileStore(
        join(dataRoot, "workbench", "kafka-operational-preferences.json"),
      ),
      topicConfigurationHistoryStore: new AtomicKafkaTopicConfigurationHistoryFileStore(
        join(dataRoot, "history", "kafka-topic-configuration-history.json"),
      ),
      queryStore: new AtomicKafkaQueryFileStore(join(dataRoot, "queries", "kafka-queries.json")),
      recipeStore: new AtomicKafkaTrustRecipeFileStore(
        join(dataRoot, "templates", "trust-acquisition-recipes.json"),
      ),
      observationStore: new AtomicObservationFileStore(
        join(dataRoot, "history", "kafka-observations.json"),
      ),
    }));
    startupCode = "NATS_RUNTIME_START_FAILED";
    const nats = (ownedNats = createNatsBackend({
      profileStore: new AtomicNatsProfileFileStore(vault.paths.natsProfiles, vault.protector, {
        protection: "passphrase-protected",
      }),
    }));
    startupCode = "PRIVATE_HOST_START_FAILED";
    const kafkaEndpoint = createKafkaProviderEndpoint(kafka);
    const providers = (ownedProviders = new ProviderHostRegistry([
      {
        ...kafkaEndpoint,
        dispatch: async (wire): Promise<unknown> => {
          let command;
          try {
            command = parseHostCommand(wire);
          } catch (error) {
            throw new ProviderWireValidationError("command", "Provider command is invalid.", {
              cause: error,
            });
          }
          return files.run(command.id, () => kafkaEndpoint.dispatch(wire));
        },
      },
      createNatsProviderEndpoint(nats),
    ]));
    startupCode = "PLUGIN_RUNTIME_START_FAILED";
    await plugins.start();
    return {
      providers,
      pluginAsset: (pathname) => plugins.rendererAsset(pathname),
      handleAuthorizedRequest: (request, response) => files.handleRequest(request, response),
      discardPluginFile: (commandId) => files.discard(commandId),
      lock: async (): Promise<void> => {
        files.close();
        await providers.shutdown();
        try {
          await vault.lock();
        } catch (cause) {
          throw new OperationalDiagnosticError("VAULT_LOCK_FAILED", { cause });
        }
      },
    };
  } catch (error) {
    files.close();
    const cleanup: Promise<void>[] = [];
    const clean = async (
      operation: () => Promise<void>,
      fallback: OperationalDiagnosticCode,
    ): Promise<void> => {
      try {
        await operation();
      } catch (cause) {
        const diagnostic = operationalDiagnostic(cause, fallback);
        throw new OperationalDiagnosticError(diagnostic.code, {
          cause,
          correlationId: diagnostic.correlationId,
        });
      }
    };
    if (ownedProviders !== undefined)
      cleanup.push(
        clean(ownedProviders.shutdown.bind(ownedProviders), "PROVIDER_CLEANUP_UNCONFIRMED"),
      );
    else {
      if (ownedKafka !== undefined)
        cleanup.push(clean(ownedKafka.shutdown.bind(ownedKafka), "KAFKA_CLEANUP_UNCONFIRMED"));
      if (ownedNats !== undefined)
        cleanup.push(clean(ownedNats.shutdown.bind(ownedNats), "NATS_CLEANUP_UNCONFIRMED"));
      if (ownedPlugins !== undefined)
        cleanup.push(clean(ownedPlugins.close.bind(ownedPlugins), "CLEANUP_UNCONFIRMED"));
    }
    const results = await Promise.allSettled(cleanup);
    const failures = results
      .filter((result) => result.status === "rejected")
      .map((result) => result.reason as unknown);
    if (failures.length > 0) {
      blockedVaults.add(vault);
      const diagnostic = operationalDiagnostic(
        failures.length === 1 ? failures[0] : undefined,
        "CLEANUP_UNCONFIRMED",
      );
      const code = diagnostic.code;
      throw new WebGatewayCleanupUnconfirmedError(
        {
          cause: new AggregateError(failures, "Runtime startup cleanup failed.", { cause: error }),
          correlationId: diagnostic.correlationId,
        },
        code === "KAFKA_CLEANUP_UNCONFIRMED" ||
          code === "NATS_CLEANUP_UNCONFIRMED" ||
          code === "PROVIDER_CLEANUP_UNCONFIRMED" ||
          code === "VAULT_LOCK_FAILED"
          ? code
          : "CLEANUP_UNCONFIRMED",
      );
    }
    try {
      await vault.lock();
    } catch (cause) {
      blockedVaults.add(vault);
      throw new WebGatewayCleanupUnconfirmedError({ cause }, "VAULT_LOCK_FAILED");
    }
    throw new OperationalDiagnosticError(startupCode, { cause: error });
  }
}
