import { join } from "node:path";

import { UnavailableRepairJobStore } from "../../../features/kafka/application/repair-journal";
import { AtomicRepairFileStore } from "../../node/kafka-repair-file-store";
import { AtomicObservationFileStore } from "../../node/kafka-observation-file-store";
import { AtomicKafkaQueryFileStore } from "../../node/kafka-query-file-store";
import { UnavailableKafkaProfileStore } from "../../../features/kafka/application";
import { PluginRuntime } from "../../node/plugins/runtime";
import { PluginStore } from "../../node/plugins/store";
import { LegacyKafkaConnectionTemplateFile } from "../../node/legacy-connection-template-file";
import { AtomicKafkaTrustRecipeFileStore } from "../../node/kafka-trust-recipe-file-store";
import { createKafkaBackend, type NodeKafkaBackend } from "../../node/kafka-backend";
import { AtomicKafkaProfileFileStore } from "../../node/kafka-profile-file-store";
import { DesktopOperationalPreferenceStore } from "../../node/desktop-operational-preference-store";
import { AtomicKafkaRuleFileStore } from "../../node/kafka-rule-file-store";
import { AtomicKafkaTopicConfigurationHistoryFileStore } from "../../node/kafka-topic-configuration-history-file-store";
import type { PluginNetworkTransport } from "../../node/plugins/network-transport";

import type { PluginPackageFilePicker } from "./plugin-file-picker";
import {
  initializeElectronProfileProtection,
  type ElectronProfileProtection,
  type ElectronSafeStoragePort,
} from "./electron-profile-protection";

export interface ElectronKafkaBackendOptions {
  readonly platform: NodeJS.Platform;
  readonly safeStorage: ElectronSafeStoragePort;
  readonly userDataPath: string;
  readonly plugins?: PluginRuntime;
  readonly profileProtection?: ElectronProfileProtection;
  readonly choosePackageFile?: PluginPackageFilePicker;
  readonly networkTransport?: PluginNetworkTransport;
}

export async function createElectronKafkaBackend(
  options: ElectronKafkaBackendOptions,
): Promise<NodeKafkaBackend> {
  const protection =
    options.profileProtection ??
    (await initializeElectronProfileProtection(options.safeStorage, options.platform));
  const profileStore =
    protection.protector === undefined
      ? new UnavailableKafkaProfileStore(protection.capability)
      : new AtomicKafkaProfileFileStore(
          join(options.userDataPath, "profiles", "kafka-profiles.json"),
          protection.protector,
          protection.capability,
        );
  const legacySource = new LegacyKafkaConnectionTemplateFile(
    join(options.userDataPath, "templates", "kafka-connection-templates.json"),
  );
  const ruleStore = new AtomicKafkaRuleFileStore(
    join(options.userDataPath, "rules", "kafka-rules.json"),
  );
  const topicConfigurationHistoryStore = new AtomicKafkaTopicConfigurationHistoryFileStore(
    join(options.userDataPath, "history", "kafka-topic-configuration-history.json"),
  );
  const preferenceStore = new DesktopOperationalPreferenceStore(options.userDataPath);
  const plugins =
    options.plugins ??
    new PluginRuntime({
      store: new PluginStore(join(options.userDataPath, "plugins")),
      ...(options.networkTransport === undefined
        ? {}
        : { networkTransport: options.networkTransport }),
      ...(protection.protector === undefined ? {} : { networkProtector: protection.protector }),
      ...(options.choosePackageFile === undefined
        ? {}
        : { choosePackageFile: options.choosePackageFile }),
    });
  const backend = createKafkaBackend({
    profileStore,
    ...(protection.protector === undefined
      ? {
          repairStore: new UnavailableRepairJobStore(),
        }
      : {
          repairStore: new AtomicRepairFileStore(
            join(options.userDataPath, "history", "kafka-repair-jobs.json"),
            protection.protector,
          ),
        }),
    legacySource,
    ruleStore,
    topicConfigurationHistoryStore,
    preferenceStore,
    recipeStore: new AtomicKafkaTrustRecipeFileStore(
      join(options.userDataPath, "templates", "trust-acquisition-recipes.json"),
    ),
    plugins,
    queryStore: new AtomicKafkaQueryFileStore(
      join(options.userDataPath, "queries", "kafka-queries.json"),
    ),
    observationStore: new AtomicObservationFileStore(
      join(options.userDataPath, "history", "kafka-observations.json"),
    ),
  });
  try {
    await plugins.start();
  } catch (error) {
    try {
      await backend.shutdown();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "Desktop Kafka startup failed and provider cleanup did not complete.",
        { cause: cleanupError },
      );
    }
    throw error;
  }
  return backend;
}
