import { join } from "node:path";

import { AtomicObservationFileStore } from "../../node/kafka-observation-file-store";
import { AtomicKafkaQueryFileStore } from "../../node/kafka-query-file-store";
import { UnavailableKafkaProfileStore } from "../../../features/kafka/application";
import type { KafkaBackendFacade } from "../../../features/kafka/facade";
import { PluginRuntime } from "../../node/plugins/runtime";
import { PluginStore } from "../../node/plugins/store";
import { LegacyKafkaConnectionTemplateFile } from "../../node/legacy-connection-template-file";
import { AtomicKafkaTrustRecipeFileStore } from "../../node/kafka-trust-recipe-file-store";
import { createKafkaBackend } from "../../node/kafka-backend";
import { AtomicKafkaProfileFileStore } from "../../node/kafka-profile-file-store";
import { AtomicKafkaOperationalPreferenceFileStore } from "../../node/kafka-operational-preference-file-store";
import { AtomicKafkaRuleFileStore } from "../../node/kafka-rule-file-store";
import { AtomicKafkaTopicConfigurationHistoryFileStore } from "../../node/kafka-topic-configuration-history-file-store";

import {
  initializeElectronProfileProtection,
  type ElectronSafeStoragePort,
} from "./electron-profile-protection";

export interface ElectronKafkaBackendOptions {
  readonly platform: NodeJS.Platform;
  readonly safeStorage: ElectronSafeStoragePort;
  readonly userDataPath: string;
  readonly plugins?: PluginRuntime;
}

export async function createElectronKafkaBackend(
  options: ElectronKafkaBackendOptions,
): Promise<KafkaBackendFacade> {
  const protection = await initializeElectronProfileProtection(
    options.safeStorage,
    options.platform,
  );
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
  const preferenceStore = new AtomicKafkaOperationalPreferenceFileStore(
    join(options.userDataPath, "preferences", "kafka-operational-preferences.json"),
  );
  const plugins =
    options.plugins ??
    new PluginRuntime({
      store: new PluginStore(join(options.userDataPath, "plugins")),
    });
  const backend = createKafkaBackend(
    profileStore,
    legacySource,
    ruleStore,
    topicConfigurationHistoryStore,
    undefined,
    preferenceStore,
    new AtomicKafkaTrustRecipeFileStore(
      join(options.userDataPath, "templates", "trust-acquisition-recipes.json"),
    ),
    plugins,
    new AtomicKafkaQueryFileStore(join(options.userDataPath, "queries", "kafka-queries.json")),
    new AtomicObservationFileStore(
      join(options.userDataPath, "history", "kafka-observations.json"),
    ),
  );
  await plugins.start();
  return backend;
}
