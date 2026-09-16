import { join } from "node:path";

import { UnavailableKafkaProfileStore } from "../../../features/kafka/application";
import type { KafkaBackendFacade } from "../../../features/kafka/facade";

import {
  initializeElectronProfileProtection,
  type ElectronSafeStoragePort,
} from "./electron-profile-protection";
import { AtomicKafkaConnectionTemplateFileStore } from "./kafka-connection-template-file-store";
import { AtomicKafkaTrustRecipeFileStore } from "./kafka-trust-recipe-file-store";
import { createKafkaBackend } from "./kafka-backend";
import { AtomicKafkaProfileFileStore } from "./kafka-profile-file-store";
import { AtomicKafkaOperationalPreferenceFileStore } from "./kafka-operational-preference-file-store";
import { AtomicKafkaRuleFileStore } from "./kafka-rule-file-store";
import { AtomicKafkaTopicConfigurationHistoryFileStore } from "./kafka-topic-configuration-history-file-store";

export interface ElectronKafkaBackendOptions {
  readonly platform: NodeJS.Platform;
  readonly safeStorage: ElectronSafeStoragePort;
  readonly userDataPath: string;
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
  const templateStore = new AtomicKafkaConnectionTemplateFileStore(
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
  return createKafkaBackend(
    profileStore,
    templateStore,
    ruleStore,
    topicConfigurationHistoryStore,
    undefined,
    preferenceStore,
    new AtomicKafkaTrustRecipeFileStore(
      join(options.userDataPath, "templates", "trust-acquisition-recipes.json"),
    ),
  );
}
