import {
  InMemoryKafkaTrustRecipeStore,
  InMemoryKafkaOperationalPreferenceStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaRuleStore,
  InMemoryKafkaTopicConfigurationHistoryStore,
  KafkaApplicationSession,
  KafkaQueryLibrary,
  type KafkaQueryStore,
  KafkaTrustRecipeLibrary,
  KafkaLiveRuleRuntime,
  KafkaOperationalPreferenceService,
  KafkaProfileService,
  KafkaRuleService,
  KafkaTopicConfigurationService,
  KafkaTrustAcquisitionService,
  type KafkaLegacyTemplateSource,
  type KafkaTrustRecipeStore,
  type KafkaProfileRecord,
  type KafkaProfileStore,
  type KafkaOperationalPreferenceStore,
  type KafkaRemoteTrustPort,
  type KafkaRuleStore,
  type KafkaTopicConfigurationHistoryStore,
} from "../../features/kafka/application";
import type { KafkaMessage } from "../../features/kafka/contracts";
import { protectKafkaRecord } from "../../features/kafka/application/record-protection";
import { KafkaBackendFacade } from "../../features/kafka/facade";
import {
  NodeBoundedJsonHttp,
  SchemaRegistryHttpAdapter,
  RedpandaTransformHttpAdapter,
  StreamSkopeKafkaEngine,
  StreamSkopeKafkaRuleEvaluator,
} from "../../features/kafka/engine";
import type { PluginRuntimePort } from "../../plugins/api";

import { NodeHttpsTrustAcquisition } from "./https-trust-acquisition";
import { createHostTrustMaterialDecoder } from "./trust-material-decoder";
import { Ssh2KafkaRemoteTrustAdapter } from "./ssh2-kafka-remote-trust-adapter";

const browserProfileCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
} as const;

const browserRecipeCapability = {
  durability: "session",
  state: "ready",
} as const;

const browserRuleCapability = {
  durability: "session",
  state: "ready",
} as const;

const browserPreferenceCapability = {
  durability: "session",
  state: "ready",
} as const;

const browserTopicConfigurationHistoryCapability = {
  durability: "session",
  state: "ready",
} as const;

export function createBrowserKafkaProfileStore(
  initialRecords: readonly KafkaProfileRecord[] = [],
): InMemoryKafkaProfileStore {
  return new InMemoryKafkaProfileStore(browserProfileCapability, initialRecords);
}

export function createKafkaBackend(
  profileStore: KafkaProfileStore = createBrowserKafkaProfileStore(),
  legacySource?: KafkaLegacyTemplateSource,
  ruleStore: KafkaRuleStore = new InMemoryKafkaRuleStore(browserRuleCapability),
  topicConfigurationHistoryStore: KafkaTopicConfigurationHistoryStore = new InMemoryKafkaTopicConfigurationHistoryStore(
    browserTopicConfigurationHistoryCapability,
  ),
  remoteTrust: KafkaRemoteTrustPort = new Ssh2KafkaRemoteTrustAdapter(),
  preferenceStore: KafkaOperationalPreferenceStore = new InMemoryKafkaOperationalPreferenceStore(
    browserPreferenceCapability,
  ),
  recipeStore: KafkaTrustRecipeStore = new InMemoryKafkaTrustRecipeStore(browserRecipeCapability),
  plugins?: PluginRuntimePort,
  queryStore?: KafkaQueryStore,
): KafkaBackendFacade {
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(ruleStore, evaluator);
  const preferences = new KafkaOperationalPreferenceService(preferenceStore);
  const session = new KafkaApplicationSession(
    new StreamSkopeKafkaEngine({
      protectRecord: (message): KafkaMessage => {
        const snapshot = preferences.currentSnapshot();
        if (snapshot.store.state !== "ready") throw new Error("Record protection is unavailable.");
        return protectKafkaRecord(message, snapshot.preferences.protection);
      },
    }),
  );
  const recipes = new KafkaTrustRecipeLibrary({ store: recipeStore }, legacySource);
  const trustDecoder = createHostTrustMaterialDecoder();
  const trustAcquisitions: KafkaTrustAcquisitionService = new KafkaTrustAcquisitionService(
    recipes,
    remoteTrust,
    trustDecoder,
    {
      https: new NodeHttpsTrustAcquisition(),
      resolveProfileApiCa: (id, revision, signal) =>
        profiles.resolveAcquisitionApiCa(id, revision, signal),
      resolveProfileBinding: (id, revision, reference, signal) =>
        profiles.resolveAcquisitionBinding(id, revision, reference, signal),
    },
  );
  const profiles = new KafkaProfileService(profileStore, trustDecoder, {
    trustAcquisitions,
    resolveRecipe: recipes.resolve.bind(recipes),
  });
  const serviceHttp = new NodeBoundedJsonHttp();
  return new KafkaBackendFacade(
    session,
    profiles,
    recipes,
    rules,
    new KafkaLiveRuleRuntime(rules, evaluator),
    new KafkaTopicConfigurationService(session, topicConfigurationHistoryStore),
    {
      ...(plugins === undefined ? {} : { plugins }),
      preferences,
      queries: new KafkaQueryLibrary(queryStore),
      schemaRegistry: new SchemaRegistryHttpAdapter(serviceHttp),
      transforms: new RedpandaTransformHttpAdapter(serviceHttp),
      trustAcquisitions,
    },
  );
}
