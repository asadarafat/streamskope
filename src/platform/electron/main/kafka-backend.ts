import {
  InMemoryKafkaConnectionTemplateStore,
  InMemoryKafkaTrustRecipeStore,
  InMemoryKafkaOperationalPreferenceStore,
  InMemoryKafkaProfileStore,
  InMemoryKafkaRuleStore,
  InMemoryKafkaTopicConfigurationHistoryStore,
  KafkaApplicationSession,
  KafkaConnectionTemplateService,
  KafkaLiveRuleRuntime,
  KafkaOperationalPreferenceService,
  KafkaProfileService,
  KafkaRuleService,
  KafkaTopicConfigurationService,
  KafkaTrustAcquisitionService,
  type KafkaConnectionTemplateStore,
  type KafkaTrustRecipeStore,
  type KafkaProfileRecord,
  type KafkaProfileStore,
  type KafkaOperationalPreferenceStore,
  type KafkaRemoteTrustPort,
  type KafkaRuleStore,
  type KafkaTopicConfigurationHistoryStore,
} from "../../../features/kafka/application";
import { KafkaBackendFacade } from "../../../features/kafka/facade";
import {
  NodeBoundedJsonHttp,
  SchemaRegistryHttpAdapter,
  RedpandaTransformHttpAdapter,
  StreamSkopeKafkaEngine,
  StreamSkopeKafkaRuleEvaluator,
} from "../../../features/kafka/engine";

import { NodeHttpsTrustAcquisition } from "./https-trust-acquisition";
import { createHostTrustMaterialDecoder } from "./trust-material-decoder";
import { Ssh2KafkaRemoteTrustAdapter } from "./ssh2-kafka-remote-trust-adapter";

const browserProfileCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
} as const;

const browserTemplateCapability = {
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
  templateStore: KafkaConnectionTemplateStore = new InMemoryKafkaConnectionTemplateStore(
    browserTemplateCapability,
  ),
  ruleStore: KafkaRuleStore = new InMemoryKafkaRuleStore(browserRuleCapability),
  topicConfigurationHistoryStore: KafkaTopicConfigurationHistoryStore = new InMemoryKafkaTopicConfigurationHistoryStore(
    browserTopicConfigurationHistoryCapability,
  ),
  remoteTrust: KafkaRemoteTrustPort = new Ssh2KafkaRemoteTrustAdapter(),
  preferenceStore: KafkaOperationalPreferenceStore = new InMemoryKafkaOperationalPreferenceStore(
    browserPreferenceCapability,
  ),
  recipeStore: KafkaTrustRecipeStore = new InMemoryKafkaTrustRecipeStore(browserTemplateCapability),
): KafkaBackendFacade {
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(ruleStore, evaluator);
  const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const templates = new KafkaConnectionTemplateService(templateStore, { store: recipeStore });
  const trustDecoder = createHostTrustMaterialDecoder();
  const trustAcquisitions: KafkaTrustAcquisitionService = new KafkaTrustAcquisitionService(
    templates,
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
    resolveRecipe: templates.recipes.resolve.bind(templates.recipes),
  });
  const serviceHttp = new NodeBoundedJsonHttp();
  return new KafkaBackendFacade(
    session,
    profiles,
    templates,
    rules,
    new KafkaLiveRuleRuntime(rules, evaluator),
    new KafkaTopicConfigurationService(session, topicConfigurationHistoryStore),
    {
      preferences: new KafkaOperationalPreferenceService(preferenceStore),
      schemaRegistry: new SchemaRegistryHttpAdapter(serviceHttp),
      transforms: new RedpandaTransformHttpAdapter(serviceHttp),
      trustAcquisitions,
    },
  );
}
