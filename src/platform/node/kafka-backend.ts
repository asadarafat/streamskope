import { ConnectHttpAdapter } from "../../features/kafka/engine/connect-http";
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
import type { ObservationStore } from "../../features/kafka/application/observation-store";
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
import { createHostRecordCodec } from "./record-codec";
import { createHostRecordPipeline } from "./record-pipeline";
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

export interface KafkaBackendOptions {
  readonly profileStore?: KafkaProfileStore;
  readonly legacySource?: KafkaLegacyTemplateSource;
  readonly ruleStore?: KafkaRuleStore;
  readonly topicConfigurationHistoryStore?: KafkaTopicConfigurationHistoryStore;
  readonly remoteTrust?: KafkaRemoteTrustPort;
  readonly preferenceStore?: KafkaOperationalPreferenceStore;
  readonly recipeStore?: KafkaTrustRecipeStore;
  readonly plugins?: PluginRuntimePort;
  readonly queryStore?: KafkaQueryStore;
  readonly observationStore?: ObservationStore;
}

export function createKafkaBackend(options: KafkaBackendOptions = {}): KafkaBackendFacade {
  const {
    profileStore = createBrowserKafkaProfileStore(),
    legacySource,
    ruleStore = new InMemoryKafkaRuleStore(browserRuleCapability),
    topicConfigurationHistoryStore = new InMemoryKafkaTopicConfigurationHistoryStore(
      browserTopicConfigurationHistoryCapability,
    ),
    remoteTrust = new Ssh2KafkaRemoteTrustAdapter(),
    preferenceStore = new InMemoryKafkaOperationalPreferenceStore(browserPreferenceCapability),
    recipeStore = new InMemoryKafkaTrustRecipeStore(browserRecipeCapability),
    plugins,
    queryStore,
    observationStore,
  } = options;
  const evaluator = new StreamSkopeKafkaRuleEvaluator();
  const rules = new KafkaRuleService(ruleStore, evaluator);
  const preferences = new KafkaOperationalPreferenceService(preferenceStore);
  const serviceHttp = new NodeBoundedJsonHttp();
  const schemaRegistry = new SchemaRegistryHttpAdapter(serviceHttp);
  const structuredWorker = createHostRecordCodec();
  const session = new KafkaApplicationSession(
    new StreamSkopeKafkaEngine(
      createHostRecordPipeline(
        () => {
          const snapshot = preferences.currentSnapshot();
          if (snapshot.store.state !== "ready")
            throw new Error("Record preferences are unavailable.");
          return snapshot.preferences;
        },
        { codec: structuredWorker, lookup: schemaRegistry },
      ),
    ),
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
  return new KafkaBackendFacade(
    session,
    profiles,
    recipes,
    rules,
    new KafkaLiveRuleRuntime(rules, evaluator),
    new KafkaTopicConfigurationService(session, topicConfigurationHistoryStore),
    {
      ...(observationStore ? { observationStore } : {}),
      replayConnections: new StreamSkopeKafkaEngine(),
      ...(plugins === undefined ? {} : { plugins }),
      preferences,
      queries: new KafkaQueryLibrary(queryStore),
      schemaRegistry,
      schemaLookup: schemaRegistry,
      recordCodec: structuredWorker,
      sampleGenerator: structuredWorker,
      transforms: new RedpandaTransformHttpAdapter(serviceHttp),
      connect: new ConnectHttpAdapter(serviceHttp),
      trustAcquisitions,
    },
  );
}
