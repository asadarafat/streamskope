import type {
  JsonValue,
  PluginSnapshot,
  PluginCatalogSnapshot,
  PluginDeliverySnapshot,
  PluginPackageInspection,
  PluginNetworkSnapshot,
  PluginNetworkTestResult,
  PluginExitPrompt,
  PluginChangePrompt,
} from "../../../plugins/contracts";

import type { TopicCatalogResults } from "./topic-catalog-protocol";
import type { RecordAnalysisResults } from "./record-analysis-protocol";
import type { RecordLocatorResults } from "./record-locator-protocol";
import type { RecordExportResults } from "./record-export-protocol";
import {
  isAclReviewCommandName,
  type AclReviewCommandName,
  type AclReviewResults,
} from "./acl-review-commands";
import type { KafkaQueryLibrarySnapshot } from "./query-library";
import type { TrustAcquisitionCommandResults } from "./remote-trust-types";
import type { TrustRecipeCommandResults } from "./trust-recipe-types";
import type { ProfileBindingDetailResult } from "./profile-binding";
import type { KafkaClusterDetailsExportResult } from "./cluster-diagnostics-types";
import type { KafkaLatencyExportResult } from "./latency-types";
import type { KafkaOperationalPreferenceResult } from "./operational-preference-types";
import type { HostCommandAccepted, HostCommandName } from "./types";

interface SpecificCommandResults
  extends
    TrustAcquisitionCommandResults,
    TrustRecipeCommandResults,
    AclReviewResults,
    RecordExportResults,
    RecordAnalysisResults,
    RecordLocatorResults,
    TopicCatalogResults {
  readonly "relationships.capture": {
    readonly correlationId: string;
    readonly graph: import("./relationships").RelationshipGraph;
  };
  readonly "observations.capture": {
    readonly correlationId: string;
    readonly capture: import("./observations").ObservationCapture;
  };
  readonly "observations.history": {
    readonly correlationId: string;
    readonly snapshot: import("./observations").ObservationSnapshot;
  };
  readonly "observations.clear": SpecificCommandResults["observations.history"];
  readonly "connect.list": {
    readonly correlationId: string;
    readonly inventory: import("./connect").ConnectInventory;
  };
  readonly "connect.load": {
    readonly correlationId: string;
    readonly detail: import("./connect").ConnectDetail;
  };
  readonly "connect.validate": {
    readonly correlationId: string;
    readonly validation: import("./connect").ConnectValidation;
  };
  readonly "connect.review": {
    readonly correlationId: string;
    readonly review: import("./connect").ConnectReview;
  };
  readonly "connect.apply": {
    readonly correlationId: string;
    readonly outcome: import("./connect").ConnectOutcome;
  };
  readonly "environments.capture": {
    readonly correlationId: string;
    readonly snapshot: import("./environment-snapshot").EnvironmentSnapshot;
  };
  readonly "environments.review": {
    readonly correlationId: string;
    readonly review: import("./environment-snapshot").EnvironmentReview;
  };
  readonly "environments.apply": {
    readonly correlationId: string;
    readonly outcome: import("./environment-snapshot").EnvironmentOutcome;
  };

  readonly "schemas.client": {
    readonly correlationId: string;
    readonly client: import("./schema-client").SchemaClient;
  };
  readonly "consumerGroups.reset.review": {
    readonly correlationId: string;
    readonly review: import("./offset-reset").OffsetResetReview;
  };
  readonly "consumerGroups.reset.apply": {
    readonly correlationId: string;
    readonly outcome: import("./offset-reset").OffsetResetOutcome;
  };
  readonly "records.repair.list": {
    readonly correlationId: string;
    readonly durability: "durable" | "session" | "unavailable";
    readonly jobs: readonly import("./repair-jobs").RepairJobSummary[];
  };
  readonly "records.replay.review": {
    readonly correlationId: string;
    readonly review: import("./record-replay").RecordReplayReview;
  };
  readonly "records.replay.apply": {
    readonly correlationId: string;
    readonly outcome: import("./record-replay").RecordReplayOutcome;
  };
  readonly "records.trace": {
    readonly correlationId: string;
    readonly trace: import("./correlation-trace").CorrelationTraceResult;
  };
  readonly "schemas.policy.load": {
    readonly correlationId: string;
    readonly baseline: import("./schema-policy").SchemaPolicyBaseline;
  };
  readonly "schemas.policy.review": {
    readonly correlationId: string;
    readonly review: import("./schema-policy").SchemaPolicyReview;
  };
  readonly "schemas.policy.apply": {
    readonly correlationId: string;
    readonly outcome: import("./schema-policy").SchemaPolicyOutcome;
  };
  readonly "schemas.change.review": {
    readonly correlationId: string;
    readonly review: import("./schema-changes").SchemaChangeReview;
  };
  readonly "schemas.change.apply": {
    readonly correlationId: string;
    readonly outcome: import("./schema-changes").SchemaChangeOutcome;
  };
  readonly "schemas.author": {
    readonly correlationId: string;
    readonly authoring: import("./schema-authoring").SchemaAuthoringResult;
  };
  readonly "schemas.samples": {
    readonly correlationId: string;
    readonly samples: import("./schema-samples").SchemaSamples;
  };
  readonly "records.batch.review": {
    readonly correlationId: string;
    readonly review: import("./schema-samples").RecordBatchReview;
  };
  readonly "records.batch.apply": {
    readonly correlationId: string;
    readonly outcome: import("./schema-samples").RecordBatchOutcome;
  };
  readonly "schemas.inspect": {
    readonly correlationId: string;
    readonly inspection: import("./schema-inspection").SchemaInspection;
  };
  readonly "records.decode": {
    readonly correlationId: string;
    readonly decoded: import("./record-codec").RecordDecodeResult;
  };
  readonly "writes.review": {
    readonly correlationId: string;
    readonly review: import("./reviewed-writes").KafkaWriteReview;
  };
  readonly "writes.apply": {
    readonly correlationId: string;
    readonly outcome: import("./reviewed-writes").KafkaWriteOutcome;
  };
  readonly "queries.list": {
    readonly correlationId: string;
    readonly snapshot: KafkaQueryLibrarySnapshot;
  };
  readonly "queries.put": SpecificCommandResults["queries.list"];
  readonly "queries.delete": SpecificCommandResults["queries.list"];
  readonly "plugin.execute": { readonly correlationId: string; readonly output: JsonValue };
  readonly "plugins.list": {
    readonly correlationId: string;
    readonly pluginSnapshot: PluginSnapshot;
  };
  readonly "plugins.catalog": {
    readonly correlationId: string;
    readonly pluginCatalog: PluginCatalogSnapshot;
  };
  readonly "plugins.delivery": {
    readonly correlationId: string;
    readonly pluginDelivery: PluginDeliverySnapshot;
  };
  readonly "plugins.network.get": {
    readonly correlationId: string;
    readonly pluginNetwork: PluginNetworkSnapshot;
  };
  readonly "plugins.network.update": SpecificCommandResults["plugins.network.get"];
  readonly "plugins.network.test": {
    readonly correlationId: string;
    readonly pluginNetworkTest: PluginNetworkTestResult;
  };
  readonly "plugins.package.inspect": {
    readonly correlationId: string;
    readonly pluginPackage: PluginPackageInspection | null;
  };
  readonly "plugins.package.change.prepare": SpecificCommandResults["plugins.change.prepare"];
  readonly "plugins.package.install": SpecificCommandResults["plugins.list"];
  readonly "plugins.change.prepare": {
    readonly correlationId: string;
    readonly pluginChange: PluginChangePrompt | null;
  };
  readonly "plugins.renderer.failed": SpecificCommandResults["plugins.list"];
  readonly "plugins.install": SpecificCommandResults["plugins.list"];
  readonly "plugins.retry": SpecificCommandResults["plugins.list"];
  readonly "plugins.remove": SpecificCommandResults["plugins.list"];
  readonly "plugins.exit.prepare": {
    readonly correlationId: string;
    readonly pluginExit: PluginExitPrompt | null;
  };
  readonly "plugins.exit.resolve": { readonly correlationId: string; readonly allowed: boolean };
  readonly "profiles.create": HostCommandAccepted;
  readonly "profiles.update": HostCommandAccepted;
  readonly "profiles.binding.get": ProfileBindingDetailResult;
  readonly "clusterDetails.export": KafkaClusterDetailsExportResult;
  readonly "latency.export": KafkaLatencyExportResult;
  readonly "preferences.get": KafkaOperationalPreferenceResult;
  readonly "preferences.update": KafkaOperationalPreferenceResult;
  readonly "preferences.reset": KafkaOperationalPreferenceResult;
}

/** Commands whose success cannot be represented by a plain acknowledgement. */
const structuredResults = {
  "records.analysis.start": true,
  "records.locator.load": true,
  "records.locator.cancel": true,
  "records.analysis.status": true,
  "records.analysis.cancel": true,
  "records.analysis.discard": true,
  "records.export.start": true,
  "records.export.status": true,
  "records.export.cancel": true,
  "records.export.discard": true,
  "relationships.capture": true,
  "observations.capture": true,
  "observations.history": true,
  "observations.clear": true,
  "connect.list": true,
  "connect.load": true,
  "connect.validate": true,
  "connect.review": true,
  "connect.apply": true,
  "environments.capture": true,
  "environments.review": true,
  "environments.apply": true,

  "schemas.client": true,
  "records.repair.list": true,
  "records.replay.review": true,
  "records.replay.apply": true,
  "consumerGroups.reset.review": true,
  "consumerGroups.reset.apply": true,
  "records.trace": true,
  "records.decode": true,
  "schemas.inspect": true,
  "schemas.samples": true,
  "schemas.author": true,
  "schemas.change.review": true,
  "schemas.change.apply": true,
  "schemas.policy.load": true,
  "schemas.policy.review": true,
  "schemas.policy.apply": true,
  "records.batch.review": true,
  "records.batch.apply": true,
  "writes.review": true,
  "writes.apply": true,
  "catalog.list": true,
  "catalog.load": true,
  "catalog.put": true,
  "catalog.delete": true,
  "queries.list": true,
  "queries.put": true,
  "queries.delete": true,
  "plugin.execute": true,
  "plugins.list": true,
  "plugins.catalog": true,
  "plugins.delivery": true,
  "plugins.network.get": true,
  "plugins.network.update": true,
  "plugins.network.test": true,
  "plugins.package.inspect": true,
  "plugins.package.change.prepare": true,
  "plugins.package.install": true,
  "plugins.change.prepare": true,
  "plugins.renderer.failed": true,
  "plugins.install": true,
  "plugins.retry": true,
  "plugins.remove": true,
  "plugins.exit.prepare": true,
  "plugins.exit.resolve": true,
  "trustAcquisition.editor.open": true,
  "trustAcquisition.capabilities": true,
  "trustAcquisition.hostKey.discover": true,
  "trustAcquisition.material.fetch": true,
  "trustAcquisition.https.fetch": true,
  "recipes.usage": true,
  "recipes.legacy.preview": true,
  "recipes.import.preview": true,
  "recipes.export": true,
  "profiles.create": true,
  "profiles.update": true,
  "profiles.binding.get": true,
  "clusterDetails.export": true,
  "latency.export": true,
  "preferences.get": true,
  "preferences.update": true,
  "preferences.reset": true,
} satisfies Record<Exclude<keyof SpecificCommandResults, AclReviewCommandName>, true>;

export type HostAcknowledgementCommandName = Exclude<HostCommandName, keyof SpecificCommandResults>;
export type HostCommandResultMap = {
  readonly [Name in HostCommandName]: Name extends keyof SpecificCommandResults
    ? SpecificCommandResults[Name]
    : { readonly correlationId: string };
};

export function isHostAcknowledgementCommand(
  name: HostCommandName,
): name is HostAcknowledgementCommandName {
  return !isAclReviewCommandName(name) && !Object.hasOwn(structuredResults, name);
}
