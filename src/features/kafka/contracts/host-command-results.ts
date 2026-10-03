import type {
  JsonValue,
  PluginSnapshot,
  PluginCatalogSnapshot,
  PluginExitPrompt,
  PluginChangePrompt,
} from "../../../plugins/contracts";

import type { KafkaQueryLibrarySnapshot } from "./query-library";
import type { TrustAcquisitionCommandResults } from "./remote-trust-types";
import type { TrustRecipeCommandResults } from "./trust-recipe-types";
import type { ProfileBindingDetailResult } from "./profile-binding";
import type { KafkaClusterDetailsExportResult } from "./cluster-diagnostics-types";
import type { KafkaLatencyExportResult } from "./latency-types";
import type { KafkaOperationalPreferenceResult } from "./operational-preference-types";
import type { HostCommandAccepted, HostCommandName } from "./types";

interface SpecificCommandResults extends TrustAcquisitionCommandResults, TrustRecipeCommandResults {
  readonly "records.trace": {
    readonly correlationId: string;
    readonly trace: import("./correlation-trace").CorrelationTraceResult;
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
  readonly "plugins.change.prepare": {
    readonly correlationId: string;
    readonly pluginChange: PluginChangePrompt | null;
  };
  readonly "plugins.renderer.failed": SpecificCommandResults["plugins.list"];
  readonly "plugins.install": SpecificCommandResults["plugins.list"];
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
  "records.trace": true,
  "records.decode": true,
  "schemas.inspect": true,
  "schemas.samples": true,
  "records.batch.review": true,
  "records.batch.apply": true,
  "writes.review": true,
  "writes.apply": true,
  "queries.list": true,
  "queries.put": true,
  "queries.delete": true,
  "plugin.execute": true,
  "plugins.list": true,
  "plugins.catalog": true,
  "plugins.change.prepare": true,
  "plugins.renderer.failed": true,
  "plugins.install": true,
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
} satisfies Record<keyof SpecificCommandResults, true>;

export type HostAcknowledgementCommandName = Exclude<HostCommandName, keyof SpecificCommandResults>;
export type HostCommandResultMap = {
  readonly [Name in HostCommandName]: Name extends keyof SpecificCommandResults
    ? SpecificCommandResults[Name]
    : { readonly correlationId: string };
};

export function isHostAcknowledgementCommand(
  name: HostCommandName,
): name is HostAcknowledgementCommandName {
  return !Object.hasOwn(structuredResults, name);
}
