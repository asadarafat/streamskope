import { parseKafkaSavedView, type KafkaSavedView } from "../contracts/query-library";
import { createDefaultKafkaInvestigationView } from "../contracts/investigation-view";
import type { KafkaInvestigationQuery } from "../contracts";

/** Reviewed settings only; connection and record authority are never carried here. */
export type KafkaViewSettings = Pick<KafkaSavedView, "configuration" | "view">;

export function parseViewSettings(settings: KafkaViewSettings): KafkaViewSettings {
  const parsed = parseKafkaSavedView({ id: "current", name: "Current view", ...settings });
  return { configuration: parsed.configuration, view: parsed.view };
}

export function queryViewSettings(configuration: KafkaInvestigationQuery): KafkaViewSettings {
  return parseViewSettings({ configuration, view: createDefaultKafkaInvestigationView() });
}
