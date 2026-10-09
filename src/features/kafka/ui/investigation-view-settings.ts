import { parseKafkaSavedView, type KafkaSavedView } from "../contracts/query-library";
import { createDefaultKafkaInvestigationView } from "../contracts/investigation-view";
import { createEmptyKafkaSavedRecordContext } from "../contracts/record-locator";
import type { KafkaInvestigationQuery } from "../contracts";
import { parseKafkaPortableView, type KafkaPortableView } from "../contracts/view-transfer";

/** Reviewed settings only; connection and record authority are never carried here. */
export type KafkaViewSettings = Pick<KafkaSavedView, "configuration" | "view" | "records">;

export function parseViewSettings(settings: KafkaViewSettings): KafkaViewSettings {
  const parsed = parseKafkaSavedView({ id: "current", name: "Current view", ...settings });
  return { configuration: parsed.configuration, view: parsed.view, records: parsed.records };
}

export function queryViewSettings(configuration: KafkaInvestigationQuery): KafkaViewSettings {
  return parseViewSettings({
    configuration,
    view: createDefaultKafkaInvestigationView(),
    records: createEmptyKafkaSavedRecordContext(),
  });
}

/** Allocate receiver-local UI keys only when an operator explicitly opens a reviewed view. */
export function portableViewSettings(
  input: KafkaPortableView,
  createId: () => string = () => globalThis.crypto.randomUUID(),
): KafkaViewSettings {
  const view = parseKafkaPortableView(input);
  return parseViewSettings({
    configuration: view.configuration,
    view: view.view,
    records: {
      selected: view.records.selected,
      comparison: view.records.comparison,
      bookmarks: view.records.bookmarks.map((bookmark) => ({ ...bookmark, id: createId() })),
    },
  });
}
