import {
  KAFKA_MESSAGE_VIEW_COLUMNS,
  type KafkaMessageViewColumn,
  type KafkaMessageViewPresentation,
} from "../contracts/investigation-view";

const compactHidden = new Set<KafkaMessageViewColumn>(["offset", "partition", "preview"]);

/** Compact rendering is an overlay; it never becomes the saved column selection. */
export function messageGridVisibility(
  presentation: KafkaMessageViewPresentation,
  compact: boolean,
): Record<string, boolean> {
  const model = Object.fromEntries(
    KAFKA_MESSAGE_VIEW_COLUMNS.map((column) => [
      column,
      presentation.visibleColumns.includes(column) && (!compact || !compactHidden.has(column)),
    ]),
  );
  if (!Object.values(model).some(Boolean)) model[presentation.visibleColumns[0]!] = true;
  return model;
}

export function changedMessageGridColumns(
  presentation: KafkaMessageViewPresentation,
  compact: boolean,
  next: Readonly<Record<string, boolean>>,
): readonly KafkaMessageViewColumn[] {
  const current = messageGridVisibility(presentation, compact);
  const visible = KAFKA_MESSAGE_VIEW_COLUMNS.filter((column) => {
    const requested = next[column] !== false;
    return requested === current[column] ? presentation.visibleColumns.includes(column) : requested;
  });
  return visible.length === 0 ? presentation.visibleColumns : visible;
}
