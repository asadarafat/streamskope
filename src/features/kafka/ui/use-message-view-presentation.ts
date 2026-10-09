import { useCallback, useState } from "react";

import {
  createDefaultKafkaInvestigationView,
  KAFKA_INVESTIGATION_VIEW_LIMITS,
  parseKafkaInvestigationView,
  parseKafkaMessageViewPresentation,
  type KafkaMessageViewPresentation,
} from "../contracts/investigation-view";

export interface MessageViewPresentationController {
  readonly value: KafkaMessageViewPresentation;
  change(this: void, patch: Partial<KafkaMessageViewPresentation>): void;
  restore(this: void, value: KafkaMessageViewPresentation): void;
}

function initialPresentation(): KafkaMessageViewPresentation {
  const defaults = createDefaultKafkaInvestigationView();
  try {
    const width = globalThis.localStorage?.getItem("streamskope-inspector-pane-width");
    if (width !== null && width !== undefined)
      return parseKafkaInvestigationView({
        ...defaults,
        messages: { ...defaults.messages, inspectorWidth: Number(width) },
      }).messages;
  } catch {
    // Optional legacy presentation storage cannot prevent a fresh investigation.
  }
  return defaults.messages;
}

/** User intent survives resource navigation; responsive overlays never feed back into it. */
export function useMessageViewPresentation(): MessageViewPresentationController {
  const [value, setValue] = useState(initialPresentation);
  const restore = useCallback((messages: KafkaMessageViewPresentation): void => {
    setValue(parseKafkaMessageViewPresentation(messages));
  }, []);
  const change = useCallback((patch: Partial<KafkaMessageViewPresentation>): void => {
    const bounds = KAFKA_INVESTIGATION_VIEW_LIMITS.inspectorWidth;
    const width = patch.inspectorWidth;
    const inspectorWidth =
      width === undefined
        ? undefined
        : Math.max(bounds.minimum, Math.min(bounds.maximum, Math.round(width)));
    const normalized = inspectorWidth === undefined ? patch : { ...patch, inspectorWidth };
    setValue((previous) => parseKafkaMessageViewPresentation({ ...previous, ...normalized }));
    if (inspectorWidth !== undefined) {
      try {
        globalThis.localStorage?.setItem(
          "streamskope-inspector-pane-width",
          String(inspectorWidth),
        );
      } catch {
        /* Optional presentation defaults must not prevent resizing. */
      }
    }
  }, []);
  return { value, change, restore };
}
