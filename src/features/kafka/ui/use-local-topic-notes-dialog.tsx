import { useState } from "react";

import type { StreamSkopeHost } from "../contracts";

import { LocalTopicNotesDialog } from "./LocalTopicNotesDialog";

interface TopicNotesDialogController {
  readonly dialog: React.JSX.Element | null;
  readonly openCurrent: () => void;
  readonly openLibrary: () => void;
}

export function useLocalTopicNotesDialog(
  host: StreamSkopeHost,
  scope: { readonly connected: boolean; readonly authorityKey: string },
  currentTopic: string | null,
): TopicNotesDialogController {
  const [initialTopic, setInitialTopic] = useState<string | null>();
  return {
    openCurrent: (): void => setInitialTopic(currentTopic),
    openLibrary: (): void => setInitialTopic(null),
    dialog:
      initialTopic === undefined ? null : (
        <LocalTopicNotesDialog
          host={host}
          connected={scope.connected}
          authorityKey={scope.authorityKey}
          initialTopic={initialTopic}
          currentTopic={currentTopic}
          onClose={() => setInitialTopic(undefined)}
        />
      ),
  };
}
