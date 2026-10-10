import type { ProfileSummary, StreamSkopeHost } from "../contracts";
import type { KafkaRecordLocator } from "../contracts/record-locator";

import { SavedViewsDialog } from "./SavedViewsDialog";
import type { TextDocumentTransferPort } from "./text-document-transfer";
import type { useInvestigationViews } from "./use-investigation-views";

/** Keep dialog preparation, restoration and bookmark cleanup in one interaction. */
export function WorkbenchSavedViews({
  host,
  transfer,
  profiles,
  views,
  selectedTopic,
  bookmarkCandidate,
  clearBookmark,
  onOpenTopicNotes,
}: {
  readonly host: StreamSkopeHost;
  readonly transfer: TextDocumentTransferPort;
  readonly profiles: readonly ProfileSummary[];
  readonly views: ReturnType<typeof useInvestigationViews>;
  readonly selectedTopic: string | null;
  readonly bookmarkCandidate: KafkaRecordLocator | undefined;
  readonly clearBookmark: () => void;
  readonly onOpenTopicNotes: () => void;
}): React.JSX.Element | null {
  if (!views.open) return null;
  return (
    <SavedViewsDialog
      host={host}
      transfer={transfer}
      profiles={profiles}
      initialImport={views.queryImport}
      preparedInvestigation={views.prepared}
      currentResource={views.currentResource}
      currentQueryAvailable={selectedTopic !== null}
      readActive={views.readActive}
      captureCurrent={views.capture}
      bookmarkCandidate={bookmarkCandidate}
      restoreError={views.error}
      onClose={() => {
        clearBookmark();
        views.close();
      }}
      onRestore={(settings, profileId) => {
        clearBookmark();
        views.restore(settings, profileId);
      }}
      onOpenTopicNotes={() => {
        clearBookmark();
        views.close();
        onOpenTopicNotes();
      }}
    />
  );
}
