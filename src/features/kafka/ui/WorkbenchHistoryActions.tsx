import type { ProfileSummary, StreamSkopeHost } from "../contracts";
import { StudioButton } from "../../../platform/ui/controls";

import { RepairJobHistory } from "./RepairJobHistory";

/** History remains reachable before a topic or a source record is selected. */
export function WorkbenchHistoryActions({
  host,
  profiles,
  repairVisible,
  onOpenViews,
}: {
  readonly host: StreamSkopeHost;
  readonly profiles: readonly ProfileSummary[];
  readonly repairVisible: boolean;
  readonly onOpenViews: () => void;
}): React.JSX.Element {
  return (
    <>
      <StudioButton aria-label="Saved views" onClick={onOpenViews}>
        Views
      </StudioButton>
      {repairVisible && <RepairJobHistory host={host} profiles={profiles} />}
    </>
  );
}
