import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";

import { StudioButton } from "../../../platform/ui/controls";
import type { StreamSkopeHost } from "../contracts";

import { ResourcePageHeader, TopicSectionTabs } from "./ResourcePageHeader";
import { ReviewedWriteAction } from "./ReviewedWriteAction";
import type { TopicWorkspaceView } from "./WorkbenchContextBar";

interface TopicDetailPageProperties {
  readonly host: StreamSkopeHost;
  readonly canProduce: boolean;
  readonly children: React.ReactNode;
  readonly onOpenTopicNotes: () => void;
  readonly onWorkspaceChange: (workspace: TopicWorkspaceView) => void;
  readonly selectedTopic: string;
  readonly workspace: TopicWorkspaceView;
}

export function TopicDetailPage({
  host,
  canProduce,
  children,
  onOpenTopicNotes,
  onWorkspaceChange,
  selectedTopic,
  workspace,
}: TopicDetailPageProperties): React.JSX.Element {
  return (
    <Box
      aria-label="Topic detail page"
      component="main"
      sx={{
        bgcolor: "background.default",
        display: "grid",
        gridTemplateColumns: "minmax(0, 1fr)",
        gridTemplateRows: "auto auto minmax(0, 1fr)",
        minHeight: 0,
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <ResourcePageHeader
        action={
          <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap", rowGap: 1 }}>
            <StudioButton onClick={onOpenTopicNotes}>Local topic notes</StudioButton>
            <ReviewedWriteAction
              key={selectedTopic}
              host={host}
              topic={selectedTopic}
              disabled={!canProduce}
            />
          </Stack>
        }
        compact
        title={selectedTopic}
      />
      <TopicSectionTabs onChange={onWorkspaceChange} value={workspace} />
      <Box
        id="streamskope-task-workspace"
        sx={{
          display: "grid",
          gridTemplateColumns: "minmax(0, 1fr)",
          height: "100%",
          minHeight: 0,
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        {children}
      </Box>
    </Box>
  );
}
