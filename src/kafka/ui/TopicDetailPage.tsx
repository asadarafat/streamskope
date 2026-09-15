import Box from "@mui/material/Box";

import { ResourcePageHeader, TopicSectionTabs } from "./ResourcePageHeader";
import type { TopicWorkspaceView } from "./WorkbenchContextBar";

interface TopicDetailPageProperties {
  readonly children: React.ReactNode;
  readonly onWorkspaceChange: (workspace: TopicWorkspaceView) => void;
  readonly selectedTopic: string;
  readonly workspace: TopicWorkspaceView;
}

export function TopicDetailPage({
  children,
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
        gridTemplateRows: "auto auto minmax(0, 1fr)",
        minHeight: 0,
        overflow: "hidden",
      }}
    >
      <ResourcePageHeader compact title={selectedTopic} />
      <TopicSectionTabs onChange={onWorkspaceChange} value={workspace} />
      <Box
        id="streamskope-task-workspace"
        sx={{ display: "grid", height: "100%", minHeight: 0, overflow: "hidden" }}
      >
        {children}
      </Box>
    </Box>
  );
}
