import Box from "@mui/material/Box";

import { ProfilePanel, type ProfilePanelProperties } from "./ProfilePanel";
import { ProfileWorkspace, type ProfileWorkspaceProperties } from "./ProfileWorkspace";
import { ResourcePageHeader } from "./ResourcePageHeader";

export function ConnectionProfilesPage({
  panel,
  workspace,
}: {
  readonly panel: ProfilePanelProperties;
  readonly workspace: ProfileWorkspaceProperties;
}): React.JSX.Element {
  return (
    <Box
      aria-label="Connection profiles page"
      component="main"
      sx={{
        bgcolor: "background.default",
        display: "grid",
        gridTemplateRows: "auto minmax(0, 1fr)",
        minHeight: 0,
        overflow: "hidden",
      }}
    >
      <ResourcePageHeader
        description="Store, test, and activate reusable Kafka connection profiles."
        title="Connection Profiles"
      />
      <Box
        sx={{
          display: "grid",
          gridTemplateColumns: "minmax(300px, 34%) minmax(0, 1fr)",
          minHeight: 0,
          overflow: "hidden",
        }}
      >
        <Box
          component="section"
          sx={{
            bgcolor: "background.paper",
            borderRight: 1,
            borderColor: "divider",
            minHeight: 0,
            overflow: "auto",
          }}
        >
          <ProfilePanel {...panel} />
        </Box>
        <ProfileWorkspace {...workspace} component="section" />
      </Box>
    </Box>
  );
}
