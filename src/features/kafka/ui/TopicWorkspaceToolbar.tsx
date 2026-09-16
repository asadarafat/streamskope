import type { ReactNode } from "react";
import { Toolbar } from "@mui/material";

import { studioLayoutSpacing } from "../../../platform/ui/muiSpacing";
import { streamSkopeGeometry } from "../../../platform/ui/studioTokens";

/** Common task chrome; feature components retain ownership of actions and state. */
export function TopicWorkspaceToolbar({
  children,
  label,
}: {
  readonly children: ReactNode;
  readonly label: string;
}): React.JSX.Element {
  return (
    <Toolbar
      aria-label={label}
      component="section"
      disableGutters
      role="group"
      variant="dense"
      sx={{
        alignItems: "center",
        bgcolor: "background.paper",
        borderBottom: 1,
        borderColor: "divider",
        boxSizing: "border-box",
        display: "flex",
        flexShrink: 0,
        gap: studioLayoutSpacing.contentGap,
        height: streamSkopeGeometry.localToolbarHeight,
        minHeight: `${String(streamSkopeGeometry.localToolbarHeight)}px !important`,
        minWidth: 0,
        overflowX: "auto",
        position: "sticky",
        px: studioLayoutSpacing.panelInline,
        top: 0,
        zIndex: 1,
        "& > button": { flexShrink: 0, whiteSpace: "nowrap" },
      }}
    >
      {children}
    </Toolbar>
  );
}
