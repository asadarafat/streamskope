import Box from "@mui/material/Box";
import SvgIcon from "@mui/material/SvgIcon";
import Typography from "@mui/material/Typography";
import type { ReactNode } from "react";

import { StudioIconButton } from "./controls";
import { studioLayoutSpacing } from "./muiSpacing";
import { studioGeometry } from "./studioTokens";

function CloseIcon(): React.JSX.Element {
  return (
    <SvgIcon fontSize="small" viewBox="0 0 24 24">
      <path d="M18.3 5.71 16.89 4.3 12 9.17 7.11 4.3 5.7 5.71 10.59 10.6 5.7 15.49l1.41 1.41L12 12.01l4.89 4.89 1.41-1.41-4.89-4.89 4.89-4.89z" />
    </SvgIcon>
  );
}

/** Panel headers share the command bar's height so the frame keeps one horizontal rhythm. */
export function StudioPanelHeader({
  actions,
  collapseLabel = "Collapse workspace panel",
  density = "standard",
  onCollapse,
  title,
}: {
  actions?: ReactNode;
  collapseLabel?: string;
  density?: "standard" | "toolbar";
  onCollapse?: () => void;
  title: string;
}): React.JSX.Element {
  return (
    <Box
      sx={{
        alignItems: "center",
        borderBottom: 1,
        borderColor: "divider",
        display: "flex",
        gap: studioLayoutSpacing.contentGap,
        justifyContent: "space-between",
        minHeight:
          density === "toolbar"
            ? studioGeometry.localToolbarHeight
            : studioGeometry.commandBarHeight,
        px: studioLayoutSpacing.panelInline,
        "@container studio-workspace (max-width: 280px)": {
          px: studioLayoutSpacing.contentGap,
        },
      }}
    >
      <Typography component="h2" noWrap sx={{ minWidth: 0 }} variant="subtitle2">
        {title}
      </Typography>
      <Box
        sx={{
          alignItems: "center",
          display: "flex",
          flexShrink: 0,
          gap: studioLayoutSpacing.controlGap,
        }}
      >
        {actions}
        {onCollapse ? (
          <StudioIconButton aria-label={collapseLabel} onClick={onCollapse} title={collapseLabel}>
            <CloseIcon />
          </StudioIconButton>
        ) : null}
      </Box>
    </Box>
  );
}

export function StudioPanelEmpty({ children }: { children: ReactNode }): React.JSX.Element {
  return (
    <Box
      sx={{
        color: "text.secondary",
        display: "grid",
        gap: studioLayoutSpacing.contentGap,
        p: studioLayoutSpacing.panelEmptyInset,
      }}
    >
      {children}
    </Box>
  );
}
