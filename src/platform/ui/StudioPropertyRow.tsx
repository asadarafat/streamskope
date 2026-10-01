import type { ReactNode } from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

import { StudioTooltip } from "./controls";
import { studioLayoutSpacing, studioSpace } from "./muiSpacing";

interface StudioPropertyRowProps {
  children: ReactNode;
  className?: string;
  description?: string;
  label: string;
}

export function StudioPropertyField({
  children,
  label,
}: {
  children: ReactNode;
  label: string;
}): React.JSX.Element {
  return (
    <Box
      className="studio-compact-property-field"
      data-property-label={label}
      sx={{
        minWidth: 0,
        px: studioLayoutSpacing.propertyRowInline,
        py: studioLayoutSpacing.contentGap,
      }}
    >
      {children}
    </Box>
  );
}

export function StudioPropertyRow({
  children,
  className,
  description,
  label,
}: StudioPropertyRowProps): React.JSX.Element {
  const labelNode = (
    <Typography
      className="studio-property-row-label"
      color="text.secondary"
      component="span"
      sx={{ minWidth: 0, overflowWrap: "anywhere" }}
      title={description ? undefined : label}
      variant="body2"
    >
      {label}
    </Typography>
  );

  return (
    <Box
      className={["studio-property-row", className].filter(Boolean).join(" ")}
      data-property-label={label}
      sx={{
        alignItems: "center",
        display: "grid",
        gap: studioLayoutSpacing.sectionGap,
        gridTemplateColumns: "minmax(108px, 0.9fr) minmax(148px, 1.25fr)",
        minHeight: 48,
        px: studioLayoutSpacing.propertyRowInline,
        py: studioLayoutSpacing.propertyRowBlock,
        "@container studio-workspace (max-width: 340px)": {
          alignItems: "stretch",
          gap: studioLayoutSpacing.compactGap,
          gridTemplateColumns: "minmax(0, 1fr)",
          py: studioLayoutSpacing.contentGap,
        },
      }}
    >
      {description ? (
        <StudioTooltip describeChild placement="right" title={description}>
          {labelNode}
        </StudioTooltip>
      ) : (
        labelNode
      )}
      <Box
        className="studio-property-row-control"
        sx={{
          minWidth: 0,
          overflow: "hidden",
          "& > *": { maxWidth: "100%", width: "100%" },
        }}
      >
        {children}
      </Box>
    </Box>
  );
}

interface StudioDetailRowProps {
  label: string;
  value: ReactNode;
}

/**
 * Studio's session-dock detail anatomy expressed as a semantic definition row.
 * The owning feature supplies evidence; this shared primitive owns its key/value
 * geometry and typography.
 */
export function StudioDetailRow({ label, value }: StudioDetailRowProps): React.JSX.Element {
  return (
    <Box
      className="studio-detail-row"
      component="div"
      data-property-label={label}
      sx={{
        borderBottom: 1,
        borderColor: "divider",
        display: "grid",
        gap: studioSpace.space12,
        gridTemplateColumns: "140px minmax(0, 1fr)",
        minHeight: 30,
        px: studioLayoutSpacing.propertyRowInline,
        py: studioSpace.space6,
        "&:last-child": { borderBottom: 0 },
        "@container studio-workspace (max-width: 340px)": {
          gap: studioSpace.space2,
          gridTemplateColumns: "minmax(0, 1fr)",
        },
      }}
    >
      <Typography color="text.secondary" component="dt" variant="body2">
        {label}
      </Typography>
      <Typography
        component="dd"
        sx={{
          m: 0,
          minWidth: 0,
          overflowWrap: "anywhere",
          whiteSpace: "pre-line",
        }}
        variant="body2"
      >
        {value}
      </Typography>
    </Box>
  );
}
