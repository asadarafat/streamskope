import Box, { type BoxProps } from "@mui/material/Box";
import type { Theme } from "@mui/material/styles";
import type { SystemStyleObject } from "@mui/system";

import { streamSkopeMuiMonospaceTypography } from "./createStreamSkopeTheme";

interface StudioCodeBlockProperties extends Omit<BoxProps, "sx"> {
  readonly sx?: SystemStyleObject<Theme>;
}

/** Technical evidence at the single shared monospace size. */
export function StudioTechnicalText({
  sx,
  ...properties
}: StudioCodeBlockProperties): React.JSX.Element {
  return (
    <Box component="span" {...properties} sx={{ ...streamSkopeMuiMonospaceTypography, ...sx }} />
  );
}

export function StudioCodeBlock({
  sx,
  ...properties
}: StudioCodeBlockProperties): React.JSX.Element {
  return (
    <Box component="pre" {...properties} sx={{ ...streamSkopeMuiMonospaceTypography, ...sx }} />
  );
}
