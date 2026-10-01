import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

import { studioSpace } from "../../../platform/ui/muiSpacing";

export interface DiagnosticMetricProperties {
  readonly label: string;
  readonly value: React.ReactNode;
}

/**
 * One exact current measurement in a diagnostic summary. The caller owns the
 * measurement and status; this component owns only the shared visual grammar.
 */
export function DiagnosticMetric({ label, value }: DiagnosticMetricProperties): React.JSX.Element {
  return (
    <Box
      component="dl"
      sx={{
        borderRight: 1,
        borderColor: "divider",
        m: 0,
        minWidth: 0,
        px: studioSpace.space12,
        py: studioSpace.space8,
      }}
    >
      <Typography color="text.secondary" component="dt" variant="caption">
        {label}
      </Typography>
      <Typography component="dd" noWrap sx={{ m: 0, mt: studioSpace.space2 }} variant="subtitle2">
        {value}
      </Typography>
    </Box>
  );
}
