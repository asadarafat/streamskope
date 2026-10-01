import { Box, Stack, Typography } from "@mui/material";

export type StatusIndicatorTone = "error" | "info" | "neutral" | "success" | "warning";

interface StatusIndicatorProperties {
  readonly ariaLabel: string;
  readonly label: string;
  readonly live?: "assertive" | "polite";
  readonly tone?: StatusIndicatorTone;
}

function toneColor(tone: StatusIndicatorTone): string {
  return tone === "neutral" ? "text.secondary" : `${tone}.main`;
}

/** Presents confirmed operational state as quiet text, never as an action-like control. */
export function StatusIndicator({
  ariaLabel,
  label,
  live,
  tone = "neutral",
}: StatusIndicatorProperties): React.JSX.Element {
  return (
    <Stack
      aria-label={ariaLabel}
      aria-live={live}
      component="span"
      direction="row"
      role="status"
      spacing={0.75}
      sx={{ alignItems: "center", flex: "0 0 auto", minHeight: 24 }}
    >
      <Box
        aria-hidden
        component="span"
        sx={{
          bgcolor: toneColor(tone),
          borderRadius: "50%",
          height: 7,
          width: 7,
        }}
      />
      <Typography component="span" noWrap variant="caption">
        {label}
      </Typography>
    </Stack>
  );
}
