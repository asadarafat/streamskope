import { Box, Typography } from "@mui/material";

export interface WorkspaceStateProperties {
  readonly label: string;
  readonly detail: string;
  readonly title: string;
}

/** A truthful empty or idle state for a task workspace. */
export function WorkspaceState({
  detail,
  label,
  title,
}: WorkspaceStateProperties): React.JSX.Element {
  return (
    <Box aria-label={label} role="status" sx={{ maxWidth: 520 }}>
      <Typography component="h2" variant="h6">
        {title}
      </Typography>
      <Typography color="text.secondary" sx={{ mt: 0.5 }} variant="body2">
        {detail}
      </Typography>
    </Box>
  );
}
