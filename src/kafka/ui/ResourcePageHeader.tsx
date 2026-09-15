import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import { StudioTab as Tab, StudioTabs as Tabs } from "../../ui/controls";

import type { TopicWorkspaceView } from "./WorkbenchContextBar";
export const resourcePageGutter = { md: 2, xs: 1.5 } as const;

export const TOPIC_WORKSPACES = [
  ["Messages", "messages"],
  ["Monitor", "monitor"],
  ["Latency", "latency"],
  ["Rules", "rules"],
  ["Configuration", "configuration"],
] as const satisfies readonly (readonly [string, TopicWorkspaceView])[];

interface ResourcePageHeaderProperties {
  readonly action?: React.ReactNode;
  readonly compact?: boolean;
  readonly description?: string;
  readonly eyebrow?: string;
  readonly title: string;
}

export function ResourcePageHeader({
  action,
  compact = false,
  description,
  eyebrow,
  title,
}: ResourcePageHeaderProperties): React.JSX.Element {
  if (compact) {
    return (
      <Box
        component="header"
        sx={{ bgcolor: "background.paper", borderBottom: 1, borderColor: "divider" }}
      >
        <Stack
          direction="row"
          sx={{ alignItems: "center", gap: 1.5, minHeight: 52, px: resourcePageGutter }}
        >
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography component="h1" noWrap title={title} variant="h5">
              {title}
            </Typography>
          </Box>
          {action}
        </Stack>
      </Box>
    );
  }

  return (
    <Box
      component="header"
      sx={{
        bgcolor: "background.paper",
        borderBottom: 1,
        borderColor: "divider",
      }}
    >
      <Stack
        direction="row"
        sx={{ alignItems: "center", gap: 3, minHeight: 72, px: resourcePageGutter, py: 1.25 }}
      >
        <Box sx={{ flex: 1, minWidth: 0 }}>
          {eyebrow === undefined ? null : (
            <Typography color="text.secondary" component="p" variant="overline">
              {eyebrow}
            </Typography>
          )}
          <Typography component="h1" noWrap title={title} variant="h5">
            {title}
          </Typography>
          {description === undefined ? null : (
            <Typography color="text.secondary" sx={{ mt: 0.5 }} variant="body2">
              {description}
            </Typography>
          )}
        </Box>
        {action}
      </Stack>
    </Box>
  );
}

export function TopicSectionTabs({
  onChange,
  value,
}: {
  readonly onChange: (value: TopicWorkspaceView) => void;
  readonly value: TopicWorkspaceView;
}): React.JSX.Element {
  return (
    <Tabs
      aria-label="Topic sections"
      onChange={(_event, nextValue: TopicWorkspaceView) => onChange(nextValue)}
      scrollButtons="auto"
      sx={{
        bgcolor: "background.paper",
        borderBottom: 1,
        borderColor: "divider",
        minHeight: 42,
        px: resourcePageGutter,
        "& .MuiTab-root": { minHeight: 42 },
      }}
      value={value}
      variant="scrollable"
    >
      {TOPIC_WORKSPACES.map(([label, workspace]) => (
        <Tab
          aria-controls="streamskope-task-workspace"
          key={workspace}
          label={label}
          value={workspace}
        />
      ))}
    </Tabs>
  );
}
