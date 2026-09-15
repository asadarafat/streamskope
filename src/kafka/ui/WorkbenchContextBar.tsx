import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

import {
  streamSkopeGeometry,
  streamSkopeMuiMonospaceTypography,
  streamSkopeSpacing,
} from "../../ui/createStreamSkopeTheme";
import {
  StudioToggleButton as ToggleButton,
  StudioToggleButtonGroup as ToggleButtonGroup,
} from "../../ui/controls";

import type { NavigationView } from "./workbench-navigation";

export type TopicWorkspaceView = "configuration" | "latency" | "messages" | "monitor" | "rules";

interface WorkbenchContextBarProperties {
  readonly compact: boolean;
  readonly navigation: NavigationView;
  readonly onWorkspaceChange: (workspace: TopicWorkspaceView) => void;
  readonly profileName: string | null;
  readonly selectedConsumerGroupId: string | null;
  readonly selectedTopic: string | null;
  readonly workspace: TopicWorkspaceView;
}

const TOPIC_WORKSPACES = [
  ["Messages", "messages"],
  ["Monitor", "monitor"],
  ["Latency", "latency"],
  ["Rules", "rules"],
  ["Configuration", "configuration"],
] as const satisfies readonly (readonly [string, TopicWorkspaceView])[];

export function WorkbenchContextBar({
  compact,
  navigation,
  onWorkspaceChange,
  profileName,
  selectedConsumerGroupId,
  selectedTopic,
  workspace,
}: WorkbenchContextBarProperties): React.JSX.Element {
  const context =
    navigation === "overview"
      ? "Overview"
      : navigation === "profiles"
        ? `Connection / ${profileName ?? "No profile selected"}`
        : navigation === "topics"
          ? `Topics / ${selectedTopic ?? "No topic selected"}`
          : `Consumer groups / ${selectedConsumerGroupId ?? "No group selected"}`;

  return (
    <Box
      aria-label="Workbench context"
      component="section"
      sx={{
        alignItems: "center",
        bgcolor: "background.paper",
        borderBottom: 1,
        borderColor: "divider",
        columnGap: `${String(streamSkopeSpacing.scale.space6)}px`,
        display: "grid",
        gridTemplateColumns: "minmax(0, 1fr) auto",
        height: streamSkopeGeometry.contextBarHeight,
        minWidth: 0,
      }}
    >
      <Typography
        noWrap
        sx={{
          color: "text.secondary",
          minWidth: 0,
          px: `${String(streamSkopeSpacing.scale.space10)}px`,
        }}
        title={context}
        variant="body2"
      >
        {navigation === "topics" ? (
          <>
            Topics /{" "}
            {selectedTopic === null ? (
              "No topic selected"
            ) : (
              <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
                {selectedTopic}
              </Box>
            )}
          </>
        ) : navigation === "consumer-groups" ? (
          <>
            Consumer groups /{" "}
            {selectedConsumerGroupId === null ? (
              "No group selected"
            ) : (
              <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
                {selectedConsumerGroupId}
              </Box>
            )}
          </>
        ) : (
          context
        )}
      </Typography>

      {navigation === "topics" && selectedTopic !== null ? (
        <Box
          aria-label="Topic tasks"
          component="nav"
          sx={{
            minWidth: 0,
            overflowX: "auto",
            overflowY: "hidden",
            pr: `${String(streamSkopeSpacing.scale.space10)}px`,
          }}
        >
          <ToggleButtonGroup
            aria-label="Topic workspace"
            exclusive
            onChange={(_event, task: TopicWorkspaceView | null) => {
              if (task !== null) {
                onWorkspaceChange(task);
              }
            }}
            sx={{
              bgcolor: "background.default",
              "& .MuiToggleButton-root": {
                height: streamSkopeGeometry.resourceTabHeight,
                minHeight: streamSkopeGeometry.resourceTabHeight,
                minWidth: compact ? 54 : 64,
                py: 0,
              },
            }}
            value={workspace}
          >
            {TOPIC_WORKSPACES.map(([label, task]) => (
              <ToggleButton
                aria-controls="streamskope-task-workspace"
                aria-pressed={workspace === task}
                id={`streamskope-task-${task}`}
                key={task}
                value={task}
              >
                {label}
              </ToggleButton>
            ))}
          </ToggleButtonGroup>
        </Box>
      ) : (
        <Box />
      )}
    </Box>
  );
}
