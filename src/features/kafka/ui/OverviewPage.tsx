import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";

import type { BackendAvailability, ConnectionState } from "../contracts";
import { StudioButton as Button } from "../../../platform/ui/controls";

import { DiagnosticMetric } from "./DiagnosticMetric";
import { ResourcePageHeader, resourcePageGutter } from "./ResourcePageHeader";
import { connectionStateLabel } from "./state";

function backendLabel(backend: "checking" | BackendAvailability): string {
  if (backend === "ready") return "Ready";
  if (backend === "unavailable") return "Unavailable";
  return "Checking";
}

export function OverviewPage({
  activeConnectionName,
  backend,
  connectionState,
  consumerGroupCount,
  onOpenProfiles,
  profileCount,
  topicCount,
}: {
  readonly activeConnectionName: string | null;
  readonly backend: "checking" | BackendAvailability;
  readonly connectionState: ConnectionState;
  readonly consumerGroupCount: number;
  readonly onOpenProfiles: () => void;
  readonly profileCount: number;
  readonly topicCount: number;
}): React.JSX.Element {
  return (
    <Box
      aria-label="Overview page"
      component="main"
      sx={{ bgcolor: "background.default", minHeight: 0, overflow: "auto" }}
    >
      <ResourcePageHeader
        action={
          <Button onClick={onOpenProfiles} variant="outlined">
            Connection profiles
          </Button>
        }
        description="Confirmed local host and Kafka session state. Unavailable data is never inferred."
        title="Overview"
      />
      <Stack sx={{ px: resourcePageGutter, py: { md: 3, xs: 2 } }}>
        <Box
          aria-label="Overview summary"
          component="section"
          role="group"
          sx={{
            bgcolor: "background.paper",
            border: 1,
            borderColor: "divider",
            display: "grid",
            gridTemplateColumns: {
              lg: "repeat(6, minmax(0, 1fr))",
              sm: "repeat(3, minmax(0, 1fr))",
              xs: "repeat(2, minmax(0, 1fr))",
            },
            m: 0,
            overflow: "hidden",
            "& > div:nth-of-type(6n)": { borderRight: { lg: 0 } },
          }}
        >
          <DiagnosticMetric label="Session" value={connectionStateLabel(connectionState)} />
          <DiagnosticMetric label="Active connection" value={activeConnectionName ?? "None"} />
          <DiagnosticMetric label="Application host" value={backendLabel(backend)} />
          <DiagnosticMetric label="Topics" value={topicCount.toLocaleString()} />
          <DiagnosticMetric label="Consumer groups" value={consumerGroupCount.toLocaleString()} />
          <DiagnosticMetric label="Saved profiles" value={profileCount.toLocaleString()} />
        </Box>
      </Stack>
    </Box>
  );
}
