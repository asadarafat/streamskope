import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";

import type { BackendAvailability, StreamSkopeHost } from "../contracts";
import { StudioButton as Button } from "../../../platform/ui/controls";

import { DiagnosticMetric } from "./DiagnosticMetric";
import { ClientQuotaAction } from "./ClientQuotaAction";
import { ResourcePageHeader, resourcePageGutter } from "./ResourcePageHeader";
import { connectionStateLabel, type KafkaUiState } from "./state";

function backendLabel(backend: "checking" | BackendAvailability): string {
  if (backend === "ready") return "Ready";
  if (backend === "unavailable") return "Unavailable";
  return "Checking";
}

export function OverviewPage({
  snapshot,
  host,
  onOpenProfiles,
}: {
  readonly snapshot: Pick<
    KafkaUiState,
    | "backend"
    | "connectionName"
    | "connectionState"
    | "consumerGroupInventory"
    | "profiles"
    | "topics"
    | "preferenceSnapshot"
  >;
  readonly host: StreamSkopeHost;
  readonly onOpenProfiles: () => void;
}): React.JSX.Element {
  const { backend, connectionState, connectionName: activeConnectionName } = snapshot;
  const connected = connectionState === "connected",
    canWrite = snapshot.preferenceSnapshot?.preferences.protection.readOnly === false,
    consumerGroupCount = snapshot.consumerGroupInventory.groups.length,
    profileCount = snapshot.profiles.length,
    topicCount = snapshot.topics.length;
  return (
    <Box
      aria-label="Overview page"
      component="main"
      sx={{ bgcolor: "background.default", minHeight: 0, overflow: "auto" }}
    >
      <ResourcePageHeader
        action={
          <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
            <ClientQuotaAction
              host={host}
              connected={connected}
              connectionName={activeConnectionName}
              canWrite={canWrite}
            />
            <Button onClick={onOpenProfiles} variant="outlined">
              Connection profiles
            </Button>
          </Stack>
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
