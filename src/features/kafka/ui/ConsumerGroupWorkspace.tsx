import { useMemo } from "react";
import Box from "@mui/material/Box";
import Divider from "@mui/material/Divider";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import { DataGrid, type GridColDef } from "@mui/x-data-grid";

import type {
  KafkaConsumerGroupDetailSnapshot,
  KafkaConsumerGroupMember,
  KafkaConsumerGroupOffset,
} from "../contracts";
import {
  streamSkopeLayout,
  streamSkopeMuiMonospaceTypography,
} from "../../../platform/ui/createStreamSkopeTheme";
import { StudioDetailRow } from "../../../platform/ui/StudioPropertyRow";
import { StudioAlert as Alert } from "../../../platform/ui/controls";

import { StatusIndicator, type StatusIndicatorTone } from "./StatusIndicator";
import { formatUtcTimestamp } from "./timestamp-presentation";
import { WorkspaceState } from "./WorkspaceState";

interface MemberRow {
  readonly assignments: string;
  readonly clientHost: string;
  readonly clientId: string;
  readonly id: string;
  readonly instanceId: string;
}

interface OffsetRow {
  readonly committedOffset: string;
  readonly endOffset: string;
  readonly id: string;
  readonly lag: string;
  readonly partition: number;
  readonly topic: string;
}

const memberColumns: readonly GridColDef<MemberRow>[] = [
  { field: "clientId", flex: 1, headerName: "Client", minWidth: 150 },
  { field: "clientHost", flex: 0.8, headerName: "Host", minWidth: 120 },
  { field: "instanceId", flex: 0.8, headerName: "Instance", minWidth: 110 },
  { field: "assignments", flex: 1.4, headerName: "Assignments", minWidth: 180, sortable: false },
];

const offsetColumns: readonly GridColDef<OffsetRow>[] = [
  { field: "topic", flex: 1.2, headerName: "Topic", minWidth: 150 },
  {
    align: "right",
    field: "partition",
    headerAlign: "right",
    headerName: "Partition",
    width: 82,
  },
  {
    align: "right",
    field: "committedOffset",
    headerAlign: "right",
    headerName: "Committed",
    minWidth: 112,
  },
  {
    align: "right",
    field: "endOffset",
    headerAlign: "right",
    headerName: "End",
    minWidth: 112,
  },
  { align: "right", field: "lag", headerAlign: "right", headerName: "Lag", minWidth: 90 },
];

function boundedGridHeight(rowCount: number, maximumVisibleRows: number): number {
  const visibleRows = Math.max(1, Math.min(rowCount, maximumVisibleRows));
  return streamSkopeLayout.tableHeaderHeight + streamSkopeLayout.tableRowHeight * visibleRows + 2;
}

function assignmentLabel(member: KafkaConsumerGroupMember): string {
  if (member.assignments.length === 0) {
    return "No assignments";
  }
  return member.assignments
    .map((assignment) => `${assignment.topic} [${assignment.partitions.join(", ")}]`)
    .join("; ");
}

function memberRow(member: KafkaConsumerGroupMember): MemberRow {
  return {
    assignments: assignmentLabel(member),
    clientHost: member.clientHost || "Unavailable",
    clientId: member.clientId || "Unavailable",
    id: member.id,
    instanceId: member.groupInstanceId ?? "Dynamic",
  };
}

function offsetRow(offset: KafkaConsumerGroupOffset): OffsetRow {
  return {
    committedOffset: offset.committedOffset ?? "Unavailable",
    endOffset: offset.endOffset ?? "Unavailable",
    id: `${offset.topic}:${String(offset.partition)}`,
    lag: offset.lag ?? "Unavailable",
    partition: offset.partition,
    topic: offset.topic,
  };
}

function totalLag(offsets: readonly KafkaConsumerGroupOffset[]): string {
  let total = 0n;
  let unavailable = 0;
  for (const offset of offsets) {
    if (offset.lag === null) {
      unavailable += 1;
    } else {
      total += BigInt(offset.lag);
    }
  }
  if (offsets.length === 0 || unavailable === offsets.length) {
    return "Unavailable";
  }
  const confirmed = total.toString();
  return unavailable === 0
    ? confirmed
    : `${confirmed} confirmed · ${unavailable.toLocaleString()} unavailable`;
}

function groupTone(state: string): StatusIndicatorTone {
  if (state === "stable") {
    return "success";
  }
  if (state === "preparing-rebalance" || state === "completing-rebalance") {
    return "warning";
  }
  return "neutral";
}

function FailureState({
  snapshot,
}: {
  readonly snapshot: KafkaConsumerGroupDetailSnapshot;
}): React.JSX.Element {
  const title =
    snapshot.state === "not-found"
      ? "Consumer group not found"
      : snapshot.state === "denied"
        ? "Consumer-group access denied"
        : "Consumer group unavailable";
  return (
    <Box sx={{ maxWidth: 620, p: 2 }}>
      <Alert severity="error">
        <Typography component="h2" variant="subtitle1">
          {title}
        </Typography>
        <Typography component="p" variant="body2">
          {snapshot.error?.summary ?? "Consumer-group detail could not be loaded."}
        </Typography>
        <Typography component="p" variant="body2">
          {snapshot.error?.recovery ?? "Refresh the inventory, review Activity, and retry."}
        </Typography>
      </Alert>
    </Box>
  );
}

export function ConsumerGroupWorkspace({
  component = "main",
  selectedGroupId,
  snapshot,
}: {
  readonly component?: "main" | "section";
  readonly selectedGroupId: string | null;
  readonly snapshot: KafkaConsumerGroupDetailSnapshot;
}): React.JSX.Element {
  const memberRows = useMemo(
    () => snapshot.group?.members.map(memberRow) ?? [],
    [snapshot.group?.members],
  );
  const offsetRows = useMemo(
    () => snapshot.group?.offsets.map(offsetRow) ?? [],
    [snapshot.group?.offsets],
  );

  if (selectedGroupId === null || snapshot.state === "unavailable") {
    return (
      <Box
        aria-label="Consumer group workspace"
        component={component}
        sx={{
          bgcolor: "background.paper",
          display: "grid",
          minHeight: 0,
          p: 2,
          placeItems: "center",
        }}
      >
        <WorkspaceState
          detail="Choose a group from Resources to inspect members, assignments, offsets, and lag."
          label="Consumer group workspace state"
          title="Choose a consumer group"
        />
      </Box>
    );
  }

  if (snapshot.state === "loading") {
    return (
      <Box
        aria-label="Consumer group workspace"
        component={component}
        sx={{
          bgcolor: "background.paper",
          display: "grid",
          minHeight: 0,
          p: 2,
          placeItems: "center",
        }}
      >
        <WorkspaceState
          detail={`Reading members, assignments, and offsets for ${selectedGroupId}.`}
          label="Consumer group detail status"
          title="Loading consumer group…"
        />
      </Box>
    );
  }

  if (
    snapshot.state === "denied" ||
    snapshot.state === "not-found" ||
    snapshot.state === "failed"
  ) {
    return (
      <Box
        aria-label="Consumer group workspace"
        component={component}
        sx={{ bgcolor: "background.paper", minHeight: 0, overflow: "auto" }}
      >
        <FailureState snapshot={snapshot} />
      </Box>
    );
  }

  const group = snapshot.group;
  if (group === null) {
    return (
      <Box
        aria-label="Consumer group workspace"
        component={component}
        sx={{ bgcolor: "background.paper", minHeight: 0, overflow: "auto" }}
      >
        <FailureState snapshot={{ ...snapshot, state: "failed" }} />
      </Box>
    );
  }

  return (
    <Box
      aria-label="Consumer group workspace"
      component={component}
      sx={{ bgcolor: "background.paper", minHeight: 0, overflow: "auto" }}
    >
      <Stack spacing={2} sx={{ minWidth: 0, p: 2 }}>
        {snapshot.state === "stale" ? (
          <Alert severity="warning">
            <Typography component="p" variant="subtitle2">
              Consumer-group detail is stale
            </Typography>
            <Typography component="p" variant="body2">
              {snapshot.error?.summary ?? "The latest detail request did not complete."}
            </Typography>
          </Alert>
        ) : null}

        <Box component="section">
          <Stack direction="row" sx={{ alignItems: "center", justifyContent: "space-between" }}>
            <Typography component="h2" variant="subtitle1">
              Group status
            </Typography>
            <StatusIndicator
              ariaLabel="Consumer group state"
              label={group.state.replaceAll("-", " ")}
              tone={groupTone(group.state)}
            />
          </Stack>
          <Box component="dl" sx={{ m: 0, mt: 0.75 }}>
            <StudioDetailRow label="Protocol type" value={group.protocolType || "Unavailable"} />
            <StudioDetailRow label="Protocol" value={group.protocol || "Unavailable"} />
            <StudioDetailRow label="Members" value={group.members.length.toLocaleString()} />
            <StudioDetailRow
              label="Total confirmed lag"
              value={
                <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
                  {totalLag(group.offsets)}
                </Box>
              }
            />
            <StudioDetailRow
              label="Refreshed"
              value={
                snapshot.refreshedAt === null ? (
                  "Unavailable"
                ) : (
                  <time dateTime={snapshot.refreshedAt} title={snapshot.refreshedAt}>
                    {formatUtcTimestamp(snapshot.refreshedAt)}
                  </time>
                )
              }
            />
          </Box>
        </Box>

        <Divider />

        <Box component="section" sx={{ minWidth: 0 }}>
          <Typography component="h2" variant="subtitle1">
            Members
          </Typography>
          <Typography color="text.secondary" sx={{ mt: 0.25 }} variant="body2">
            {group.members.length.toLocaleString()} active member
            {group.members.length === 1 ? "" : "s"}
            {group.omittedMembers === 0
              ? ""
              : ` · ${group.omittedMembers.toLocaleString()} omitted by the safety bound`}
          </Typography>
          {memberRows.length === 0 ? (
            <Typography color="text.secondary" sx={{ py: 1 }} variant="body2">
              This group has no active members.
            </Typography>
          ) : (
            <Box
              data-testid="consumer-group-members-grid"
              sx={{ height: boundedGridHeight(memberRows.length, 4), mt: 0.75, minWidth: 0 }}
            >
              <DataGrid
                aria-label="Consumer group members"
                columnHeaderHeight={streamSkopeLayout.tableHeaderHeight}
                columns={memberColumns}
                density="compact"
                disableRowSelectionOnClick
                hideFooter
                rowHeight={streamSkopeLayout.tableRowHeight}
                rows={memberRows}
                sx={{
                  border: 0,
                  "& .MuiDataGrid-columnHeaders": { borderBottom: 1, borderColor: "divider" },
                }}
              />
            </Box>
          )}
        </Box>

        <Divider />

        <Box component="section" sx={{ minWidth: 0 }}>
          <Typography component="h2" variant="subtitle1">
            Offsets and lag
          </Typography>
          <Typography color="text.secondary" sx={{ mt: 0.25 }} variant="body2">
            {group.offsets.length.toLocaleString()} committed partition
            {group.offsets.length === 1 ? "" : "s"}
            {group.omittedOffsets === 0
              ? ""
              : ` · ${group.omittedOffsets.toLocaleString()} omitted by the safety bound`}
          </Typography>
          {offsetRows.length === 0 ? (
            <Typography color="text.secondary" sx={{ py: 1 }} variant="body2">
              This group has no committed offsets. Lag is unavailable.
            </Typography>
          ) : (
            <Box
              data-testid="consumer-group-offsets-grid"
              sx={{ height: boundedGridHeight(offsetRows.length, 6), mt: 0.75, minWidth: 0 }}
            >
              <DataGrid
                aria-label="Consumer group offsets"
                columnHeaderHeight={streamSkopeLayout.tableHeaderHeight}
                columns={offsetColumns}
                density="compact"
                disableRowSelectionOnClick
                hideFooter
                rowHeight={streamSkopeLayout.tableRowHeight}
                rows={offsetRows}
                sx={{
                  border: 0,
                  "& .MuiDataGrid-cell": streamSkopeMuiMonospaceTypography,
                  "& .MuiDataGrid-columnHeaders": { borderBottom: 1, borderColor: "divider" },
                }}
              />
            </Box>
          )}
        </Box>
      </Stack>
    </Box>
  );
}
