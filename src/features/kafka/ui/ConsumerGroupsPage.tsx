import { useMemo } from "react";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import type { GridColDef } from "@mui/x-data-grid";

import type {
  KafkaConsumerGroupDetailSnapshot,
  KafkaConsumerGroupInventorySnapshot,
  KafkaConsumerGroupSummary,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import {
  StudioInventoryCellAction,
  StudioInventoryGrid,
} from "../../../platform/ui/StudioInventoryGrid";

import { ConsumerGroupWorkspace } from "./ConsumerGroupWorkspace";
import { ResourcePageHeader, resourcePageGutter } from "./ResourcePageHeader";
import { formatUtcTimestamp } from "./timestamp-presentation";
import { WorkbenchIcon } from "./WorkbenchIcons";

interface ConsumerGroupsPageProperties {
  readonly connected: boolean;
  readonly detail: KafkaConsumerGroupDetailSnapshot;
  readonly filter: string;
  readonly inventory: KafkaConsumerGroupInventorySnapshot;
  readonly onFilterChange: (value: string) => void;
  readonly onRefresh: () => void;
  readonly onSelect: (groupId: string) => void;
  readonly requestError?: string;
  readonly selectedGroupId: string | null;
}

export function ConsumerGroupsPage({
  connected,
  detail,
  filter,
  inventory,
  onFilterChange,
  onRefresh,
  onSelect,
  requestError,
  selectedGroupId,
}: ConsumerGroupsPageProperties): React.JSX.Element {
  const normalizedFilter = filter.trim().toLocaleLowerCase("en-US");
  const groups = useMemo(
    () =>
      normalizedFilter.length === 0
        ? inventory.groups
        : inventory.groups.filter((group) =>
            group.id.toLocaleLowerCase("en-US").includes(normalizedFilter),
          ),
    [inventory.groups, normalizedFilter],
  );
  const loading = inventory.state === "loading";
  const groupsPresentable = inventory.state === "ready" || inventory.state === "stale";
  const visibleGroups = groupsPresentable ? groups : [];
  const visibleGroupCount = groupsPresentable ? inventory.groups.length : 0;
  const columns = useMemo<readonly GridColDef<KafkaConsumerGroupSummary>[]>(
    () => [
      {
        field: "id",
        flex: 1,
        headerName: "Group ID",
        minWidth: 220,
        renderCell: ({ row }): React.JSX.Element => (
          <StudioInventoryCellAction accessibleName={row.id} onActivate={() => onSelect(row.id)}>
            {row.id}
          </StudioInventoryCellAction>
        ),
        sortable: true,
      },
      {
        field: "state",
        headerName: "State",
        minWidth: 140,
        valueFormatter: (value): string => String(value).replaceAll("-", " "),
      },
      {
        field: "protocolType",
        flex: 0.5,
        headerName: "Protocol",
        minWidth: 140,
        renderCell: ({ row }): string => row.protocolType || row.groupType || "Unavailable",
      },
    ],
    [onSelect],
  );
  const emptyMessage =
    inventory.state === "unavailable"
      ? "Connect a profile to load consumer groups."
      : inventory.state === "denied" || inventory.state === "failed"
        ? null
        : inventory.state === "empty" || (groupsPresentable && inventory.groups.length === 0)
          ? "No consumer groups found."
          : `No consumer groups match “${filter.trim()}”.`;
  const inventoryState =
    inventory.state === "ready" || inventory.state === "empty"
      ? "Current"
      : inventory.state === "loading"
        ? "Loading"
        : inventory.state === "denied"
          ? "Access denied"
          : inventory.state === "failed"
            ? "Unavailable"
            : inventory.state === "stale"
              ? "Stale"
              : "Not loaded";

  if (selectedGroupId !== null) {
    return (
      <Box
        aria-label="Consumer group detail page"
        component="main"
        sx={{ display: "grid", gridTemplateRows: "auto minmax(0, 1fr)", minHeight: 0 }}
      >
        <ResourcePageHeader
          action={
            <Button
              aria-label={`Refresh consumer group ${selectedGroupId}`}
              disabled={!connected || detail.state === "loading"}
              onClick={() => onSelect(selectedGroupId)}
              startIcon={<WorkbenchIcon name="refresh" />}
              variant="outlined"
            >
              Refresh
            </Button>
          }
          description="Members, assignments, committed offsets, and confirmed lag."
          title={selectedGroupId}
        />
        <ConsumerGroupWorkspace
          component="section"
          selectedGroupId={selectedGroupId}
          snapshot={detail}
        />
      </Box>
    );
  }

  return (
    <Box
      aria-label="Consumer groups page"
      component="main"
      sx={{
        bgcolor: "background.default",
        display: "grid",
        gridTemplateRows: "auto minmax(0, 1fr)",
        height: "100%",
        minHeight: 0,
        overflow: "hidden",
      }}
    >
      <ResourcePageHeader
        action={
          <Button
            aria-label="Refresh consumer groups"
            disabled={!connected || loading}
            onClick={onRefresh}
            startIcon={<WorkbenchIcon name="refresh" />}
            variant="outlined"
          >
            Refresh
          </Button>
        }
        description="Inspect group state and follow confirmed lag to the owning topic partitions."
        title="Consumer Groups"
      />
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          minHeight: 0,
          overflow: "hidden",
          px: resourcePageGutter,
          py: { md: 2, xs: 1.5 },
        }}
      >
        <Stack
          aria-label="Consumer-group inventory status"
          component="section"
          direction={{ sm: "row", xs: "column" }}
          role="group"
          sx={{
            alignItems: { sm: "center", xs: "stretch" },
            gap: 1.5,
            justifyContent: "space-between",
            mb: 1.25,
          }}
        >
          <Box sx={{ flex: "1 1 420px", maxWidth: 520 }}>
            <TextField
              fullWidth
              onChange={(event) => onFilterChange(event.target.value)}
              placeholder="Search consumer groups"
              slotProps={{
                htmlInput: { "aria-label": "Search consumer groups", type: "search" },
              }}
              value={filter}
            />
          </Box>
          <Typography color="text.secondary" noWrap variant="caption">
            {normalizedFilter.length === 0
              ? `${visibleGroupCount.toLocaleString()} ${visibleGroupCount === 1 ? "group" : "groups"}`
              : `${visibleGroups.length.toLocaleString()} of ${visibleGroupCount.toLocaleString()} groups`}
            {` · ${inventoryState} · `}
            {inventory.refreshedAt === null ? (
              "Not refreshed"
            ) : (
              <time dateTime={inventory.refreshedAt}>
                {formatUtcTimestamp(inventory.refreshedAt)}
              </time>
            )}
          </Typography>
        </Stack>
        {requestError === undefined ? null : (
          <Alert severity="error" sx={{ mb: 1 }}>
            {requestError}
          </Alert>
        )}
        {inventory.state === "denied" || inventory.state === "failed" ? (
          <Alert severity="error" sx={{ mb: 1 }}>
            <Typography component="p" variant="subtitle2">
              {inventory.state === "denied"
                ? "Consumer-group access denied"
                : "Consumer groups unavailable"}
            </Typography>
            <Typography component="p" variant="body2">
              {inventory.error?.summary ?? "Consumer-group metadata could not be loaded."}
            </Typography>
            <Typography component="p" variant="body2">
              {inventory.error?.recovery ?? "Review Activity and retry."}
            </Typography>
          </Alert>
        ) : null}
        <StudioInventoryGrid
          ariaLabel="Kafka consumer groups"
          columns={columns}
          emptyMessage={emptyMessage}
          loading={loading}
          loadingMessage="Loading consumer groups…"
          rows={visibleGroups}
          stateLabel="Consumer-group list status"
          testId="consumer-group-inventory-data-plane"
        />
      </Box>
    </Box>
  );
}
