import { useMemo } from "react";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import type { GridColDef } from "@mui/x-data-grid";

import type { HostError, TopicListState } from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { StudioTechnicalText } from "../../../platform/ui/StudioCodeBlock";
import { StudioInventoryCellAction, StudioInventoryGrid } from "../../../platform/ui/StudioInventoryGrid";

import { ResourcePageHeader, resourcePageGutter } from "./ResourcePageHeader";
import { formatUtcTimestamp } from "./timestamp-presentation";
import { WorkbenchIcon } from "./WorkbenchIcons";

interface TopicInventoryPageProperties {
  readonly connected: boolean;
  readonly filter: string;
  readonly onFilterChange: (value: string) => void;
  readonly onOpen: (topic: string) => void;
  readonly onRefresh: () => void;
  readonly refreshedAt: string | null;
  readonly requestError?: string;
  readonly topicError: HostError | null;
  readonly topicListState: "unavailable" | TopicListState;
  readonly topics: readonly string[];
}

interface TopicInventoryRow {
  readonly id: string;
  readonly topic: string;
}

export function TopicInventoryPage({
  connected,
  filter,
  onFilterChange,
  onOpen,
  onRefresh,
  refreshedAt,
  requestError,
  topicError,
  topicListState,
  topics,
}: TopicInventoryPageProperties): React.JSX.Element {
  const normalizedFilter = filter.trim().toLocaleLowerCase("en-US");
  const filteredTopics = useMemo(
    () =>
      normalizedFilter.length === 0
        ? topics
        : topics.filter((topic) => topic.toLocaleLowerCase("en-US").includes(normalizedFilter)),
    [normalizedFilter, topics],
  );
  const loading = topicListState === "loading";
  const topicsPresentable = topicListState === "ready";
  const visibleTopics = topicsPresentable ? filteredTopics : [];
  const visibleTopicCount = topicsPresentable ? topics.length : 0;
  const rows = useMemo<readonly TopicInventoryRow[]>(
    () => visibleTopics.map((topic) => ({ id: topic, topic })),
    [visibleTopics],
  );
  const columns = useMemo<readonly GridColDef<TopicInventoryRow>[]>(
    () => [
      {
        field: "topic",
        flex: 1,
        headerName: "Topic name",
        minWidth: 220,
        renderCell: ({ row }): React.JSX.Element => (
          <StudioInventoryCellAction
            accessibleName={row.topic}
            onActivate={() => onOpen(row.topic)}
          >
            <StudioTechnicalText>{row.topic}</StudioTechnicalText>
          </StudioInventoryCellAction>
        ),
        sortable: true,
      },
    ],
    [onOpen],
  );
  const emptyMessage =
    topicListState === "unavailable"
      ? "Connect a profile to load topics."
      : topicListState === "denied" || topicListState === "failed"
        ? null
        : topicListState === "ready" && topics.length === 0
          ? "Cluster contains no authorized topics."
          : topicListState === "ready" && visibleTopics.length === 0
            ? `No topics match “${filter.trim()}”.`
            : "No topic data available.";
  const inventoryState =
    topicListState === "ready"
      ? "Current"
      : topicListState === "loading"
        ? "Loading"
        : topicListState === "denied"
          ? "Access denied"
          : topicListState === "failed"
            ? "Unavailable"
            : "Not loaded";

  return (
    <Box
      aria-label="Topics page"
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
            aria-label="Refresh topics"
            disabled={!connected || loading}
            onClick={onRefresh}
            startIcon={<WorkbenchIcon name="refresh" />}
            variant="outlined"
          >
            Refresh
          </Button>
        }
        description="Browse the topics the active Kafka principal is authorized to inspect."
        title="Topics"
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
          aria-label="Topic inventory status"
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
              placeholder="Search topics"
              slotProps={{ htmlInput: { "aria-label": "Search topics", type: "search" } }}
              value={filter}
            />
          </Box>
          <Typography color="text.secondary" noWrap variant="caption">
            {normalizedFilter.length === 0
              ? `${visibleTopicCount.toLocaleString()} ${visibleTopicCount === 1 ? "topic" : "topics"}`
              : `${visibleTopics.length.toLocaleString()} of ${visibleTopicCount.toLocaleString()} topics`}
            {` · ${inventoryState} · `}
            {refreshedAt === null ? (
              "Not refreshed"
            ) : (
              <time dateTime={refreshedAt}>{formatUtcTimestamp(refreshedAt)}</time>
            )}
          </Typography>
        </Stack>

        {requestError === undefined ? null : (
          <Alert severity="error" sx={{ mb: 1 }}>
            {requestError}
          </Alert>
        )}
        {topicListState === "denied" || topicListState === "failed" ? (
          <Alert severity="error" sx={{ mb: 1 }}>
            <Typography component="p" variant="subtitle2">
              {topicListState === "denied" ? "Topic access denied" : "Topics unavailable"}
            </Typography>
            <Typography component="p" variant="body2">
              {topicError?.summary ?? "Kafka topic metadata could not be loaded."}
            </Typography>
            <Typography component="p" variant="body2">
              {topicError?.recovery ?? "Review Activity and retry."}
            </Typography>
          </Alert>
        ) : null}

        <StudioInventoryGrid
          ariaLabel="Kafka topics"
          columns={columns}
          emptyMessage={emptyMessage}
          loading={loading}
          loadingMessage="Loading topics…"
          rows={rows}
          stateLabel="Topic list status"
          testId="topic-inventory-data-plane"
        />
      </Box>
    </Box>
  );
}
