import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import type { ActivityEntry, ActivitySeverity, HostTextDocument } from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioCheckbox as Checkbox,
  StudioFormControl as FormControl,
  StudioIconButton as IconButton,
  StudioInputLabel as InputLabel,
  StudioLabeledControl as FormControlLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import {
  streamSkopeLayout,
  streamSkopeMuiMonospaceTypography,
} from "../../../platform/ui/createStreamSkopeTheme";

import {
  browserTextDocumentTransfer,
  type TextDocumentTransferPort,
} from "./text-document-transfer";
import { WorkbenchIcon } from "./WorkbenchIcons";

interface ActivityLogDrawerProperties {
  readonly initialQuery?: string;
  readonly height?: number;
  readonly entries: readonly ActivityEntry[];
  readonly onHeightChange?: (height: number) => void;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly transfer?: TextDocumentTransferPort;
}

interface ActivityResize {
  readonly id: number;
  readonly startHeight: number;
  readonly startY: number;
}

function ActivityDockSeparator({
  height,
  onChange,
}: {
  readonly height: number;
  readonly onChange: (height: number) => void;
}): React.JSX.Element {
  const resize = useRef<ActivityResize | null>(null);
  const clamp = (value: number): number =>
    Math.min(
      streamSkopeLayout.activityMaximumHeight,
      Math.max(streamSkopeLayout.activityMinimumHeight, Math.round(value)),
    );
  return (
    <Box
      aria-label="Resize Activity dock"
      aria-orientation="horizontal"
      aria-valuemax={streamSkopeLayout.activityMaximumHeight}
      aria-valuemin={streamSkopeLayout.activityMinimumHeight}
      aria-valuenow={height}
      onKeyDown={(event) => {
        const step = event.shiftKey ? 48 : 16;
        if (event.key === "ArrowUp") {
          event.preventDefault();
          onChange(clamp(height + step));
        } else if (event.key === "ArrowDown") {
          event.preventDefault();
          onChange(clamp(height - step));
        } else if (event.key === "Home") {
          event.preventDefault();
          onChange(streamSkopeLayout.activityMinimumHeight);
        } else if (event.key === "End") {
          event.preventDefault();
          onChange(streamSkopeLayout.activityMaximumHeight);
        }
      }}
      onPointerDown={(event) => {
        resize.current = { id: event.pointerId, startHeight: height, startY: event.clientY };
        event.currentTarget.setPointerCapture?.(event.pointerId);
      }}
      onPointerMove={(event) => {
        const current = resize.current;
        if (current === null || current.id !== event.pointerId) return;
        onChange(clamp(current.startHeight + current.startY - event.clientY));
      }}
      onPointerUp={(event) => {
        if (resize.current?.id !== event.pointerId) return;
        resize.current = null;
        event.currentTarget.releasePointerCapture?.(event.pointerId);
      }}
      role="separator"
      sx={{
        alignItems: "center",
        bgcolor: "background.paper",
        borderTop: 1,
        borderColor: "divider",
        cursor: "row-resize",
        display: "flex",
        height: streamSkopeLayout.activitySeparatorHeight,
        justifyContent: "center",
        position: "relative",
        touchAction: "none",
        "&:hover [data-testid=activity-resize-grip], &:focus-visible [data-testid=activity-resize-grip]":
          {
            bgcolor: "primary.main",
          },
      }}
      tabIndex={0}
    >
      <Box
        data-testid="activity-resize-grip"
        sx={{ bgcolor: "text.disabled", borderRadius: 999, height: 3, width: 36 }}
      />
    </Box>
  );
}

type SeverityFilter = "all" | ActivitySeverity;

function escapedLogValue(value: string): string {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll("\r\n", "\\n")
    .replaceAll("\r", "\\n")
    .replaceAll("\n", "\\n")
    .replaceAll('"', '\\"');
}

function structuredLogLine(entry: ActivityEntry): string {
  return [
    `time="${escapedLogValue(entry.timestamp)}"`,
    `level=${entry.severity}`,
    `outcome=${entry.outcome}`,
    `operation="${escapedLogValue(entry.operation)}"`,
    `object="${escapedLogValue(entry.object)}"`,
    `correlation_id="${escapedLogValue(entry.correlationId)}"`,
    `msg="${escapedLogValue(entry.detail)}"`,
  ].join(" ");
}

function matchesQuery(entry: ActivityEntry, query: string): boolean {
  if (query.length === 0) {
    return true;
  }
  return [
    entry.timestamp,
    entry.severity,
    entry.outcome,
    entry.operation,
    entry.object,
    entry.correlationId,
    entry.detail,
  ].some((value) => value.toLowerCase().includes(query));
}

function exportDocument(entries: readonly ActivityEntry[]): HostTextDocument {
  const content = `${JSON.stringify(entries, null, 2)}\n`;
  const timestamp = new Date().toISOString().replaceAll(":", "-").replaceAll(".", "-");
  return {
    byteSize: new TextEncoder().encode(content).byteLength,
    content,
    fileName: `streamskope-activity-${timestamp}.json`,
    mediaType: "application/json",
  };
}

export function ActivityLogDrawer({
  initialQuery = "",
  height = streamSkopeLayout.activityHeight,
  entries,
  onHeightChange = (): void => undefined,
  onClose,
  open,
  transfer = browserTextDocumentTransfer,
}: ActivityLogDrawerProperties): React.JSX.Element {
  const [followLatest, setFollowLatest] = useState(true);
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [query, setQuery] = useState("");
  useEffect(() => {
    if (open) {
      setQuery(initialQuery);
      if (initialQuery !== "") {
        setFiltersOpen(true);
        setSeverity("all");
      }
    }
  }, [initialQuery, open]);
  const [severity, setSeverity] = useState<SeverityFilter>("all");
  const [wrapLines, setWrapLines] = useState(false);
  const [transferError, setTransferError] = useState<string>();
  const [transferStatus, setTransferStatus] = useState("");
  const rawLog = useRef<HTMLPreElement>(null);
  const readingAnchor = useRef<{ id: string; offset: number } | undefined>(undefined);
  const [readingExpired, setReadingExpired] = useState(false);
  function rememberReadingPosition(): void {
    const log = rawLog.current;
    if (!log) return;
    const top = log.getBoundingClientRect().top;
    const row = Array.from(log.children).find(
      (child) => child.getBoundingClientRect().bottom > top,
    );
    if (row instanceof HTMLElement && row.dataset.activityId !== undefined)
      readingAnchor.current = {
        id: row.dataset.activityId,
        offset: row.getBoundingClientRect().top - top,
      };
  }
  const normalizedQuery = query.trim().toLowerCase();
  const activeFilterCount = Number(normalizedQuery !== "") + Number(severity !== "all");
  const visibleEntries = useMemo(
    () =>
      entries.filter(
        (entry) =>
          (severity === "all" || entry.severity === severity) &&
          matchesQuery(entry, normalizedQuery),
      ),
    [entries, normalizedQuery, severity],
  );
  const highestSeverity = visibleEntries.some((entry) => entry.severity === "error")
    ? "Error"
    : visibleEntries.some((entry) => entry.severity === "warning")
      ? "Warning"
      : visibleEntries.length > 0
        ? "Info"
        : null;
  const entryLabel = `${visibleEntries.length.toLocaleString()} ${
    visibleEntries.length === 1 ? "entry" : "entries"
  }${highestSeverity === null ? "" : ` · ${highestSeverity}`}`;

  useLayoutEffect(() => {
    const log = rawLog.current;
    if (open && followLatest) {
      log?.scrollTo?.({ top: log.scrollHeight });
      readingAnchor.current = undefined;
      setReadingExpired(false);
    } else if (open && log && readingAnchor.current) {
      const anchor = readingAnchor.current;
      const row = Array.from(log.children).find(
        (child) => child instanceof HTMLElement && child.dataset.activityId === anchor.id,
      );
      if (row)
        log.scrollTop +=
          row.getBoundingClientRect().top - log.getBoundingClientRect().top - anchor.offset;
      else {
        log.scrollTop = 0;
        setReadingExpired(!entries.some((entry) => entry.id === anchor.id));
      }
      rememberReadingPosition();
    }
  }, [followLatest, open, visibleEntries, entries, wrapLines]);

  async function copyRaw(): Promise<void> {
    if (visibleEntries.length === 0) {
      return;
    }
    setTransferError(undefined);
    setTransferStatus("");
    try {
      await transfer.copy(visibleEntries.map(structuredLogLine).join("\n"));
      setTransferStatus("Visible raw logs copied.");
    } catch {
      setTransferError("The raw logs could not be copied. Check permission and retry.");
    }
  }

  async function exportVisible(): Promise<void> {
    if (visibleEntries.length === 0) {
      return;
    }
    setTransferError(undefined);
    setTransferStatus("");
    try {
      const outcome = await transfer.download(exportDocument(visibleEntries));
      setTransferStatus(
        outcome === "cancelled"
          ? "Visible Activity export cancelled."
          : outcome === "saved"
            ? "Visible Activity export saved."
            : "Visible Activity download started.",
      );
    } catch {
      setTransferError("The Activity export failed. No file was saved. Retry the export.");
    }
  }

  return (
    <Box
      aria-label="Activity dock"
      component="section"
      onKeyDown={(event) => {
        if (open && event.key === "Escape") {
          event.preventDefault();
          onClose();
        }
      }}
      role="region"
      sx={{
        bgcolor: "background.paper",
        borderColor: "divider",
        borderTop: open ? 0 : 1,
        display: "grid",
        gridTemplateRows: open
          ? `${String(streamSkopeLayout.activitySeparatorHeight)}px ${String(streamSkopeLayout.activityCollapsedHeight)}px minmax(0, 1fr)`
          : `${String(streamSkopeLayout.activityCollapsedHeight)}px`,
        minHeight: 0,
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      {open ? <ActivityDockSeparator height={height} onChange={onHeightChange} /> : null}
      <Box
        sx={{
          alignItems: "center",
          borderBottom: open ? 1 : 0,
          borderColor: "divider",
          display: "flex",
          minHeight: streamSkopeLayout.activityCollapsedHeight,
          minWidth: 0,
        }}
      >
        <Typography
          component="h2"
          noWrap
          sx={{ flex: 1, minWidth: 0, px: 1.5 }}
          variant="subtitle2"
        >
          Raw logs
        </Typography>
        <Typography
          color="text.secondary"
          noWrap
          sx={{ flex: "0 1 auto", px: 0.75 }}
          variant="caption"
        >
          {entryLabel}
        </Typography>
        {open ? (
          <>
            <Button
              aria-pressed={followLatest}
              onClick={() => setFollowLatest((current) => !current)}
              sx={{ whiteSpace: "nowrap", px: 0.75 }}
              variant="text"
            >
              {followLatest ? "Follow latest" : "Resume live"}
            </Button>
            <Button
              aria-controls="activity-filter-controls"
              aria-expanded={filtersOpen}
              onClick={() => setFiltersOpen((current) => !current)}
              sx={{ minHeight: 24, px: 0.75 }}
              variant="text"
            >
              {activeFilterCount === 0 ? "Filter" : `Filter (${String(activeFilterCount)})`}
            </Button>
            <Button
              aria-label="Copy raw logs"
              disabled={visibleEntries.length === 0}
              onClick={() => void copyRaw()}
              sx={{ minHeight: 24, px: 0.75 }}
              variant="text"
            >
              Copy
            </Button>
            <Button
              aria-label="Export visible"
              disabled={visibleEntries.length === 0}
              onClick={() => void exportVisible()}
              sx={{ minHeight: 24, px: 0.75 }}
              variant="text"
            >
              Export
            </Button>
          </>
        ) : null}
        <IconButton
          aria-label={`${open ? "Collapse" : "Expand"} Activity`}
          onClick={onClose}
          title={`${open ? "Collapse" : "Expand"} Activity`}
        >
          <WorkbenchIcon fontSize="small" name={open ? "expand" : "collapse"} />
        </IconButton>
      </Box>

      {open ? (
        <Box
          aria-label="Activity log"
          component="aside"
          sx={{
            display: "grid",
            gridTemplateRows: filtersOpen ? "auto minmax(0, 1fr) auto" : "minmax(0, 1fr) auto",
            minHeight: 0,
          }}
        >
          {filtersOpen ? (
            <Stack
              direction="row"
              id="activity-filter-controls"
              spacing={0.75}
              sx={{
                alignItems: "center",
                borderBottom: 1,
                borderColor: "divider",
                flexWrap: "nowrap",
                px: 1,
                py: 0.5,
              }}
            >
              <TextField
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search activity"
                slotProps={{ htmlInput: { "aria-label": "Search activity", type: "search" } }}
                sx={{ flex: "1 1 220px", maxWidth: 360 }}
                value={query}
              />
              <FormControl sx={{ minWidth: 108 }}>
                <InputLabel id="activity-severity-label">Severity</InputLabel>
                <Select
                  id="activity-severity"
                  label="Severity"
                  labelId="activity-severity-label"
                  onChange={(event) => setSeverity(event.target.value)}
                  value={severity}
                >
                  <MenuItem value="all">All</MenuItem>
                  <MenuItem value="info">Info</MenuItem>
                  <MenuItem value="warning">Warning</MenuItem>
                  <MenuItem value="error">Error</MenuItem>
                </Select>
              </FormControl>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={wrapLines}
                    onChange={(event) => setWrapLines(event.target.checked)}
                  />
                }
                label="Wrap lines"
                sx={{ m: 0 }}
              />
            </Stack>
          ) : null}

          {entries.length === 0 ? (
            <Box sx={{ bgcolor: "background.default", p: 2 }}>
              <Typography color="text.secondary" variant="body2">
                No activity recorded.
              </Typography>
            </Box>
          ) : visibleEntries.length === 0 ? (
            <Box sx={{ bgcolor: "background.default", p: 2 }}>
              <Typography color="text.secondary" variant="body2">
                No Activity entries match the current filters.
              </Typography>
            </Box>
          ) : (
            <Box
              aria-label="Raw activity log"
              aria-relevant="additions text"
              component="pre"
              aria-live={followLatest ? "polite" : "off"}
              onScroll={(event) => {
                const target = event.currentTarget;
                if (target.scrollHeight - target.clientHeight - target.scrollTop > 4) {
                  setFollowLatest(false);
                  rememberReadingPosition();
                }
              }}
              ref={rawLog}
              role="log"
              sx={[
                streamSkopeMuiMonospaceTypography,
                {
                  bgcolor: "var(--streamskope-surface-recessed)",
                  m: 0,
                  minHeight: 0,
                  overflow: "auto",
                  overflowAnchor: "none",
                  overflowWrap: wrapLines ? "anywhere" : "normal",
                  p: 1.5,
                  tabSize: 2,
                  whiteSpace: wrapLines ? "pre-wrap" : "pre",
                },
              ]}
              tabIndex={0}
            >
              {visibleEntries.map((entry) => (
                <Box
                  component="span"
                  key={entry.id}
                  data-activity-id={entry.id}
                  sx={{ display: "block" }}
                >
                  {structuredLogLine(entry)}
                </Box>
              ))}
            </Box>
          )}
          {readingExpired ? (
            <Typography role="status" variant="caption">
              Previously viewed events are no longer retained. Showing the earliest available event.
            </Typography>
          ) : null}
          {transferError === undefined && transferStatus.length === 0 ? null : (
            <Box sx={{ borderTop: 1, borderColor: "divider", px: 1.5, py: 0.75 }}>
              {transferError === undefined ? null : <Alert severity="error">{transferError}</Alert>}
              {transferStatus.length === 0 ? null : (
                <Typography aria-live="polite" role="status" variant="caption">
                  {transferStatus}
                </Typography>
              )}
            </Box>
          )}
        </Box>
      ) : null}
    </Box>
  );
}
