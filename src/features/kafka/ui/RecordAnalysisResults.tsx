import { useMemo, useState } from "react";
import {
  Box,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";

import {
  recordAnalysisGroupIdentity,
  type RecordAnalysisOperation,
} from "../contracts/record-analysis";
import {
  StudioAlert as Alert,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
} from "../../../platform/ui/controls";

import { ANALYSIS_REASONS, analysisCell, analysisCountLabel } from "./record-analysis-presentation";
import { QueryReadCoverage } from "./QueryReadCoverage";

export function RecordAnalysisResults({
  operation,
}: {
  readonly operation: RecordAnalysisOperation;
}): React.JSX.Element {
  const [order, setOrder] = useState<"count-desc" | "count-asc" | "type">("count-desc");
  const result = operation.result;
  const grouping = result?.grouping;
  const groups = useMemo(
    () =>
      [...(grouping?.groups ?? [])].sort((left, right) => {
        const fallback = recordAnalysisGroupIdentity(left.key).localeCompare(
          recordAnalysisGroupIdentity(right.key),
        );
        return order === "type"
          ? fallback
          : (order === "count-desc" ? -1 : 1) * (left.count - right.count) || fallback;
      }),
    [grouping, order],
  );
  const excluded = grouping ? grouping.excluded.masked + grouping.excluded.unavailable : 0;
  return (
    <Stack spacing={2}>
      <Typography role="status" aria-label="Analysis count" variant="subtitle2">
        {analysisCountLabel(operation)}
      </Typography>
      <Typography variant="body2">
        {operation.input.topic} · {operation.source.connectionName} ·{" "}
        {operation.counts.scannedRecords.toLocaleString()} scanned ·{" "}
        {(operation.counts.scannedBytes / 1_048_576).toFixed(2)} MiB scanned ·{" "}
        {operation.counts.passes.toLocaleString()}{" "}
        {operation.counts.passes === 1 ? "pass" : "passes"}
      </Typography>
      {operation.reason && (
        <Typography variant="body2">{ANALYSIS_REASONS[operation.reason]}</Typography>
      )}
      {operation.counts.unavailableRecords > 0 && (
        <Alert severity="warning">
          {operation.counts.unavailableRecords.toLocaleString()} scanned records could not be
          evaluated by the filter. They are excluded from the match count.
        </Alert>
      )}
      {operation.error && (
        <Alert severity="error">
          {operation.error.summary} {operation.error.recovery}
        </Alert>
      )}
      <Box component="details">
        <Typography component="summary" variant="body2">
          Captured range, settings and limits
        </Typography>
        <Typography variant="body2">
          {operation.input.range.mode === "earliest"
            ? "From earliest retained offsets"
            : `${new Date(operation.input.range.startTimeMs).toISOString()} → ${new Date(operation.input.range.endTimeMs).toISOString()}`}{" "}
          · Started {operation.startedAt}
        </Typography>
        <Typography variant="body2">
          Key encoding: {operation.settings.codecs.key}; value encoding:{" "}
          {operation.settings.codecs.value}. Maximum {operation.input.maxRecords.toLocaleString()}{" "}
          matches; {operation.limits.scanRecords.toLocaleString()} scanned records;{" "}
          {operation.limits.durationMs / 60_000} minutes.
        </Typography>
        <Typography
          component="pre"
          variant="caption"
          sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
        >
          {JSON.stringify(
            {
              columns: operation.input.columns,
              groupBy: operation.input.groupBy,
              filters: operation.input.search,
              protection: operation.settings.protection,
            },
            null,
            2,
          )}
        </Typography>
        <QueryReadCoverage coverage={operation.coverage} search />
      </Box>
      {grouping && (
        <Box component="section" aria-label="Count by results">
          <Typography variant="subtitle2">
            Count by{" "}
            {operation.input.columns.find((column) => column.id === operation.input.groupBy)?.label}
          </Typography>
          <Typography variant="body2">
            {grouping.groupedRecords.toLocaleString()} grouped of{" "}
            {operation.counts.countedRecords.toLocaleString()} counted records.{" "}
            {excluded > 0
              ? `Grouping excludes ${excluded.toLocaleString()} records: ${grouping.excluded.masked.toLocaleString()} masked; ${grouping.excluded.unavailable.toLocaleString()} unavailable or unsupported.`
              : operation.state === "completed"
                ? "Grouping covers every counted record."
                : "Group counts cover the counted prefix only."}
          </Typography>
          <Select
            inputProps={{ "aria-label": "Sort groups by" }}
            value={order}
            onChange={(event) => setOrder(event.target.value)}
            sx={{ my: 1, minWidth: 220 }}
          >
            <MenuItem value="count-desc">Count, highest first</MenuItem>
            <MenuItem value="count-asc">Count, lowest first</MenuItem>
            <MenuItem value="type">Type and value</MenuItem>
          </Select>
          <TableContainer sx={{ maxHeight: 280 }}>
            <Table size="small" stickyHeader aria-label="Analysis groups">
              <TableHead>
                <TableRow>
                  <TableCell>Type</TableCell>
                  <TableCell>Value</TableCell>
                  <TableCell align="right">Count</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {groups.map((group) => {
                  const cell = analysisCell(group.key);
                  return (
                    <TableRow key={recordAnalysisGroupIdentity(group.key)}>
                      <TableCell>{cell.type}</TableCell>
                      <TableCell sx={{ overflowWrap: "anywhere", maxWidth: 400 }}>
                        {cell.text}
                      </TableCell>
                      <TableCell align="right">{group.count.toLocaleString()}</TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
          {groups.length === 0 && (
            <Typography variant="body2">No known grouping values were counted.</Typography>
          )}
        </Box>
      )}
      {result && (
        <Box component="section" aria-label="Projection preview">
          <Typography variant="subtitle2">Projection preview</Typography>
          <Typography variant="body2">
            First {result.preview.length.toLocaleString()} of{" "}
            {operation.counts.countedRecords.toLocaleString()} counted records shown;{" "}
            {result.previewOmittedRecords.toLocaleString()} omitted from the preview. The preview
            limit does not stop counting. Rows follow the captured read order, not global timestamp
            order.
          </Typography>
          {result.preview.some((row) =>
            row.cells.some((cell) => cell.state === "unavailable" && cell.reason === "value-limit"),
          ) && (
            <Typography variant="caption" color="text.secondary">
              Oversized preview cells are unavailable here; whole-count field totals can still
              identify those values as scalars.
            </Typography>
          )}
          <TableContainer sx={{ maxHeight: 320 }}>
            <Table stickyHeader size="small" aria-label="Analysis preview">
              <TableHead>
                <TableRow>
                  <TableCell>Partition</TableCell>
                  <TableCell>Offset</TableCell>
                  <TableCell>Timestamp</TableCell>
                  {operation.input.columns.map((column) => (
                    <TableCell key={column.id}>{column.label}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {result.preview.map((row) => (
                  <TableRow key={`${row.partition}:${row.offset}`}>
                    <TableCell>{row.partition}</TableCell>
                    <TableCell>{row.offset}</TableCell>
                    <TableCell sx={{ whiteSpace: "nowrap" }}>{row.timestamp}</TableCell>
                    {row.cells.map((value, index) => {
                      const cell = analysisCell(value);
                      return (
                        <TableCell
                          key={operation.input.columns[index]?.id ?? index}
                          aria-label={`${cell.type}: ${cell.text}`}
                          sx={{ minWidth: 130, maxWidth: 300, overflowWrap: "anywhere" }}
                        >
                          {cell.text}
                        </TableCell>
                      );
                    })}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          {result.preview.length === 0 && (
            <Typography variant="body2">No records are retained in the preview.</Typography>
          )}
        </Box>
      )}
      {result && result.columns.length > 0 && (
        <Box component="details">
          <Typography component="summary" variant="body2">
            Field availability across all counted records
          </Typography>
          <TableContainer>
            <Table size="small" aria-label="Analysis field availability">
              <TableHead>
                <TableRow>
                  <TableCell>Field</TableCell>
                  <TableCell>Scalar</TableCell>
                  <TableCell>Missing</TableCell>
                  <TableCell>Null key</TableCell>
                  <TableCell>Tombstone</TableCell>
                  <TableCell>Masked</TableCell>
                  <TableCell>Unavailable</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {result.columns.map((column) => (
                  <TableRow key={column.columnId}>
                    <TableCell>
                      {operation.input.columns.find((input) => input.id === column.columnId)?.label}
                    </TableCell>
                    <TableCell>{column.scalar}</TableCell>
                    <TableCell>{column.missing}</TableCell>
                    <TableCell>{column.nullKey}</TableCell>
                    <TableCell>{column.tombstone}</TableCell>
                    <TableCell>{column.masked}</TableCell>
                    <TableCell>{column.unavailable}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      )}
    </Stack>
  );
}
