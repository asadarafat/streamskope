import { useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import { RECORD_ANALYSIS_LIMITS, type RecordAnalysisColumn } from "../contracts/record-analysis";
import { parseRecordAnalysisInput } from "../contracts/record-analysis-validation";
import { compileKafkaProjectionPath } from "../contracts/rule-expression-parser";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioCheckbox as Checkbox,
  StudioLabeledControl as LabeledControl,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
  StudioTab as Tab,
  StudioTabs as Tabs,
} from "../../../platform/ui/controls";

import {
  FiniteRangeControls,
  finiteRangeError,
  finiteRangeInput,
  initialFiniteRangeDraft,
} from "./FiniteRangeControls";
import type { KafkaMessageFilters } from "./message-operations";
import { RecordAnalysisResults } from "./RecordAnalysisResults";
import type { RecordAnalysisController } from "./use-record-analysis";

function pathError(path: string): string | undefined {
  try {
    compileKafkaProjectionPath(path);
    return undefined;
  } catch (error) {
    return error instanceof Error ? error.message : "Enter a supported field path.";
  }
}
export function RecordAnalysisDialog({
  topic,
  filters,
  controller,
  initialTab = "setup",
  onClose,
}: {
  readonly topic: string;
  readonly filters: KafkaMessageFilters;
  readonly controller: RecordAnalysisController;
  readonly initialTab?: "setup" | "results";
  readonly onClose: () => void;
}): React.JSX.Element {
  const [tab, setTab] = useState(initialTab);
  const [range, setRange] = useState(() => initialFiniteRangeDraft(RECORD_ANALYSIS_LIMITS.records));
  const [columns, setColumns] = useState<readonly RecordAnalysisColumn[]>([]);
  const [groupBy, setGroupBy] = useState<string | null>(null);
  const [replace, setReplace] = useState(false);
  const [error, setError] = useState<string>();
  const operation = controller.snapshot?.operation;
  const active =
    operation !== undefined &&
    operation !== null &&
    ["preparing", "reading", "stopping"].includes(operation.state);
  const replacing = operation?.result !== null && operation?.result !== undefined;
  const blocked =
    active ||
    controller.busy ||
    !controller.connected ||
    controller.snapshot === null ||
    controller.uncertainStart ||
    operation?.reason === "cleanup-failed" ||
    filters.activeRuleMatchesOnly ||
    finiteRangeError(range) !== undefined ||
    columns.some((column) => pathError(column.path) !== undefined) ||
    (replacing && !replace);
  const updateColumn = (id: string, patch: Partial<RecordAnalysisColumn>): void => {
    setError(undefined);
    setColumns((items) =>
      items.map((column) => (column.id === id ? { ...column, ...patch } : column)),
    );
  };
  async function start(): Promise<void> {
    setError(undefined);
    try {
      const input = parseRecordAnalysisInput(
        {
          ...finiteRangeInput(topic, filters, range),
          requestId: crypto.randomUUID(),
          columns: columns.map((column, index) => ({
            ...column,
            label:
              column.label.trim() ||
              (`${column.source} ${column.path}`.length <= 64
                ? `${column.source} ${column.path}`
                : `Field ${index + 1}`),
          })),
          groupBy,
        },
        "Analysis",
      );
      if (await controller.start(input)) setTab("results");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Check the analysis inputs.");
    }
  }
  return (
    <Dialog open onClose={onClose} fullWidth maxWidth="lg" aria-labelledby="record-analysis-title">
      <DialogTitle id="record-analysis-title">Analyze a topic range</DialogTitle>
      <Tabs
        value={tab}
        onChange={(_event, value: "setup" | "results") => setTab(value)}
        aria-label="Range analysis sections"
        sx={{ px: 3 }}
      >
        <Tab
          value="setup"
          label="Setup"
          id="analysis-setup-tab"
          aria-controls="analysis-setup-panel"
        />
        <Tab
          value="results"
          label="Results"
          id="analysis-results-tab"
          aria-controls="analysis-results-panel"
          disabled={!operation}
        />
      </Tabs>
      <DialogContent>
        {tab === "setup" ? (
          <Stack
            role="tabpanel"
            id="analysis-setup-panel"
            aria-labelledby="analysis-setup-tab"
            spacing={2}
          >
            <Typography variant="body2">
              Count matching records in <strong>{topic}</strong>, independently of the grid.
              Optionally preview scalar fields and count by one field.
            </Typography>
            <FiniteRangeControls
              value={range}
              onChange={setRange}
              disabled={controller.busy}
              maximum={RECORD_ANALYSIS_LIMITS.records}
              rangeLabel="Analysis range"
              limitLabel="Maximum counted records"
              actionLabel="Start analysis"
              ruleMatchesOnly={filters.activeRuleMatchesOnly}
            />
            <Box component="details">
              <Typography component="summary" variant="body2">
                Current message filters
              </Typography>
              <Typography
                component="pre"
                variant="caption"
                sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
              >
                {JSON.stringify(
                  finiteRangeInput(topic, filters, { ...range, mode: "earliest" }).search,
                  null,
                  2,
                )}
              </Typography>
            </Box>
            <Typography variant="subtitle2">Fields to preview (optional)</Typography>
            <Typography variant="body2" color="text.secondary">
              Leave empty for a count only. Select Value or Key and use $ for the field itself,
              $.status for a property, or $.items[0].name for an array element. Only scalar values
              are supported; strings keep their type.
            </Typography>
            {columns.map((column, index) => (
              <Box
                key={column.id}
                sx={{
                  display: "grid",
                  gridTemplateColumns: {
                    xs: "1fr",
                    sm: "110px minmax(160px, 1fr) minmax(120px, 1fr) auto",
                  },
                  alignItems: "start",
                  gap: 1,
                }}
              >
                <Select
                  inputProps={{ "aria-label": `Field ${index + 1} source` }}
                  value={column.source}
                  disabled={controller.busy}
                  onChange={(event) => updateColumn(column.id, { source: event.target.value })}
                >
                  <MenuItem value="value">Value</MenuItem>
                  <MenuItem value="key">Key</MenuItem>
                </Select>
                <TextField
                  label={`Field ${index + 1} path`}
                  value={column.path}
                  disabled={controller.busy}
                  error={pathError(column.path) !== undefined}
                  helperText={pathError(column.path)}
                  onChange={(event) => updateColumn(column.id, { path: event.target.value })}
                  slotProps={{ htmlInput: { maxLength: RECORD_ANALYSIS_LIMITS.pathCharacters } }}
                />
                <TextField
                  label={`Field ${index + 1} label`}
                  value={column.label}
                  disabled={controller.busy}
                  onChange={(event) => updateColumn(column.id, { label: event.target.value })}
                  slotProps={{ htmlInput: { maxLength: 64 } }}
                />
                <Button
                  aria-label={`Remove field ${index + 1}`}
                  disabled={controller.busy}
                  onClick={() => {
                    setColumns((items) => items.filter((item) => item.id !== column.id));
                    if (groupBy === column.id) setGroupBy(null);
                  }}
                >
                  Remove
                </Button>
              </Box>
            ))}
            <Button
              disabled={controller.busy || columns.length >= RECORD_ANALYSIS_LIMITS.columns}
              onClick={() =>
                setColumns((items) => [
                  ...items,
                  { id: crypto.randomUUID(), label: "", source: "value", path: "$" },
                ])
              }
              sx={{ alignSelf: "flex-start" }}
            >
              Add field
            </Button>
            <Select
              displayEmpty
              inputProps={{ "aria-label": "Count by" }}
              value={groupBy ?? ""}
              disabled={controller.busy || columns.length === 0}
              onChange={(event) => setGroupBy(event.target.value || null)}
            >
              <MenuItem value="">None — count matching records</MenuItem>
              {columns.map((column) => (
                <MenuItem key={column.id} value={column.id}>
                  {column.label || `${column.source} ${column.path}`} · {column.source}{" "}
                  {column.path}
                </MenuItem>
              ))}
            </Select>
            <Typography variant="body2" color="text.secondary">
              The host captures offsets, filters, encodings and masking at start. Limits: 5 minutes,
              1,000,000 scanned records, 1 GiB scanned, and 256 groups. The preview keeps at most
              200 rows / 256 KiB while counting continues. A new-group or work limit stops the count
              with a partial result.
            </Typography>
            {active && (
              <Alert severity="info">
                Finish or cancel the current analysis before starting another. Editing this setup
                does not change its captured settings.
              </Alert>
            )}
            {replacing && (
              <LabeledControl
                control={
                  <Checkbox
                    checked={replace}
                    onChange={(event) => setReplace(event.target.checked)}
                    disabled={controller.busy}
                  />
                }
                label="Replace the previous analysis result"
              />
            )}
            {error && <Alert severity="error">{error}</Alert>}
          </Stack>
        ) : (
          <Box role="tabpanel" id="analysis-results-panel" aria-labelledby="analysis-results-tab">
            {operation ? (
              <RecordAnalysisResults operation={operation} />
            ) : (
              <Typography>No retained analysis result.</Typography>
            )}
          </Box>
        )}
        {controller.error && (
          <Alert severity="error" sx={{ mt: 1 }}>
            {controller.error}
          </Alert>
        )}
      </DialogContent>
      <DialogActions sx={{ flexWrap: "wrap" }}>
        {active && (
          <Button
            disabled={controller.busy || operation.state === "stopping"}
            onClick={() => void controller.cancel()}
          >
            Cancel analysis
          </Button>
        )}
        {operation && !active && (
          <Button disabled={controller.busy} onClick={() => void controller.discard()}>
            {operation.reason === "cleanup-failed" ? "Retry analysis cleanup" : "Discard analysis"}
          </Button>
        )}
        <Button disabled={controller.busy} onClick={() => void controller.refresh()}>
          Refresh analysis status
        </Button>
        {controller.uncertainStart && (
          <Button
            disabled={controller.busy || !controller.connected}
            onClick={() => {
              void controller.retryStart().then((accepted) => {
                if (accepted) setTab("results");
              });
            }}
          >
            Retry same analysis request
          </Button>
        )}
        <Button onClick={onClose}>Close</Button>
        {tab === "setup" && (
          <Button variant="contained" disabled={blocked} onClick={() => void start()}>
            Start analysis
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
}
