import { useMemo } from "react";
import { Typography } from "@mui/material";
import { DataGrid, type GridColDef, type GridRowSelectionModel } from "@mui/x-data-grid";

import {
  KAFKA_MESSAGE_COLUMN_MIN_WIDTHS,
  KAFKA_INVESTIGATION_VIEW_LIMITS,
  type KafkaMessageViewColumn,
  type KafkaExploredMessage,
  type KafkaRuleSeverity,
} from "../contracts";
import {
  streamSkopeLayout,
  streamSkopeMuiMonospaceTypography,
} from "../../../platform/ui/createStreamSkopeTheme";

import {
  useMessageViewPresentation,
  type MessageViewPresentationController,
} from "./use-message-view-presentation";
import { messageGridVisibility, changedMessageGridColumns } from "./message-grid-presentation";
import { highestKafkaRuleSeverity } from "./state";

export interface MessageDataGridProperties {
  readonly compactInspectorColumns?: boolean;
  readonly presentation?: MessageViewPresentationController;
  readonly messages: readonly KafkaExploredMessage[];
  readonly onSelectMessage: (id: string | null) => void;
  readonly selectedMessageId: string | null;
}

function severityLabel(severity: KafkaRuleSeverity): string {
  return `${severity.slice(0, 1).toUpperCase()}${severity.slice(1)}`;
}

function ruleResultLabel(message: KafkaExploredMessage): string {
  const evaluation = message.ruleEvaluation;
  if (evaluation.state === "unavailable") {
    return "Unavailable";
  }
  if (evaluation.activeMatchCount > 0) {
    const severity = highestKafkaRuleSeverity(message);
    const label = `${evaluation.activeMatchCount.toLocaleString()} ${
      evaluation.activeMatchCount === 1 ? "match" : "matches"
    }${severity === null ? "" : ` · ${severityLabel(severity)}`}`;
    return evaluation.state === "partial" ? `${label} · Partial` : label;
  }
  if (evaluation.suppressedMatchCount > 0) {
    const label = `${evaluation.suppressedMatchCount.toLocaleString()} suppressed`;
    return evaluation.state === "partial" ? `${label} · Partial` : label;
  }
  return evaluation.state === "partial" ? "Partial" : "—";
}

function recordCell(message: KafkaExploredMessage, part: "key" | "value"): React.JSX.Element {
  const field = message.structured?.[part];
  const fallback = part === "key" ? message.key : message.preview;
  const label =
    field?.state === "error"
      ? "Decoding unavailable"
      : field?.state === "null"
        ? part === "value"
          ? "Kafka null (tombstone)"
          : "Kafka null key"
        : (fallback ?? "");
  return (
    <Typography noWrap variant="body2" title={field?.state === "error" ? field.detail : label}>
      {label}
    </Typography>
  );
}

const columns: readonly GridColDef<KafkaExploredMessage>[] = [
  {
    field: "timestamp",
    flex: 1.1,
    headerName: "Timestamp",
    minWidth: 142,
  },
  {
    field: "key",
    renderCell: ({ row }): React.JSX.Element => recordCell(row, "key"),
    flex: 0.8,
    headerName: "Key",
    minWidth: 92,
  },
  {
    field: "preview",
    renderCell: ({ row }): React.JSX.Element => recordCell(row, "value"),
    flex: 1.2,
    headerName: "Value",
    minWidth: 130,
  },
  {
    align: "right",
    field: "partition",
    headerAlign: "right",
    headerName: "Partition",
    type: "number",
    width: 74,
  },
  {
    align: "right",
    field: "offset",
    headerAlign: "right",
    headerName: "Offset",
    width: 72,
  },
  {
    field: "rules",
    flex: 0.8,
    headerName: "Rules",
    minWidth: 106,
    renderCell: ({ row }): React.JSX.Element => {
      const label = ruleResultLabel(row);
      return (
        <Typography noWrap title={label} variant="body2">
          {label}
        </Typography>
      );
    },
    sortable: false,
  },
];

const compactColumns: readonly GridColDef<KafkaExploredMessage>[] = columns.map((column) => {
  if (column.field === "timestamp") {
    return { ...column, minWidth: 116 };
  }
  if (column.field === "key") {
    return { ...column, minWidth: 72 };
  }
  if (column.field === "rules") {
    return { ...column, minWidth: 82 };
  }
  return column;
});

export function MessageDataGrid({
  compactInspectorColumns = false,
  presentation,
  messages,
  onSelectMessage,
  selectedMessageId,
}: MessageDataGridProperties): React.JSX.Element {
  const localPresentation = useMessageViewPresentation();
  const settings = presentation ?? localPresentation;
  const visibleColumns = messageGridVisibility(settings.value, compactInspectorColumns);
  const configuredColumns = useMemo(
    () =>
      (compactInspectorColumns ? compactColumns : columns).map((column) => {
        const width = settings.value.columnWidths.find(
          (entry) => entry.column === column.field,
        )?.pixels;
        return {
          ...column,
          maxWidth: KAFKA_INVESTIGATION_VIEW_LIMITS.columnWidthMaximum,
          ...(width === undefined ? {} : { width, flex: 0 }),
        };
      }),
    [compactInspectorColumns, settings.value.columnWidths],
  );
  const rowSelectionModel = useMemo<GridRowSelectionModel>(
    () => ({
      ids: new Set(selectedMessageId === null ? [] : [selectedMessageId]),
      type: "include",
    }),
    [selectedMessageId],
  );

  return (
    <DataGrid
      aria-label="Kafka messages"
      columnHeaderHeight={streamSkopeLayout.tableHeaderHeight}
      columnVisibilityModel={visibleColumns}
      onColumnVisibilityModelChange={(model) =>
        settings.change({
          visibleColumns: changedMessageGridColumns(settings.value, compactInspectorColumns, model),
        })
      }
      onColumnWidthChange={({ colDef, width }) => {
        const column = colDef.field as KafkaMessageViewColumn;
        if (!(column in KAFKA_MESSAGE_COLUMN_MIN_WIDTHS)) return;
        const pixels = Math.max(
          KAFKA_MESSAGE_COLUMN_MIN_WIDTHS[column],
          Math.min(KAFKA_INVESTIGATION_VIEW_LIMITS.columnWidthMaximum, Math.round(width)),
        );
        settings.change({
          columnWidths: [
            ...settings.value.columnWidths.filter((entry) => entry.column !== column),
            { column, pixels },
          ],
        });
      }}
      columns={configuredColumns}
      density="compact"
      disableColumnMenu={false}
      disableMultipleRowSelection
      hideFooter
      keepNonExistentRowsSelected
      onRowSelectionModelChange={(model) => {
        const selected = model.type === "include" ? model.ids.values().next().value : undefined;
        onSelectMessage(selected === undefined ? null : String(selected));
      }}
      rowHeight={streamSkopeLayout.tableRowHeight}
      rowSelectionModel={rowSelectionModel}
      rows={messages}
      sx={{
        bgcolor: "background.paper",
        border: 0,
        flex: "1 1 0",
        minHeight: 0,
        "& .MuiDataGrid-cell": {
          ...streamSkopeMuiMonospaceTypography,
          alignItems: "center",
          display: "flex",
        },
        "& .MuiDataGrid-columnHeaders": {
          borderBottom: "1px solid var(--streamskope-divider-strong)",
        },
      }}
    />
  );
}

export default MessageDataGrid;
