import { useMemo } from "react";
import { Typography } from "@mui/material";
import { DataGrid, type GridColDef, type GridRowSelectionModel } from "@mui/x-data-grid";

import type { KafkaExploredMessage, KafkaRuleSeverity } from "../contracts";
import {
  streamSkopeLayout,
  streamSkopeMuiMonospaceTypography,
} from "../../../platform/ui/createStreamSkopeTheme";

import { highestKafkaRuleSeverity } from "./state";

export interface MessageDataGridProperties {
  readonly compactInspectorColumns?: boolean;
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

const columns: readonly GridColDef<KafkaExploredMessage>[] = [
  {
    field: "timestamp",
    flex: 1.1,
    headerName: "Timestamp",
    minWidth: 142,
  },
  {
    field: "key",
    flex: 0.8,
    headerName: "Key",
    minWidth: 92,
  },
  {
    field: "preview",
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
  messages,
  onSelectMessage,
  selectedMessageId,
}: MessageDataGridProperties): React.JSX.Element {
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
      columnVisibilityModel={
        compactInspectorColumns ? { offset: false, partition: false, preview: false } : {}
      }
      columns={compactInspectorColumns ? compactColumns : columns}
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
