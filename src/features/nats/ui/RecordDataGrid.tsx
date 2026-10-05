import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import useMediaQuery from "@mui/material/useMediaQuery";
import {
  DataGrid,
  type GridCellParams,
  type GridColDef,
  type GridRowParams,
} from "@mui/x-data-grid";

import { streamSkopeGeometry } from "../../../platform/ui/studioTokens";
import type { NatsRecord } from "../contracts";

const columns: readonly GridColDef<NatsRecord>[] = [
  { field: "subject", headerName: "Subject", flex: 2, minWidth: 150 },
  { field: "receivedAt", headerName: "Host received (UTC)", width: 215 },
  { field: "payloadBytes", headerName: "Bytes", type: "number", width: 90 },
  {
    field: "preview",
    headerName: "Payload preview",
    flex: 3,
    minWidth: 160,
    renderCell: (parameters): React.JSX.Element => (
      <Typography noWrap sx={{ fontFamily: "monospace", minWidth: 0 }} variant="body2">
        {parameters.row.payloadBytes === 0 ? "Empty payload" : parameters.row.preview}
      </Typography>
    ),
  },
];
function EmptyRecords(): React.JSX.Element {
  return (
    <Box role="status" sx={{ display: "grid", height: "100%", placeContent: "center", p: 2 }}>
      <Typography color="text.secondary" variant="body2">
        No records in the live window.
      </Typography>
    </Box>
  );
}
/** NATS evidence owns its columns; the shared product geometry remains consistent. */
export function RecordDataGrid({
  records,
  selectedId,
  onSelect,
}: {
  readonly records: readonly NatsRecord[];
  readonly selectedId: string | null;
  readonly onSelect: (id: string) => void;
}): React.JSX.Element {
  const wide = useMediaQuery("(min-width: 1200px)");
  return (
    <DataGrid<NatsRecord>
      aria-label="NATS records"
      columnHeaderHeight={streamSkopeGeometry.tableHeaderHeight}
      columnVisibilityModel={{ receivedAt: wide }}
      columns={columns}
      density="compact"
      disableColumnMenu
      disableMultipleRowSelection
      disableRowSelectionExcludeModel
      hideFooter
      onCellKeyDown={(parameters: GridCellParams<NatsRecord>, event): void => {
        if (event.key === "Enter") {
          event.preventDefault();
          onSelect(parameters.row.id);
        }
      }}
      onRowClick={(parameters: GridRowParams<NatsRecord>): void => onSelect(parameters.row.id)}
      onRowSelectionModelChange={(model): void => {
        const id = model.ids.values().next().value;
        if (typeof id === "string") onSelect(id);
      }}
      rowHeight={streamSkopeGeometry.tableRowHeight}
      rowSelectionModel={{ type: "include", ids: new Set(selectedId === null ? [] : [selectedId]) }}
      rows={records}
      slots={{ noRowsOverlay: EmptyRecords }}
      sx={{
        border: 0,
        height: "100%",
        minHeight: 0,
        minWidth: 0,
        "& .MuiDataGrid-columnHeaders": {
          borderBottom: "1px solid var(--streamskope-divider-strong)",
        },
      }}
    />
  );
}
