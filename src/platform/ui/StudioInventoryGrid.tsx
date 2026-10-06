import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { DataGrid, type GridColDef, type GridValidRowModel } from "@mui/x-data-grid";
import type { ReactNode } from "react";

import { StudioButton } from "./controls";
import { streamSkopeGeometry } from "./studioTokens";

function InventoryStateOverlay({
  children,
  label,
}: {
  readonly children: ReactNode;
  readonly label: string;
}): React.JSX.Element | null {
  if (children === null) {
    return null;
  }
  return (
    <Box
      aria-label={label}
      aria-live="polite"
      role="status"
      sx={{
        position: "absolute",
        top: streamSkopeGeometry.tableHeaderHeight,
        bottom: 0,
        left: 0,
        right: 0,
        alignItems: "center",
        display: "flex",
        justifyContent: "center",
        p: 3,
        bgcolor: "background.paper",
      }}
    >
      <Typography color="text.secondary" variant="body2">
        {children}
      </Typography>
    </Box>
  );
}

export function StudioInventoryCellAction({
  accessibleName,
  children,
  onActivate,
}: {
  readonly accessibleName: string;
  readonly children: ReactNode;
  readonly onActivate: () => void;
}): React.JSX.Element {
  return (
    <StudioButton
      aria-label={accessibleName}
      fullWidth
      onClick={onActivate}
      sx={{
        alignItems: "center",
        height: "100%",
        justifyContent: "flex-start",
        minHeight: 0,
        px: 1,
        py: 0,
      }}
      variant="text"
    >
      {children}
    </StudioButton>
  );
}

export function StudioInventoryGrid<Row extends GridValidRowModel>({
  ariaLabel,
  columns,
  emptyMessage,
  loading,
  loadingMessage,
  rows,
  stateLabel,
  testId,
}: {
  readonly ariaLabel: string;
  readonly columns: readonly GridColDef<Row>[];
  readonly emptyMessage: ReactNode;
  readonly loading: boolean;
  readonly loadingMessage: ReactNode;
  readonly rows: readonly Row[];
  readonly stateLabel: string;
  readonly testId: string;
}): React.JSX.Element {
  return (
    <Box
      data-testid={testId}
      sx={{
        bgcolor: "background.paper",
        border: 1,
        borderColor: "divider",
        flex: "1 1 0",
        minHeight: 0,
        position: "relative",
      }}
    >
      <DataGrid
        aria-label={ariaLabel}
        columnHeaderHeight={streamSkopeGeometry.tableHeaderHeight}
        columns={columns}
        density="compact"
        disableColumnMenu
        disableMultipleRowSelection
        disableRowSelectionOnClick
        hideFooter
        loading={loading}
        rowHeight={streamSkopeGeometry.tableRowHeight}
        rows={rows}
        slots={{
          loadingOverlay: () => null,
          noRowsOverlay: () => null,
        }}
        sx={{
          border: 0,
          height: "100%",
          minHeight: 0,
          "& .MuiDataGrid-columnHeaders": {
            borderBottom: "1px solid var(--streamskope-divider-strong)",
          },
        }}
      />
      {/* Live inventory state belongs beside the grid, whose children must be rows. */}
      <InventoryStateOverlay label={stateLabel}>
        {loading ? loadingMessage : rows.length === 0 ? emptyMessage : null}
      </InventoryStateOverlay>
    </Box>
  );
}
