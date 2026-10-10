import { useState } from "react";
import {
  Box,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TablePagination,
  TableRow,
  Typography,
} from "@mui/material";

import type { ObservationRollup } from "../contracts/observations";

function range(minimum: number | null, maximum: number | null): string {
  return minimum === null || maximum === null
    ? "Unknown"
    : minimum === maximum
      ? minimum.toLocaleString()
      : `${minimum.toLocaleString()}–${maximum.toLocaleString()}`;
}

/** Paginated direct-measurement summaries; never used as raw samples by forecasting. */
export function ObservationRollupTable({
  rollups,
}: {
  readonly rollups: readonly ObservationRollup[];
}): React.JSX.Element | null {
  const [requestedPage, setPage] = useState(0);
  if (!rollups.length) return null;
  const rows = [...rollups].sort((a, b) => b.lastObservedAt - a.lastObservedAt);
  const page = Math.min(requestedPage, Math.ceil(rows.length / 12) - 1);
  return (
    <Stack component="section" aria-label="Retained observation summaries" spacing={1}>
      <Typography variant="body2">
        Kafka API measurements grouped into five-minute windows. These are sampled ranges, not
        continuous coverage or rates. Gaps, restarts, offset resets, topology changes and incomplete
        reads start a separate row. Unknown lag is excluded from its range. Forecasts use raw
        observations only.
      </Typography>
      <Box sx={{ overflowX: "auto", minWidth: 0 }}>
        <Table size="small" aria-label="Five-minute observation summaries">
          <TableHead>
            <TableRow>
              <TableCell>Window start (UTC)</TableCell>
              <TableCell>Actual samples</TableCell>
              <TableCell>Known lag</TableCell>
              <TableCell>Lag range, positions</TableCell>
              <TableCell>Collection range, ms</TableCell>
              <TableCell>Partial samples</TableCell>
              <TableCell>Summary boundary</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.slice(page * 12, (page + 1) * 12).map((row) => (
              <TableRow key={row.firstSampleId}>
                <TableCell sx={{ whiteSpace: "nowrap" }}>
                  {new Date(row.bucketStart).toISOString()}
                </TableCell>
                <TableCell>
                  {row.samples.toLocaleString()}
                  <Typography
                    component="span"
                    variant="caption"
                    sx={{ display: "block", whiteSpace: "nowrap" }}
                  >
                    {new Date(row.firstObservedAt).toISOString()} to{" "}
                    {new Date(row.lastObservedAt).toISOString()}
                  </Typography>
                </TableCell>
                <TableCell>
                  {row.lagKnown.toLocaleString()} / {row.samples.toLocaleString()}
                </TableCell>
                <TableCell>{range(row.lagMin, row.lagMax)}</TableCell>
                <TableCell>{range(row.requestMin, row.requestMax)}</TableCell>
                <TableCell>{row.partial.toLocaleString()}</TableCell>
                <TableCell>{row.boundary.replaceAll("-", " ")}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Box>
      <TablePagination
        component="div"
        count={rows.length}
        page={page}
        rowsPerPage={12}
        rowsPerPageOptions={[12]}
        onPageChange={(_event, value) => setPage(value)}
      />
      <Typography variant="caption">
        Each row has comparable sampled measurements within one window. Multiple rows can share a
        window; intervals without reads have no measured values.
      </Typography>
    </Stack>
  );
}
