import { useMemo } from "react";
import { Box, Table, TableBody, TableCell, TableHead, TableRow, Typography } from "@mui/material";

import { compareDocuments } from "../contracts/document-diff";
import { StudioAlert as Alert } from "../../../platform/ui/controls";

export function DocumentDiff({
  before,
  after,
  mode,
}: {
  readonly before: string;
  readonly after: string;
  readonly mode: "json" | "text";
}): React.JSX.Element {
  const result = useMemo(() => compareDocuments(before, after, mode), [before, after, mode]);
  return (
    <Box>
      {result.error ? <Alert severity="warning">{result.error}</Alert> : null}
      {result.limited ? (
        <Alert severity="warning">
          Comparison stopped at its size, depth, work or 500-change limit. This is a partial
          comparison.
        </Alert>
      ) : null}
      {!result.error && !result.limited && result.rows.length === 0 ? (
        <Typography role="status">No differences in the compared representation.</Typography>
      ) : null}
      {result.rows.length > 0 ? (
        <Box sx={{ overflowX: "auto" }}>
          <Table size="small" aria-label="Differences">
            <TableHead>
              <TableRow>
                <TableCell>Path / line</TableCell>
                <TableCell>Change</TableCell>
                <TableCell>Before</TableCell>
                <TableCell>After</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {result.rows.map((row) => (
                <TableRow key={row.path}>
                  <TableCell sx={{ overflowWrap: "anywhere" }}>{row.path}</TableCell>
                  <TableCell>{row.kind}</TableCell>
                  <TableCell sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
                    {row.before}
                  </TableCell>
                  <TableCell sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
                    {row.after}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Box>
      ) : null}
    </Box>
  );
}
