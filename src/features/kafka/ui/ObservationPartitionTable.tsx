import { useMemo, useState } from "react";
import {
  Box,
  Stack,
  Typography,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
} from "@mui/material";

import {
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
  StudioCheckbox as Checkbox,
  StudioLabeledControl as FormControlLabel,
} from "../../../platform/ui/controls";
import type { KafkaObservation } from "../contracts/observations";

import { observationPartitionIssues } from "./observation-presentation";

export function ObservationPartitionTable({
  sample,
}: {
  readonly sample: KafkaObservation;
}): React.JSX.Element {
  const [onlyProblems, setOnlyProblems] = useState(false);
  const [sort, setSort] = useState("attention");
  const [filter, setFilter] = useState("");
  const groupSelected = sample.groupCoverage !== "not-selected";
  const rows = useMemo(
    () =>
      sample.partitions
        .map((partition) => ({
          partition,
          issues: observationPartitionIssues(partition, groupSelected),
        }))
        .filter(
          ({ partition, issues }) =>
            (!onlyProblems || issues.length > 0) &&
            (!filter.trim() ||
              `${partition.partition} ${partition.leader ?? "unknown"} ${issues.join(" ")}`
                .toLowerCase()
                .includes(filter.trim().toLowerCase())),
        )
        .sort((a, b) => {
          if (sort === "lag") {
            if (a.partition.lag === null || b.partition.lag === null)
              return a.partition.lag === b.partition.lag
                ? a.partition.partition - b.partition.partition
                : a.partition.lag === null
                  ? 1
                  : -1;
            const difference = BigInt(b.partition.lag) - BigInt(a.partition.lag);
            if (difference !== 0n) return difference > 0n ? 1 : -1;
          }
          if (sort === "attention") {
            const severity = (p: typeof a): number =>
              (p.partition.leader === null ? 4 : 0) +
              (p.partition.inSyncReplicas < p.partition.replicas ? 3 : 0) +
              (p.partition.endOffset === null || (groupSelected && p.partition.lag === null)
                ? 2
                : 0) +
              (p.partition.lag !== null && BigInt(p.partition.lag) > 0n ? 1 : 0);
            const difference = severity(b) - severity(a);
            if (difference !== 0) return difference;
          }
          return a.partition.partition - b.partition.partition;
        }),
    [sample.partitions, groupSelected, onlyProblems, sort, filter],
  );
  return (
    <Stack component="section" aria-label="Partition evidence" spacing={1}>
      <Typography component="h2" variant="h6">
        Partitions
      </Typography>
      <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
        <TextField
          label="Filter partitions"
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          helperText="Partition, leader or finding"
        />
        <TextField
          select
          label="Sort partitions"
          value={sort}
          onChange={(event) => setSort(event.target.value)}
        >
          <MenuItem value="attention">Attention first</MenuItem>
          <MenuItem value="lag">Largest lag first</MenuItem>
          <MenuItem value="partition">Partition number</MenuItem>
        </TextField>
      </Stack>
      <FormControlLabel
        control={
          <Checkbox
            checked={onlyProblems}
            onChange={(event) => setOnlyProblems(event.target.checked)}
          />
        }
        label="Only partitions with gaps or lag"
      />
      <Typography variant="caption">
        Showing {rows.length} of {sample.partitions.length}. Positive lag identifies offset backlog;
        it does not establish a consumer failure.
      </Typography>
      <Box sx={{ overflowX: "auto" }}>
        <Table size="small" aria-label="Observed partition positions">
          <TableHead>
            <TableRow>
              {[
                "Partition",
                "Finding",
                "Leader",
                "ISR / replicas",
                "End position",
                "Committed",
                "Lag",
              ].map((label) => (
                <TableCell key={label}>{label}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.map(({ partition: p, issues }) => (
              <TableRow key={p.partition}>
                <TableCell>{p.partition}</TableCell>
                <TableCell>
                  {issues.length ? issues.join(" · ") : "No issue in available metadata"}
                </TableCell>
                <TableCell>{p.leader ?? "Unknown"}</TableCell>
                <TableCell>
                  {p.inSyncReplicas} / {p.replicas}
                </TableCell>
                <TableCell>{p.endOffset ?? "Unknown"}</TableCell>
                <TableCell>
                  {p.committedOffset ?? (groupSelected ? "Unknown" : "Not selected")}
                </TableCell>
                <TableCell>{p.lag ?? (groupSelected ? "Unknown" : "Not selected")}</TableCell>
              </TableRow>
            ))}
            {rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={7}>No partitions match these filters.</TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </Box>
    </Stack>
  );
}
