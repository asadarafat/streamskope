import { Box, Typography } from "@mui/material";

import type { KafkaReadCoverage, KafkaReadReason } from "../contracts";

const reasons: Record<KafkaReadReason, string> = {
  reading: "Read in progress",
  "range-complete": "Requested offset ranges reached",
  "result-limit": "Partial read: result limit reached",
  "scan-limit": "Partial read: scan limit reached",
  "byte-limit": "Partial read: byte limit reached",
  "fetch-limit": "Partial read: fetch budget exhausted",
  deadline: "Partial read: time budget exhausted",
  cancelled: "Partial read: cancelled",
  failed: "Partial read: failed",
};

export function QueryReadCoverage({
  coverage,
  search,
}: {
  readonly coverage: KafkaReadCoverage | null;
  readonly search: boolean;
}): React.JSX.Element {
  return (
    <Box
      aria-label="Read coverage"
      component="section"
      sx={{ px: 2, py: 0.5, borderBottom: 1, borderColor: "divider" }}
    >
      <Typography variant="caption" component="p" role="status">
        {coverage === null
          ? "Coverage not reported. Loaded messages are a sample."
          : `${reasons[coverage.reason]}. ${coverage.scannedRecords.toLocaleString()} ${coverage.scannedRecords === 1 ? "record" : "records"} scanned; ${coverage.matchedRecords.toLocaleString()} ${search ? (coverage.matchedRecords === 1 ? "match" : "matches") : coverage.matchedRecords === 1 ? "record" : "records"} returned.`}
        {coverage !== null && coverage.unavailableRecords > 0
          ? ` ${coverage.unavailableRecords.toLocaleString()} records could not be evaluated; search results are partial.`
          : ""}
      </Typography>
      {coverage === null ? null : (
        <Box component="details">
          <Typography component="summary" variant="caption" sx={{ cursor: "pointer" }}>
            Partition coverage
          </Typography>
          <Typography variant="caption" component="p">
            Offsets are start inclusive / end exclusive. Only currently retained records are
            readable; timestamp lookup does not reconstruct past retention or guarantee timestamp
            order.
          </Typography>
          <Box component="ul" sx={{ maxHeight: 140, overflow: "auto", my: 0 }}>
            {coverage.partitions.map((item) => (
              <Typography component="li" variant="caption" key={item.partition}>
                {`Partition ${String(item.partition)}: requested [${item.startOffset}, ${item.endOffset}); reached ${item.nextOffset}`}
              </Typography>
            ))}
          </Box>
        </Box>
      )}
    </Box>
  );
}
