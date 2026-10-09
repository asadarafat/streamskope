import { Box, Stack, Typography } from "@mui/material";

import type { KafkaReadCoverage, KafkaReadReason } from "../contracts";
import type { KafkaSearchProgress } from "../contracts/query-search";
import { StudioButton as Button } from "../../../platform/ui/controls";

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
  progress = null,
  continuationAvailable = false,
  continuationBusy = false,
  continuationNotice,
  onContinue,
}: {
  readonly coverage: KafkaReadCoverage | null;
  readonly search: boolean;
  readonly progress?: KafkaSearchProgress | null;
  readonly continuationAvailable?: boolean;
  readonly continuationBusy?: boolean;
  readonly continuationNotice?: string | undefined;
  readonly onContinue?: (() => void) | undefined;
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
      {progress === null ? null : (
        <Stack direction="row" useFlexGap sx={{ alignItems: "center", flexWrap: "wrap", gap: 1 }}>
          <Typography aria-label="Cumulative read progress" component="p" variant="caption">
            Pass {progress.pass.toLocaleString()} · Total:{" "}
            {progress.scannedRecords.toLocaleString()} records scanned;{" "}
            {progress.matchedRecords.toLocaleString()} {search ? "matches" : "records"} returned.
            {progress.unavailableRecords > 0
              ? ` ${progress.unavailableRecords.toLocaleString()} records could not be evaluated.`
              : ""}
          </Typography>
          {progress.continuation === null && !continuationBusy ? null : (
            <Button
              size="small"
              variant="outlined"
              onClick={onContinue}
              disabled={!continuationAvailable || continuationBusy || onContinue === undefined}
            >
              {continuationBusy ? "Continuing…" : search ? "Continue search" : "Continue read"}
            </Button>
          )}
          {continuationNotice ? (
            <Typography component="p" variant="caption">
              {continuationNotice}
            </Typography>
          ) : progress.continuation === null ? null : (
            <Typography component="p" variant="caption">
              Continue reads the remaining offset ranges and replaces this result page. Export this
              page first to keep its records.
            </Typography>
          )}
        </Stack>
      )}
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
