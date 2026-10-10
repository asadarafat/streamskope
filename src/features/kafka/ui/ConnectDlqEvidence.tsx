import { Box, Stack, Typography } from "@mui/material";

import { StudioAlert as Alert } from "../../../platform/ui/controls";
import type { KafkaMessage } from "../contracts";
import { inspectConnectDlqEvidence } from "../contracts/connect-dlq";
import { StudioDetailRow } from "../../../platform/ui/StudioPropertyRow";
export function ConnectDlqEvidence({
  message,
}: {
  readonly message: KafkaMessage;
}): React.JSX.Element | null {
  const evidence = inspectConnectDlqEvidence(message);
  if (evidence.state === "absent") return null;
  if (evidence.state === "unavailable")
    return (
      <Alert severity="warning">
        Connect context is {evidence.reason}. A reliable reported source locator could not be
        established from the complete protected ordered headers. Inspect Headers; no source
        destination or position is inferred from this record.
      </Alert>
    );
  const context = evidence.context;
  return (
    <Alert severity="info">
      <Stack
        component="section"
        aria-label="Reported Connect context"
        spacing={1}
        sx={{ minWidth: 0, overflowWrap: "anywhere" }}
      >
        <Typography component="h3" variant="subtitle2">
          Reported Connect context
        </Typography>
        <Typography variant="body2">
          Record headers report this context. It is not a verified broker/topic identity or proof of
          the current task's condition.
        </Typography>
        <Box component="dl" sx={{ m: 0, minWidth: 0 }}>
          <StudioDetailRow label="Source topic" value={context.topic} />
          <StudioDetailRow
            label="Source partition / offset"
            value={`${context.partition} / ${context.offset}`}
          />
          <StudioDetailRow
            label="Connector / task"
            value={`${context.connector} / ${context.task}`}
          />
          <StudioDetailRow label="Reported stage" value={context.stage} />
        </Box>
        <Typography variant="body2">
          Use Replay to select an explicit destination and preview exact bytes or structured writer
          mapping. Repair history retains per-attempt outcomes and supports reconciliation after
          restart. Reprocessing can fail again or create a loop; the original DLQ and source offsets
          remain unchanged.
        </Typography>
      </Stack>
    </Alert>
  );
}
