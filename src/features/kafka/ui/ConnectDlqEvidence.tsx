import { Typography } from "@mui/material";

import { StudioAlert as Alert } from "../../../platform/ui/controls";
import type { KafkaMessage } from "../contracts";
import { connectDlqContext } from "../contracts/connect-dlq";
export function ConnectDlqEvidence({
  message,
}: {
  readonly message: KafkaMessage;
}): React.JSX.Element | null {
  const context = connectDlqContext(message);
  if (!context) return null;
  return (
    <Alert severity="info">
      <Typography variant="subtitle2">Connect dead-letter context</Typography>
      <Typography variant="body2">
        Reported source: {context.topic}/{context.partition}@{context.offset}. Connector{" "}
        {context.connector}, task {context.task}, stage {context.stage}. These record headers are
        unverified metadata. Use Replay to preview an explicit destination and exact bytes; the
        original key, value and ordered headers are retained. Reprocessing can fail again or create
        a loop. No source offsets are reset or skipped.
      </Typography>
    </Alert>
  );
}
