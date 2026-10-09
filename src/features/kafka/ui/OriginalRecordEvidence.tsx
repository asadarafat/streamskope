import { useState } from "react";
import { Stack, Typography } from "@mui/material";

import type { KafkaOriginalRecord } from "../contracts";
import { StudioAlert, StudioButton } from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";

import type { TextDocumentTransferPort } from "./text-document-transfer";

export function OriginalRecordEvidence({
  original,
  transfer,
}: {
  readonly original: KafkaOriginalRecord | undefined;
  readonly transfer: TextDocumentTransferPort;
}): React.JSX.Element {
  const [status, setStatus] = useState("");
  const [failed, setFailed] = useState(false);
  const [copying, setCopying] = useState(false);
  if (original?.state !== "complete") {
    return (
      <StudioAlert severity="info">
        {original?.state === "unavailable" && original.reason === "masked"
          ? "Original bytes are withheld by the message disclosure policy."
          : original?.state === "unavailable" && original.reason === "size-limit"
            ? "Original bytes exceed the retention limit. Display text and previews cannot be used as byte-exact records."
            : "Original bytes were not captured. Display text cannot be used as a byte-exact record."}
      </StudioAlert>
    );
  }
  const content = JSON.stringify(original, null, 2);
  const copy = async (): Promise<void> => {
    setCopying(true);
    setFailed(false);
    setStatus("");
    try {
      await transfer.copy(content);
      setStatus("Original record copied as Base64 JSON.");
    } catch {
      setFailed(true);
    } finally {
      setCopying(false);
    }
  };
  return (
    <Stack spacing={1} sx={{ p: 1.5, minWidth: 0 }}>
      <Typography variant="body2">
        Base64 preserves the exact key, value and ordered headers. Null and empty bytes are
        distinct. Protected views apply the saved encodings separately; they never rewrite these
        bytes.
      </Typography>
      <StudioButton
        disabled={copying}
        onClick={() => {
          void copy();
        }}
        variant="outlined"
      >
        Copy original record
      </StudioButton>
      {failed ? (
        <StudioAlert severity="error">
          The original record could not be copied. Check clipboard permission and retry.
        </StudioAlert>
      ) : null}
      {status ? (
        <Typography role="status" variant="body2">
          {status}
        </Typography>
      ) : null}
      <StudioCodeBlock sx={{ overflow: "auto", whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
        {content}
      </StudioCodeBlock>
    </Stack>
  );
}
