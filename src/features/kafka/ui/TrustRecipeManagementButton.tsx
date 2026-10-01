import { lazy, Suspense, useState } from "react";
import { Typography } from "@mui/material";

import type { StreamSkopeHost } from "../contracts";
import { StudioButton } from "../../../platform/ui/controls";

import type { TextDocumentTransferPort } from "./text-document-transfer";

const Manager = lazy(async () => {
  const module = await import("./TrustRecipeManager");
  return { default: module.TrustRecipeManager };
});

export function TrustRecipeManagementButton({
  disabled = false,
  host,
  transfer,
}: {
  readonly disabled?: boolean;
  readonly host: StreamSkopeHost;
  readonly transfer?: TextDocumentTransferPort | undefined;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  return (
    <>
      <StudioButton
        aria-label="Manage retrieval presets"
        disabled={disabled}
        onClick={() => setOpen(true)}
        sx={{ flexShrink: 0 }}
      >
        Manage…
      </StudioButton>
      {open ? (
        <Suspense
          fallback={
            <Typography role="status" variant="body2">
              Loading retrieval presets…
            </Typography>
          }
        >
          <Manager host={host} transfer={transfer} onClose={() => setOpen(false)} />
        </Suspense>
      ) : null}
    </>
  );
}
