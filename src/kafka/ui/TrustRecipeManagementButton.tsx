import { lazy, Suspense, useState } from "react";
import { Typography } from "@mui/material";

import type { StreamSkopeHost } from "../contracts";
import { StudioButton } from "../../ui/controls";

import type { TextDocumentTransferPort } from "./text-document-transfer";

const Manager = lazy(async () => {
  const module = await import("./TrustRecipeManager");
  return { default: module.TrustRecipeManager };
});

export function TrustRecipeManagementButton({
  host,
  transfer,
}: {
  readonly host: StreamSkopeHost;
  readonly transfer?: TextDocumentTransferPort | undefined;
}): React.JSX.Element {
  const [open, setOpen] = useState(false);
  return (
    <>
      <StudioButton
        aria-label="Manage retrieval profiles"
        onClick={() => setOpen(true)}
        sx={{ flexShrink: 0 }}
      >
        Manage…
      </StudioButton>
      {open ? (
        <Suspense
          fallback={
            <Typography role="status" variant="body2">
              Loading retrieval profiles…
            </Typography>
          }
        >
          <Manager host={host} transfer={transfer} onClose={() => setOpen(false)} />
        </Suspense>
      ) : null}
    </>
  );
}
