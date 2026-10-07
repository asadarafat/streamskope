import { useState } from "react";
import LockOutlinedIcon from "@mui/icons-material/LockOutlined";

import {
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioButton as Button,
  StudioIconButton as IconButton,
  StudioTooltip as Tooltip,
} from "./controls";

/** Browser-only owner control; the host callback confirms cleanup before revoking access. */
export function BrowserVaultControl(): React.JSX.Element | null {
  const lock =
    typeof window === "undefined" ? undefined : window.streamSkopeBrowserRuntime?.lockVault;
  const [pending, setPending] = useState(false);
  const [failed, setFailed] = useState(false);
  if (lock === undefined) return null;
  return (
    <>
      <Tooltip title="Lock vault and disconnect">
        <IconButton
          aria-label="Lock vault and disconnect"
          disabled={pending}
          onClick={() => {
            setPending(true);
            void lock().catch(() => {
              setFailed(true);
              setPending(false);
            });
          }}
        >
          <LockOutlinedIcon />
        </IconButton>
      </Tooltip>
      <Dialog
        open={failed}
        onClose={() => setFailed(false)}
        aria-labelledby="vault-lock-error-title"
      >
        <DialogTitle id="vault-lock-error-title">Vault lock could not be confirmed</DialogTitle>
        <DialogContent>
          Restart StreamSkope before continuing, and check any active remote capture resources. The
          host keeps access closed if cleanup fails.
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setFailed(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
