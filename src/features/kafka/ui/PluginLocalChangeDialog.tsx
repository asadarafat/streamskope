import { Stack, Typography } from "@mui/material";

import {
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
} from "../../../platform/ui/controls";

import type { PluginLocalConfirmation } from "./usePluginChanges";

export function PluginLocalChangeDialog({
  confirmation,
  pending,
  onCancel,
  onConfirm,
}: {
  readonly confirmation: PluginLocalConfirmation | undefined;
  readonly pending: boolean;
  readonly onCancel: () => void;
  readonly onConfirm: () => Promise<void>;
}): React.JSX.Element {
  return (
    <Dialog
      open={confirmation !== undefined}
      onClose={pending ? undefined : onCancel}
      aria-labelledby="change-plugin-title"
    >
      <DialogTitle id="change-plugin-title">
        {confirmation?.prompt?.title ?? `Remove ${confirmation?.plugin.name ?? "plugin"}?`}
      </DialogTitle>
      <DialogContent>
        <Stack spacing={1}>
          {confirmation?.prompt == null ? null : (
            <>
              <Typography variant="body2">{confirmation.prompt.message}</Typography>
              <Typography variant="body2">{confirmation.prompt.detail}</Typography>
            </>
          )}
          <Typography variant="body2">
            {confirmation?.command === "plugins.remove"
              ? "Saved connection settings are retained. Its connections will require reinstalling the plugin."
              : "The verified installed package is reloaded locally. Your saved connection settings are retained."}
          </Typography>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button disabled={pending} onClick={onCancel}>
          Cancel
        </Button>
        <Button
          disabled={pending}
          color={confirmation?.command === "plugins.remove" ? "error" : "primary"}
          variant="contained"
          onClick={(): void => {
            void onConfirm();
          }}
        >
          {pending ? "Applying change…" : (confirmation?.prompt?.confirmLabel ?? "Remove plugin")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
