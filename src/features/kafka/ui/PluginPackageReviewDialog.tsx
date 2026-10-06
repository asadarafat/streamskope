import { Box, Stack, Typography } from "@mui/material";

import type { PluginChangePrompt, PluginManifest } from "../../../plugins/contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
} from "../../../platform/ui/controls";
import type { PluginPackageReview } from "./usePluginChanges";

export function PluginCompatibility({
  manifest,
  update = false,
}: {
  readonly manifest: PluginManifest;
  readonly update?: boolean;
}): React.JSX.Element | null {
  const bounds = manifest.compatibility;
  if (bounds === undefined) return null;
  return (
    <Typography variant="body2">
      {update ? "Available update: " : ""}Requires StreamSkope {bounds.streamskope.minimum}
      {bounds.streamskope.maximumExclusive === undefined
        ? " or later"
        : ` up to, but excluding, ${bounds.streamskope.maximumExclusive}`}
      {" · "}Supports {bounds.target.system.toUpperCase()} {bounds.target.minimum}–
      {bounds.target.maximum}
      {" (inclusive) · Plugin API "}
      {manifest.apiVersion}
    </Typography>
  );
}

export function PluginPackageReviewDialog({
  review,
  prompt,
  pending,
  onApply,
  onClose,
}: {
  readonly review: PluginPackageReview | undefined;
  readonly prompt: PluginChangePrompt | undefined;
  readonly pending: boolean;
  readonly onApply: () => Promise<void>;
  readonly onClose: () => Promise<void>;
}): React.JSX.Element {
  const actionable =
    review !== undefined && (review.status === "install" || review.status === "update");
  return (
    <Dialog
      open={review !== undefined}
      onClose={
        pending
          ? undefined
          : (): void => {
              void onClose();
            }
      }
      aria-labelledby="review-plugin-title"
    >
      <DialogTitle id="review-plugin-title">{prompt?.title ?? "Review plugin"}</DialogTitle>
      <DialogContent>
        {review === undefined ? null : (
          <Stack spacing={2}>
            <Typography component="h4" variant="subtitle1">
              {review.manifest.name} {review.manifest.version}
            </Typography>
            <Box
              component="dl"
              sx={{
                display: "grid",
                gridTemplateColumns: "max-content minmax(0, 1fr)",
                gap: 1,
                m: 0,
                "& dt": { color: "text.secondary" },
                "& dd": { m: 0, overflowWrap: "anywhere" },
              }}
            >
              <Typography component="dt" variant="body2">
                Source
              </Typography>
              <Typography component="dd" variant="body2">
                {review.source === "file"
                  ? "Signed local file"
                  : review.source === "cache"
                    ? "Verified local cache"
                    : review.trust === "development"
                      ? "Local development package"
                      : "Official release download"}
              </Typography>
              <Typography component="dt" variant="body2">
                Publisher
              </Typography>
              <Typography component="dd" variant="body2">
                {review.publisher === undefined
                  ? review.trust === "development"
                    ? "Development source"
                    : "Official StreamSkope release"
                  : `${review.publisher.name} (${review.publisher.keyId})`}
              </Typography>
              <Typography component="dt" variant="body2">
                Installed version
              </Typography>
              <Typography component="dd" variant="body2">
                {review.installedVersion ?? "Not installed"}
              </Typography>
              <Typography component="dt" variant="body2">
                Selected version
              </Typography>
              <Typography component="dd" variant="body2">
                {review.manifest.version}
              </Typography>
              <Typography component="dt" variant="body2">
                SHA256
              </Typography>
              <Typography component="dd" variant="body2" sx={{ fontFamily: "monospace" }}>
                {review.sha256}
              </Typography>
            </Box>
            <PluginCompatibility manifest={review.manifest} />
            {review.status === "blocked" ? (
              <Alert severity="error">{review.reason ?? "This package cannot be installed."}</Alert>
            ) : null}
            {review.status === "already-installed" ? (
              <Alert severity="info">
                This plugin version and content are already installed. No changes are needed.
              </Alert>
            ) : null}
            {prompt === undefined ? null : (
              <Alert severity="warning">
                <Typography variant="body2">{prompt.message}</Typography>
                <Typography variant="body2">{prompt.detail}</Typography>
              </Alert>
            )}
            {actionable ? (
              <Typography variant="body2">
                Saved connection settings are retained. The verified package becomes available
                without restarting StreamSkope.
              </Typography>
            ) : null}
          </Stack>
        )}
      </DialogContent>
      <DialogActions>
        <Button
          disabled={pending}
          onClick={(): void => {
            void onClose();
          }}
        >
          {actionable ? "Cancel" : "Close"}
        </Button>
        {!actionable ? null : (
          <Button
            disabled={pending}
            variant="contained"
            onClick={(): void => {
              void onApply();
            }}
          >
            {pending
              ? "Applying change…"
              : (prompt?.confirmLabel ??
                (review?.status === "update" ? "Update plugin" : "Install plugin"))}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
}
