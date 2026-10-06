import { Stack, Typography } from "@mui/material";

import { StudioButton as Button } from "../../../platform/ui/controls";

import type { PluginAcquisitions } from "./usePluginAcquisitions";

export function PluginAcquisitionStatus({
  acquisitions,
  onCancelInspection,
}: {
  readonly acquisitions: PluginAcquisitions;
  readonly onCancelInspection: () => Promise<void>;
}): React.JSX.Element | null {
  if (acquisitions.progress.length === 0) return null;
  return (
    <Stack component="section" aria-label="Plugin acquisition progress" spacing={1}>
      {acquisitions.progress.map((entry) => {
        const label =
          entry.operation === "catalog"
            ? "Update check"
            : entry.operation === "test"
              ? "Connection test"
              : "Package acquisition";
        const phase =
          entry.phase === "catalog"
            ? "Reading release catalog"
            : entry.phase === "download"
              ? "Downloading package"
              : "Verifying package";
        return (
          <Stack key={entry.requestId} direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <Typography role="status" aria-live="polite" variant="body2">
              {label}: {phase} · {entry.state}
              {entry.receivedBytes === undefined
                ? ""
                : ` · ${entry.receivedBytes.toLocaleString()}${entry.totalBytes === undefined ? " bytes" : ` / ${entry.totalBytes.toLocaleString()} bytes`}`}
            </Typography>
            {entry.state !== "running" ? null : (
              <Button
                variant="text"
                onClick={(): void => {
                  void (
                    entry.operation === "inspect"
                      ? onCancelInspection()
                      : acquisitions.cancel(entry.requestId)
                  ).catch((): void => undefined);
                }}
              >
                {entry.operation === "catalog"
                  ? "Cancel update check"
                  : entry.operation === "test"
                    ? "Cancel connection test"
                    : "Cancel package acquisition"}
              </Button>
            )}
          </Stack>
        );
      })}
    </Stack>
  );
}
