import { Stack, Typography } from "@mui/material";
import { useEffect, useState } from "react";

import type {
  PluginTransition,
  PluginTransitionOperation,
  PluginTransitionStage,
} from "../../../plugins/contracts";
import { StudioAlert as Alert } from "../../../platform/ui/controls";

const operations: Record<PluginTransitionOperation, string> = {
  startup: "Starting plugin",
  install: "Installing plugin",
  retry: "Retrying plugin activation",
  remove: "Removing plugin",
  "renderer-recovery": "Recovering plugin controls",
  "review-install": "Reviewing plugin installation",
  "review-retry": "Reviewing activation retry",
  "review-remove": "Reviewing plugin removal",
  "review-exit": "Reviewing plugin work before exit",
  "resolve-exit": "Completing plugin exit action",
  shutdown: "Closing plugin",
};
const stages: Record<PluginTransitionStage, string> = {
  queued: "Waiting for another plugin change",
  "verify-package": "Verifying the package",
  "wait-connections": "Waiting for connection attempts to finish",
  "review-change": "Checking current plugin work",
  "load-candidate": "Loading the selected plugin",
  "prepare-unload": "Preparing plugin cleanup",
  "drain-requests": "Waiting for plugin requests to finish",
  "commit-storage": "Updating installation storage",
  "retire-previous": "Closing the previous plugin instance",
  "activate-candidate": "Activating the selected plugin",
  "rollback-storage": "Restoring the previous installation",
  "startup-recovery": "Recovering the installed plugin",
  "review-exit": "Checking plugin work before exit",
  "resolve-exit": "Completing the selected exit action",
  "close-backend": "Closing the plugin instance",
  "close-candidate": "Closing the staged plugin instance",
  "discard-package": "Discarding the staged package",
};

/** Host progress observes real ownership; it does not provide cancellation authority. */
export function PluginTransitionStatus({
  transition,
}: {
  readonly transition: PluginTransition;
}): React.JSX.Element {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return (): void => clearInterval(timer);
  }, [transition.operationId]);
  const elapsed = Math.max(0, Math.floor((now - Date.parse(transition.startedAt)) / 1000));
  const stageElapsed = Math.max(
    0,
    Math.floor((now - Date.parse(transition.stageStartedAt)) / 1000),
  );
  return (
    <Alert
      severity={transition.state === "waiting" ? "warning" : "info"}
      role="group"
      aria-label="Plugin change progress"
    >
      <Stack spacing={0.5}>
        <Typography variant="body2" role="status">
          {operations[transition.operation]} ·{" "}
          {transition.state === "queued"
            ? "Queued"
            : transition.state === "waiting"
              ? "Still waiting"
              : "In progress"}
        </Typography>
        <Typography variant="body2">{stages[transition.stage]}</Typography>
        <Typography variant="body2" color="text.secondary" aria-live="off">
          {elapsed}s elapsed · {stageElapsed}s in this step
        </Typography>
        {transition.outstandingRequests === 0 && transition.outstandingConnections === 0 ? null : (
          <Typography variant="body2">
            Outstanding: {transition.outstandingRequests}{" "}
            {transition.outstandingRequests === 1 ? "request" : "requests"} ·{" "}
            {transition.outstandingConnections}{" "}
            {transition.outstandingConnections === 1 ? "connection" : "connections"}
          </Typography>
        )}
        {transition.commit === "in-progress" ? (
          <Typography variant="body2">
            The saved installation may be changing; storage completion is not yet confirmed.
          </Typography>
        ) : transition.commit === "confirmed" ? (
          <Typography variant="body2">
            The storage change is saved. This operation is still completing.
          </Typography>
        ) : null}
        {transition.state !== "waiting" ? null : (
          <Typography variant="body2">
            This step is taking longer than usual. Further changes to this plugin remain blocked
            until the existing work finishes.
          </Typography>
        )}
      </Stack>
    </Alert>
  );
}
