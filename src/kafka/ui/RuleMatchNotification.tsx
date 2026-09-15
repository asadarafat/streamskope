import AlertTitle from "@mui/material/AlertTitle";
import Snackbar from "@mui/material/Snackbar";
import Typography from "@mui/material/Typography";

import { StudioAlert as Alert, StudioButton as Button } from "../../ui/controls";

import type { KafkaRuleNotificationUiEntry } from "./state";

export function RuleMatchNotification({
  notification,
  onDismiss,
}: {
  readonly notification: KafkaRuleNotificationUiEntry | undefined;
  readonly onDismiss: (sequence: number) => void;
}): React.JSX.Element | null {
  if (notification === undefined) return null;

  return (
    <Snackbar
      anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
      key={notification.sequence}
      open
      sx={{ zIndex: (theme) => theme.zIndex.modal - 1 }}
    >
      <Alert
        action={
          <Button
            aria-label="Dismiss rule notification"
            color="inherit"
            onClick={() => onDismiss(notification.sequence)}
            size="small"
            sx={{ whiteSpace: "nowrap" }}
            variant="text"
          >
            Dismiss
          </Button>
        }
        aria-label="Rule match notification"
        severity={
          notification.highestSeverity === "warn" ? "warning" : notification.highestSeverity
        }
        sx={{ alignItems: "flex-start", width: "min(560px, calc(100vw - 32px))" }}
      >
        <AlertTitle>
          {notification.activeMatchCount} active rule match
          {notification.activeMatchCount === 1 ? "" : "es"} on {notification.topic}
        </AlertTitle>
        {notification.matches.length === 0 ? null : (
          <Typography component="p" variant="body2">
            {notification.matches
              .map((match) => `${match.name} · ${match.level} · ${String(match.count)}`)
              .join("; ")}
          </Typography>
        )}
        {notification.omittedMatches === 0 ? null : (
          <Typography component="p" variant="body2">
            {notification.omittedMatches} additional match
            {notification.omittedMatches === 1 ? "" : "es"} not shown in this notification.
          </Typography>
        )}
      </Alert>
    </Snackbar>
  );
}
