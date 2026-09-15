import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

import type { BackendAvailability, ConnectionState } from "../contracts";
import { streamSkopeGeometry, streamSkopeSpacing } from "../../ui/createStreamSkopeTheme";
import { StudioButton as Button } from "../../ui/controls";

import { connectionStateLabel } from "./state";
import { StatusIndicator, type StatusIndicatorTone } from "./StatusIndicator";
import { connectionColor } from "./workbench-status";

export interface WorkbenchStatusBarProperties {
  readonly activeConnectionName: string | null;
  readonly backend: "checking" | BackendAvailability;
  readonly connectionState: ConnectionState;
  readonly droppedMessages: number;
  readonly messageRequestError: string | undefined;
  readonly operationStatus: string;
  readonly onReload: () => void;
  readonly resourceStatus: string;
}

export function WorkbenchStatusBar({
  activeConnectionName,
  backend,
  connectionState,
  droppedMessages,
  messageRequestError,
  operationStatus,
  onReload,
  resourceStatus,
}: WorkbenchStatusBarProperties): React.JSX.Element {
  const backendUnavailable = backend === "unavailable";
  const backendLabel =
    backend === "ready" ? "Host ready" : backendUnavailable ? "Host unavailable" : "Checking host";
  const connectionLabel = backendUnavailable
    ? `Last confirmed ${connectionStateLabel(connectionState).toLowerCase()}`
    : connectionStateLabel(connectionState);
  const connectionSemanticColor = backendUnavailable ? "warning" : connectionColor(connectionState);
  const connectionTone: StatusIndicatorTone =
    connectionSemanticColor === "default" ? "neutral" : connectionSemanticColor;

  return (
    <Box
      component="footer"
      sx={{
        alignItems: "center",
        bgcolor: "background.paper",
        borderTop: 1,
        borderColor: "divider",
        display: "flex",
        gap: `${String(streamSkopeSpacing.scale.space12)}px`,
        height: streamSkopeGeometry.statusBarHeight,
        minWidth: 0,
        overflow: "hidden",
        px: `${String(streamSkopeSpacing.scale.space12)}px`,
      }}
    >
      <StatusIndicator
        ariaLabel="Backend status"
        label={backendLabel}
        tone={backend === "ready" ? "success" : backendUnavailable ? "error" : "warning"}
      />
      <StatusIndicator
        ariaLabel="Connection status"
        label={`${connectionLabel}${activeConnectionName === null ? "" : ` · ${activeConnectionName}`}`}
        live="polite"
        tone={connectionTone}
      />
      <Typography color="text.secondary" noWrap variant="caption">
        {backendUnavailable ? "Data is stale" : resourceStatus}
      </Typography>
      <Typography
        color={messageRequestError === undefined ? "text.secondary" : "error.main"}
        noWrap
        role={messageRequestError === undefined ? undefined : "alert"}
        sx={{ flex: 1, minWidth: 0 }}
        variant="caption"
      >
        {messageRequestError ?? operationStatus}
        {droppedMessages > 0 ? ` · ${droppedMessages.toLocaleString()} dropped or omitted` : ""}
      </Typography>
      {backendUnavailable ? (
        <Button onClick={onReload} sx={{ minHeight: 20, py: 0 }} variant="text">
          Reload workbench
        </Button>
      ) : null}
    </Box>
  );
}
