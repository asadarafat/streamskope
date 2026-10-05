import Typography from "@mui/material/Typography";

import type {
  BackendAvailability,
  ConnectionState,
  KafkaOperationalPreferenceSnapshot,
} from "../contracts";
import { StudioButton as Button } from "../../../platform/ui/controls";

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
  readonly protection?: KafkaOperationalPreferenceSnapshot | null;
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
  protection,
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
    <>
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
      {protection?.store.state === "unavailable" ? (
        <StatusIndicator
          ariaLabel="Record protection"
          label="Protection unavailable"
          tone="error"
        />
      ) : protection?.preferences.protection.readOnly ? (
        <StatusIndicator ariaLabel="Record protection" label="Read-only" tone="warning" />
      ) : null}
      {protection !== null &&
      protection !== undefined &&
      (protection.preferences.protection.maskKey ||
        protection.preferences.protection.maskHeaders.length > 0 ||
        protection.preferences.protection.valuePaths.length > 0) ? (
        <StatusIndicator ariaLabel="Message masking" label="Masking active" tone="warning" />
      ) : null}
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
    </>
  );
}
