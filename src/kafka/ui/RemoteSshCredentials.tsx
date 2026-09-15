import { useEffect, useRef, useState } from "react";
import { InputAdornment, Stack, Typography } from "@mui/material";

import { REMOTE_TRUST_ACQUISITION_LIMITS } from "../contracts";
import type { RemoteSshAuthentication } from "../contracts/remote-trust-types";
import { StudioAlert, StudioButton, StudioMenuItem, StudioTextField } from "../../ui/controls";

export function RemoteSshCredentials({
  authentication,
  agentStatus = "unknown",
  disabled,
  error,
  onChange,
}: {
  readonly authentication: RemoteSshAuthentication;
  readonly agentStatus?: "checking" | "unknown" | "configured" | "unavailable";
  readonly disabled: boolean;
  readonly error?: string | undefined;
  readonly onChange: (value: RemoteSshAuthentication) => void;
}): React.JSX.Element {
  const [visible, setVisible] = useState(false);
  const [fileError, setFileError] = useState<string>();
  const reader = useRef<FileReader | null>(null);
  useEffect(() => {
    setVisible(false);
    return (): void => {
      const active = reader.current;
      reader.current = null;
      if (active?.readyState === FileReader.LOADING) active.abort();
    };
  }, [authentication.mode]);
  useEffect(() => {
    if (disabled) setVisible(false);
  }, [disabled]);

  function upload(file: File | undefined): void {
    if (file === undefined || authentication.mode !== "private-key") return;
    const previous = reader.current;
    reader.current = null;
    if (previous?.readyState === FileReader.LOADING) previous.abort();
    onChange({ mode: "private-key", privateKey: "" });
    setFileError(undefined);
    if (file.size === 0 || file.size > REMOTE_TRUST_ACQUISITION_LIMITS.privateKeyCharacters) {
      setFileError("Select a non-empty SSH private key no larger than 64 KiB.");
      return;
    }
    const pending = new FileReader();
    reader.current = pending;
    pending.onerror = (): void => {
      if (reader.current !== pending) return;
      reader.current = null;
      setFileError("The selected key could not be read. Choose a readable private key file.");
    };
    pending.onload = (): void => {
      if (reader.current !== pending) return;
      reader.current = null;
      if (typeof pending.result !== "string" || pending.result.includes("\0")) {
        setFileError("The selected file is not a supported textual SSH private key.");
        return;
      }
      onChange({ mode: "private-key", privateKey: pending.result });
    };
    pending.readAsText(file);
  }

  const secretLabel = authentication.mode === "password" ? "SSH password" : "SSH key passphrase";
  return (
    <Stack spacing={2}>
      <StudioTextField
        label="SSH authentication"
        select
        fullWidth
        disabled={disabled}
        value={authentication.mode}
        onChange={(event) => {
          setVisible(false);
          setFileError(undefined);
          if (event.target.value === "agent") onChange({ mode: "agent" });
          else if (event.target.value === "private-key")
            onChange({ mode: "private-key", privateKey: "" });
          else if (event.target.value === "password") onChange({ mode: "password", password: "" });
        }}
      >
        <StudioMenuItem value="password">Password</StudioMenuItem>
        <StudioMenuItem value="private-key">Private key</StudioMenuItem>
        <StudioMenuItem value="agent">Local SSH agent</StudioMenuItem>
      </StudioTextField>
      {authentication.mode === "agent" ? (
        <Typography variant="body2" color="text.secondary">
          {agentStatus === "configured"
            ? "The application host has an SSH agent configured. Key availability and authentication are checked only when acquiring."
            : agentStatus === "unavailable"
              ? "No SSH agent is configured in the application host. Start an agent before launching StreamSkope, or select Password or Private key."
              : agentStatus === "checking"
                ? "Checking the application host's SSH agent configuration…"
                : "SSH agent configuration has not been verified. Reopen the acquisition panel to retry, or select Password or Private key."}{" "}
          Agent forwarding and automatic password fallback are disabled.
        </Typography>
      ) : (
        <>
          {authentication.mode === "private-key" ? (
            <>
              <StudioTextField
                label="SSH private key file"
                type="file"
                fullWidth
                disabled={disabled}
                slotProps={{
                  inputLabel: { shrink: true },
                  htmlInput: { "aria-label": "SSH private key file" },
                }}
                onChange={(event) => {
                  const input = event.target;
                  if (input instanceof HTMLInputElement) {
                    upload(input.files?.[0]);
                    input.value = "";
                  }
                }}
              />
              <Typography role="status" variant="body2">
                {authentication.privateKey
                  ? "Private key loaded for this editor only."
                  : "Select an SSH private key file. It is not saved with the profile."}
              </Typography>
            </>
          ) : null}
          <StudioTextField
            disabled={
              disabled ||
              (authentication.mode === "private-key" && authentication.privateKey.length === 0)
            }
            fullWidth
            label={secretLabel}
            error={error !== undefined}
            helperText={error}
            type={visible ? "text" : "password"}
            value={
              authentication.mode === "password"
                ? authentication.password
                : (authentication.passphrase ?? "")
            }
            onChange={(event) => {
              onChange(
                authentication.mode === "password"
                  ? { mode: "password", password: event.target.value }
                  : {
                      mode: "private-key",
                      privateKey: authentication.privateKey,
                      ...(event.target.value.length === 0
                        ? {}
                        : { passphrase: event.target.value }),
                    },
              );
            }}
            slotProps={{
              htmlInput: { autoComplete: "new-password" },
              input: {
                endAdornment: (
                  <InputAdornment position="end">
                    <StudioButton
                      type="button"
                      variant="text"
                      aria-label={`${visible ? "Hide" : "Show"} ${secretLabel}`}
                      aria-pressed={visible}
                      disabled={disabled}
                      onClick={() => setVisible((value) => !value)}
                    >
                      {visible ? "Hide" : "Show"}
                    </StudioButton>
                  </InputAdornment>
                ),
              },
            }}
          />
        </>
      )}
      {fileError === undefined ? null : <StudioAlert severity="error">{fileError}</StudioAlert>}
    </Stack>
  );
}
