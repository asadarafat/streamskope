import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import type { ProfileTrustKind } from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { readTrustFile } from "./profile-dialog-model";
import type { ProtectedFieldForm } from "./profile-security-form";

export function ProfileProtectedTextField({
  disabled,
  label,
  value,
  onChange,
  optional = false,
}: {
  readonly disabled: boolean;
  readonly label: string;
  readonly value: ProtectedFieldForm;
  readonly onChange: (value: ProtectedFieldForm) => void;
  readonly optional?: boolean;
}): React.JSX.Element {
  return (
    <Stack spacing={0.5}>
      <TextField
        disabled={disabled}
        fullWidth
        helperText={
          value.value.length === 0 && value.retain
            ? "Saved value will be retained. Type to replace it."
            : undefined
        }
        label={label}
        onChange={(event) => onChange({ ...value, value: event.target.value })}
        slotProps={{ htmlInput: { autoComplete: "new-password", spellCheck: false } }}
        type="password"
        value={value.value}
      />
      {optional && (value.retain || value.value.length > 0) ? (
        <Button
          disabled={disabled}
          onClick={() => onChange({ value: "", retain: false })}
          sx={{ alignSelf: "flex-start" }}
          variant="text"
        >
          Clear {label.toLocaleLowerCase("en-US")}
        </Button>
      ) : null}
    </Stack>
  );
}

export function ProfileProtectedFileField({
  disabled,
  label,
  value,
  onChange,
  kind = "pem",
}: {
  readonly disabled: boolean;
  readonly label: string;
  readonly value: ProtectedFieldForm;
  readonly onChange: (value: ProtectedFieldForm, filename: string) => void;
  readonly kind?: ProfileTrustKind;
}): React.JSX.Element {
  const input = useRef<HTMLInputElement>(null);
  const controller = useRef<AbortController | undefined>(undefined);
  const current = useRef({ onChange, value });
  useEffect(() => {
    current.current = { onChange, value };
  }, [onChange, value]);
  const [error, setError] = useState<string>();
  useEffect(() => {
    controller.current?.abort();
    return (): void => {
      controller.current?.abort();
    };
  }, [disabled, kind]);

  async function selectFile(file?: File): Promise<void> {
    if (file === undefined) return;
    controller.current?.abort();
    const next = new AbortController();
    controller.current = next;
    setError(undefined);
    try {
      const material = await readTrustFile(file, kind, next.signal);
      if (!next.signal.aborted)
        current.current.onChange({ ...current.current.value, value: material }, file.name);
    } catch {
      if (!next.signal.aborted)
        setError(`${label} could not be read. Select a valid bounded file.`);
    }
  }

  return (
    <Stack spacing={0.5}>
      <Stack direction="row" spacing={1} sx={{ alignItems: "center", flexWrap: "wrap" }}>
        <Button disabled={disabled} onClick={() => input.current?.click()} variant="outlined">
          Select {label.toLocaleLowerCase("en-US")}
        </Button>
        <input
          accept={kind === "pem" ? ".pem,.crt,.cer,.key" : ".jks,.p12,.pfx"}
          aria-label={`${label} file`}
          disabled={disabled}
          hidden
          onChange={(event) => {
            void selectFile(event.target.files?.[0]);
            event.target.value = "";
          }}
          ref={input}
          type="file"
        />
        <Typography color="text.secondary" variant="body2">
          {value.value.length > 0
            ? `${label} selected.`
            : value.retain
              ? `Saved ${label.toLocaleLowerCase("en-US")} will be retained.`
              : `No ${label.toLocaleLowerCase("en-US")} selected.`}
        </Typography>
      </Stack>
      {value.retain && value.value.length > 0 ? (
        <Button
          disabled={disabled}
          onClick={() => onChange({ ...value, value: "" }, "")}
          sx={{ alignSelf: "flex-start" }}
          variant="text"
        >
          Retain saved {label.toLocaleLowerCase("en-US")}
        </Button>
      ) : null}
      {error === undefined ? null : <Alert severity="error">{error}</Alert>}
    </Stack>
  );
}
