import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import type { HttpsTrustAuthentication } from "../contracts/https-trust-types";
import type { TrustAcquisitionRecipe } from "../contracts/trust-recipe-types";
import { resolveHttpsGet } from "../contracts/https-trust-validation";
import type { HttpsProfileAccess } from "../contracts/https-profile-access";
import type { ProfileCreateInput } from "../contracts/profile-types";
import {
  StudioAlert,
  StudioButton,
  StudioMenuItem,
  StudioTextField,
} from "../../../platform/ui/controls";

import { readTrustFile } from "./profile-dialog-model";

export interface HttpsAccessDraft extends HttpsProfileAccess {
  readonly secret: string;
  readonly caPem: string;
  readonly retainCa: boolean;
}

export const emptyHttpsAccess: HttpsAccessDraft = {
  host: "",
  username: "",
  tls: "system",
  secret: "",
  caPem: "",
  retainCa: false,
};

export function httpsAcquisitionOrigin(
  recipe: Extract<TrustAcquisitionRecipe, { method: "https" }>,
  overrides: Readonly<Record<string, string>>,
  host: string,
): string {
  try {
    const values = new Map(
      recipe.parameters.map((entry) => [
        entry.key,
        entry.type === "secret" ? "redacted" : (overrides[entry.key] ?? entry.defaultValue ?? ""),
      ]),
    );
    values.set("host", host);
    return new URL(resolveHttpsGet(recipe.https.material, recipe.parameters, values).url).origin;
  } catch {
    return "Complete the API host and template parameters to resolve the origin.";
  }
}

export function httpsAuthentication(
  mode: "none" | "bearer" | "basic",
  value: HttpsAccessDraft,
): HttpsTrustAuthentication {
  return mode === "none"
    ? { mode }
    : mode === "bearer"
      ? { mode, token: value.secret }
      : { mode, username: value.username, password: value.secret };
}

export function HttpsAcquisitionAccess({
  value,
  onChange,
  onCaChange,
  authentication,
  usesHost,
  origin,
  disabled,
}: {
  readonly value: HttpsAccessDraft;
  readonly onChange: (value: HttpsAccessDraft) => void;
  readonly onCaChange?: ((value: NonNullable<ProfileCreateInput["apiCa"]>) => void) | undefined;
  readonly authentication: "none" | "bearer" | "basic";
  readonly usesHost: boolean;
  readonly origin: string;
  readonly disabled: boolean;
}): React.JSX.Element {
  const [visible, setVisible] = useState(false);
  const [error, setError] = useState<string>();
  const readGeneration = useRef(0);
  const activeRead = useRef<AbortController | null>(null);
  useEffect(() => {
    setVisible(false);
    return (): void => {
      readGeneration.current += 1;
      activeRead.current?.abort();
    };
  }, [authentication]);
  return (
    <Stack spacing={2}>
      <Typography variant="body2">API origin: {origin}</Typography>
      <Typography variant="body2" color="text.secondary">
        Verified HTTPS only. API credentials are used for this acquisition and are never saved or
        reused from Kafka OAuth. Redirects are not followed.
      </Typography>
      {usesHost ? (
        <StudioTextField
          label="API host"
          value={value.host}
          disabled={disabled}
          onChange={(event) => onChange({ ...value, host: event.target.value })}
        />
      ) : null}
      {authentication === "basic" ? (
        <StudioTextField
          label="API username"
          value={value.username}
          disabled={disabled}
          onChange={(event) => onChange({ ...value, username: event.target.value })}
        />
      ) : null}
      {authentication !== "none" ? (
        <>
          <StudioTextField
            label={authentication === "bearer" ? "API bearer token" : "API password"}
            required
            type={visible ? "text" : "password"}
            value={value.secret}
            disabled={disabled}
            onChange={(event) => onChange({ ...value, secret: event.target.value })}
            helperText="Required again after closing the editor. Not included in templates, profiles or logs."
          />
          <StudioButton disabled={disabled} onClick={() => setVisible((previous) => !previous)}>
            {visible ? "Hide API credential" : "Show API credential"}
          </StudioButton>
        </>
      ) : null}
      <StudioTextField
        label="API TLS trust"
        select
        value={value.tls}
        disabled={disabled}
        onChange={(event) => {
          const tls = event.target.value;
          if (tls !== "system" && tls !== "custom") return;
          readGeneration.current += 1;
          activeRead.current?.abort();
          onChange({ ...value, tls, ...(tls === "system" ? { caPem: "", retainCa: false } : {}) });
          if (tls === "system") onCaChange?.({ mode: "clear" });
          setError(undefined);
        }}
      >
        <StudioMenuItem value="system">System certificate authorities</StudioMenuItem>
        <StudioMenuItem value="custom">Separate API CA (PEM)</StudioMenuItem>
      </StudioTextField>
      {value.tls === "custom" ? (
        <>
          <Typography variant="body2">
            {value.retainCa
              ? "Protected API CA retained. Select a file to replace it."
              : value.caPem
                ? "API CA selected. It is protected when you save this profile."
                : "Obtain the API CA independently from its administrator. The Kafka CA is not reused."}
          </Typography>
          <StudioTextField
            label="API CA file"
            type="file"
            disabled={disabled}
            slotProps={{ inputLabel: { shrink: true }, htmlInput: { accept: ".pem,.crt,.cer" } }}
            onChange={(event) => {
              const file = (event.target as HTMLInputElement).files?.[0];
              if (file === undefined) return;
              const generation = ++readGeneration.current;
              activeRead.current?.abort();
              const controller = new AbortController();
              activeRead.current = controller;
              setError(undefined);
              void readTrustFile(file, "pem", controller.signal)
                .then((caPem) => {
                  if (generation !== readGeneration.current) return;
                  if (!caPem.trim()) throw new Error("Choose a non-empty PEM CA file.");
                  onChange({ ...value, caPem, retainCa: false });
                  onCaChange?.({ mode: "replace", value: caPem });
                })
                .catch((failure: unknown) => {
                  if (generation === readGeneration.current)
                    setError(
                      failure instanceof Error
                        ? failure.message
                        : "The API CA file could not be read.",
                    );
                });
            }}
          />
        </>
      ) : null}
      {error === undefined ? null : <StudioAlert severity="error">{error}</StudioAlert>}
    </Stack>
  );
}
