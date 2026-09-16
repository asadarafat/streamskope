import { useEffect, useRef, useState, type ReactNode } from "react";
import { Stack, Typography } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { ProfileAcquisitionBinding, ProfileBindingInput } from "../contracts/profile-binding";
import type {
  TrustAcquisitionRecipe,
  TrustAcquisitionRecipeSnapshot,
} from "../contracts/trust-recipe-types";
import type { AcceptedSshIdentity } from "../contracts/remote-trust-types";
import { validateTrustRecipeParameterValue } from "../contracts/trust-recipe-validation";
import { StudioAlert, StudioButton, StudioMenuItem, StudioTextField } from "../../../platform/ui/controls";

export interface ProfileTrustRecipeSelection {
  readonly apiCaPresent?: boolean;
  readonly identity?: AcceptedSshIdentity;
  readonly recipe: TrustAcquisitionRecipe;
  readonly reference: Extract<ProfileBindingInput, { readonly mode: "replace" }>;
}

/** Chooses a pinned recipe and profile overrides; never executes or edits the library. */
export function ProfileTrustRecipeSelector({
  host,
  profile,
  disabled = false,
  onChange,
  managementAction,
}: {
  readonly host: StreamSkopeHost;
  readonly profile?: { readonly id: string; readonly revision: number } | undefined;
  readonly disabled?: boolean;
  readonly onChange: (selection: ProfileTrustRecipeSelection | null) => void;
  readonly managementAction?: ReactNode;
}): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<TrustAcquisitionRecipeSnapshot>();
  const [selection, setSelection] = useState<ProfileTrustRecipeSelection | null>(null);
  const [loading, setLoading] = useState(true);
  const [methods, setMethods] = useState<readonly string[]>(["ssh"]);
  const [error, setError] = useState<string>();
  const [review, setReview] = useState<TrustAcquisitionRecipe>();
  const [resetIdentity, setResetIdentity] = useState(false);
  const changed = useRef(onChange);
  changed.current = onChange;
  const profileId = profile?.id;
  const profileRevision = profile?.revision;

  useEffect(() => {
    let current = true;
    setLoading(true);
    setError(undefined);
    setSelection(null);
    const unsubscribe = host.subscribe((event) => {
      if (current && event.event === "recipes.changed") setSnapshot(event.payload);
    });
    async function load(): Promise<void> {
      const capability = await host
        .execute({
          command: "trustAcquisition.capabilities",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: {},
        })
        .catch(() => undefined);
      if (current && capability?.ok && "sshAgent" in capability.result)
        setMethods(capability.result.methods ?? ["ssh"]);
      const response = await host.execute({
        command: "recipes.list",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      if (profileId !== undefined) {
        const detail = await host.execute({
          command: "profiles.binding.get",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { profileId },
        });
        if (!detail.ok) throw new Error(`${detail.error.summary} ${detail.error.recovery}`);
        if (
          !("bindingDetail" in detail.result) ||
          detail.result.bindingDetail.revision !== profileRevision
        )
          throw new Error(
            "This profile changed. Reopen the editor before changing its acquisition template.",
          );
        if (current && detail.result.bindingDetail.binding !== null) {
          const next = {
            ...fromBinding(detail.result.bindingDetail.binding),
            apiCaPresent: detail.result.bindingDetail.apiCaPresent === true,
          };
          setSelection(next);
          changed.current(next);
        }
      }
    }
    void load()
      .catch((failure: unknown) => {
        if (current)
          setError(
            failure instanceof Error
              ? failure.message
              : "Acquisition templates could not be loaded. Reopen the editor to retry.",
          );
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return (): void => {
      current = false;
      unsubscribe();
    };
  }, [host, profileId, profileRevision]);

  function select(next: ProfileTrustRecipeSelection | null): void {
    setSelection(next);
    changed.current(next);
  }
  const library = snapshot?.recipes ?? [];
  const latest = library.find((item) => item.id === selection?.recipe.id);
  const choices =
    selection === null
      ? library
      : [selection.recipe, ...library.filter((recipe) => recipe.id !== selection.recipe.id)];
  const unavailable = loading || error !== undefined || snapshot?.store.state !== "ready";

  return (
    <Stack spacing={1.5}>
      {error === undefined ? null : <StudioAlert severity="error">{error}</StudioAlert>}
      {loading ? (
        <Typography role="status" variant="body2">
          Loading retrieval profiles…
        </Typography>
      ) : null}
      <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
        <StudioTextField
          select
          fullWidth
          sx={{ minWidth: 0 }}
          label="Use profile"
          value={selection?.recipe.id ?? ""}
          disabled={disabled || unavailable}
          onChange={(event) => {
            const recipe = choices.find((item) => item.id === event.target.value);
            select(
              recipe === undefined
                ? null
                : {
                    recipe,
                    reference: {
                      mode: "replace",
                      recipeId: recipe.id,
                      recipeRevision: recipe.revision,
                      overrides: {},
                    },
                  },
            );
          }}
        >
          <StudioMenuItem value="">Choose a retrieval profile</StudioMenuItem>
          {choices.map((recipe) => (
            <StudioMenuItem
              key={recipe.id}
              value={recipe.id}
              disabled={!methods.includes(recipe.method)}
            >
              {recipe.name}
              {!methods.includes(recipe.method) ? " — acquisition unavailable on this host" : ""}
            </StudioMenuItem>
          ))}
        </StudioTextField>
        {managementAction}
      </Stack>
      {selection === null ? (
        <Typography variant="body2" color="text.secondary">
          Choose saved SSH or HTTPS instructions to fill your certificate and truststore password.
          Optional OAuth suggestions can fill endpoint, client ID and scope. Enter the Kafka OAuth
          client secret manually. Selecting a profile does not change connection fields.
        </Typography>
      ) : (
        <>
          {!methods.includes(selection.recipe.method) ? (
            <StudioAlert severity="warning">
              This host cannot execute HTTPS acquisition yet. Existing saved Kafka trust is
              unchanged.
            </StudioAlert>
          ) : null}
          <Typography variant="body2" color="text.secondary">
            {selection.recipe.kind.toUpperCase()} · {selection.recipe.method.toUpperCase()} · Pinned
            revision {selection.recipe.revision}
          </Typography>
          {selection.identity === undefined ? null : (
            <Stack spacing={1}>
              <Typography variant="body2" sx={{ overflowWrap: "anywhere" }}>
                Saved SSH identity: {selection.identity.host}:{selection.identity.port} ·{" "}
                {selection.identity.fingerprint}
              </Typography>
              {selection.reference.identity?.mode === "reset" ? (
                <StudioAlert severity="warning">
                  Identity reset is pending. Save and reopen this profile before acquiring again.
                  Existing Kafka trust will be retained.
                </StudioAlert>
              ) : (
                <>
                  <StudioButton disabled={disabled} onClick={() => setResetIdentity(true)}>
                    Reset saved SSH identity
                  </StudioButton>
                  {resetIdentity ? (
                    <StudioAlert severity="warning">
                      <Stack spacing={1}>
                        <Typography variant="body2">
                          Reset the accepted identity for {selection.identity.host}:
                          {selection.identity.port}? Verify the new server key independently before
                          accepting it. This does not replace existing Kafka trust.
                        </Typography>
                        <Stack direction="row" spacing={1}>
                          <StudioButton onClick={() => setResetIdentity(false)}>
                            Cancel identity reset
                          </StudioButton>
                          <StudioButton
                            disabled={disabled}
                            onClick={() => {
                              select({
                                ...selection,
                                reference: { ...selection.reference, identity: { mode: "reset" } },
                              });
                              setResetIdentity(false);
                            }}
                          >
                            Confirm identity reset
                          </StudioButton>
                        </Stack>
                      </Stack>
                    </StudioAlert>
                  ) : null}
                </>
              )}
            </Stack>
          )}
          {latest === undefined ? (
            <StudioAlert severity="warning">
              Removed from the library. This profile retains its pinned template.
            </StudioAlert>
          ) : latest.revision === selection.recipe.revision ? null : (
            <>
              <StudioAlert severity="info">
                Revision {latest.revision} is available. This profile continues to use revision{" "}
                {selection.recipe.revision} until you adopt the update.
              </StudioAlert>
              <StudioButton disabled={disabled} onClick={() => setReview(latest)}>
                Review template update
              </StudioButton>
            </>
          )}
          {review === undefined ? null : (
            <Stack spacing={1} role="region" aria-label="Template update review">
              <Typography variant="body2">
                Review changes before adopting revision {review.revision}. Incompatible parameter
                overrides will be reset; existing trust remains unchanged.
              </Typography>
              {(
                [
                  "name",
                  "kind",
                  "method",
                  "ssh",
                  "https",
                  "parameters",
                  "timeoutSeconds",
                  "oauth",
                ] as const
              )
                .filter(
                  (key) => JSON.stringify(selection.recipe[key]) !== JSON.stringify(review[key]),
                )
                .map((key) => (
                  <Stack key={key} spacing={0.5}>
                    <Typography variant="body2">{key}</Typography>
                    <Typography
                      component="pre"
                      variant="body2"
                      sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
                    >
                      Current: {JSON.stringify(selection.recipe[key]) ?? "Not set"}
                      {"\n"}Proposed: {JSON.stringify(review[key]) ?? "Not set"}
                    </Typography>
                  </Stack>
                ))}
              <Stack direction="row" spacing={1}>
                <StudioButton onClick={() => setReview(undefined)}>Cancel update</StudioButton>
                <StudioButton
                  disabled={disabled}
                  onClick={() => {
                    const overrides = Object.fromEntries(
                      Object.entries(selection.reference.overrides).filter(([key, value]) => {
                        const old = selection.recipe.parameters.find(
                          (parameter) => parameter.key === key,
                        );
                        const parameter = review.parameters.find(
                          (parameter) =>
                            parameter.key === key &&
                            parameter.type !== "secret" &&
                            parameter.type === old?.type,
                        );
                        if (parameter === undefined) return false;
                        try {
                          validateTrustRecipeParameterValue(parameter, value, "override");
                          return true;
                        } catch {
                          return false;
                        }
                      }),
                    );
                    select({
                      recipe: review,
                      reference: {
                        ...selection.reference,
                        recipeId: review.id,
                        recipeRevision: review.revision,
                        overrides,
                      },
                    });
                    setReview(undefined);
                  }}
                >
                  Adopt template update
                </StudioButton>
              </Stack>
            </Stack>
          )}
          {selection.recipe.parameters
            .filter((parameter) => parameter.type !== "secret")
            .map((parameter) => (
              <Stack
                key={parameter.key}
                direction={{ xs: "column", sm: "row" }}
                spacing={1}
                sx={{ alignItems: "flex-start" }}
              >
                <StudioTextField
                  fullWidth
                  select={parameter.type === "choice"}
                  label={parameter.label}
                  required={parameter.required}
                  helperText={parameter.help}
                  disabled={disabled}
                  value={
                    selection.reference.overrides[parameter.key] ?? parameter.defaultValue ?? ""
                  }
                  onChange={(event) =>
                    select({
                      ...selection,
                      reference: {
                        ...selection.reference,
                        overrides: {
                          ...selection.reference.overrides,
                          [parameter.key]: event.target.value,
                        },
                      },
                    })
                  }
                >
                  {parameter.type === "choice"
                    ? (parameter.choices ?? []).map((choice) => (
                        <StudioMenuItem key={choice} value={choice}>
                          {choice}
                        </StudioMenuItem>
                      ))
                    : undefined}
                </StudioTextField>
                <StudioButton
                  disabled={
                    disabled || !Object.hasOwn(selection.reference.overrides, parameter.key)
                  }
                  aria-label={`Reset ${parameter.label}`}
                  onClick={() => {
                    const overrides = { ...selection.reference.overrides };
                    delete overrides[parameter.key];
                    select({ ...selection, reference: { ...selection.reference, overrides } });
                  }}
                >
                  Reset
                </StudioButton>
              </Stack>
            ))}
        </>
      )}
    </Stack>
  );
}

function fromBinding(binding: ProfileAcquisitionBinding): ProfileTrustRecipeSelection {
  return {
    ...(binding.identity === undefined ? {} : { identity: binding.identity }),
    recipe: binding.recipe,
    reference: {
      mode: "replace",
      recipeId: binding.recipe.id,
      recipeRevision: binding.recipe.revision,
      overrides: binding.overrides,
      ...(binding.access === undefined ? {} : { access: binding.access }),
      ...(binding.apiAccess === undefined ? {} : { apiAccess: binding.apiAccess }),
    },
  };
}
