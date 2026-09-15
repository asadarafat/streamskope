import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  TRUST_RECIPE_LIMITS,
  TRUST_RECIPE_PARAMETER_TYPES,
  type TrustAcquisitionRecipeInput,
  type TrustRecipeParameter,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioAccordion as Accordion,
  StudioAccordionSummary as AccordionSummary,
  StudioAccordionDetails as AccordionDetails,
  StudioCheckbox as Checkbox,
  StudioLabeledControl as FormControlLabel,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../ui/controls";

import { HttpsTrustRecipeFields } from "./HttpsTrustRecipeFields";

interface Properties {
  readonly value: TrustAcquisitionRecipeInput;
  readonly onChange: (value: TrustAcquisitionRecipeInput) => void;
  readonly issue?: { readonly path: string; readonly message: string } | undefined;
}

function withActiveFormat(
  value: TrustAcquisitionRecipeInput,
  kind: TrustAcquisitionRecipeInput["kind"],
): TrustAcquisitionRecipeInput {
  if (value.method === "https")
    return {
      ...value,
      kind,
      https: {
        ...value.https,
        material: {
          ...value.https.material,
          extraction:
            kind !== "pem" && value.https.material.extraction.mode === "json-pem"
              ? { mode: "json-base64", pointer: value.https.material.extraction.pointer }
              : value.https.material.extraction,
        },
        password:
          kind === "pem"
            ? { source: "none" }
            : value.https.password.source === "none"
              ? { source: "ask" }
              : value.https.password,
      },
    };
  return {
    ...value,
    kind,
    ssh: {
      ...value.ssh,
      password:
        kind === "pem"
          ? { source: "none" }
          : value.ssh.password.source === "none"
            ? { source: "ask" }
            : value.ssh.password,
    },
  };
}

export function TrustRecipeFields({ value, onChange, issue }: Properties): React.JSX.Element {
  const [advanced, setAdvanced] = useState(false);
  const fields = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (issue === undefined) return;
    if (issue.path.startsWith("recipe.parameters") || issue.path.startsWith("recipe.oauth"))
      setAdvanced(true);
    fields.current
      ?.querySelector<HTMLElement>(
        'input[aria-invalid="true"],textarea[aria-invalid="true"],[role="combobox"][aria-invalid="true"]',
      )
      ?.focus();
  }, [issue, advanced]);
  function oauth(patch: Partial<NonNullable<TrustAcquisitionRecipeInput["oauth"]>>): void {
    if (value.oauth !== undefined) onChange({ ...value, oauth: { ...value.oauth, ...patch } });
  }
  function field(path: string): { error: boolean; helperText: string | undefined } {
    return {
      error: issue?.path === `recipe.${path}`,
      helperText: issue?.path === `recipe.${path}` ? issue.message : undefined,
    };
  }
  function parameter(index: number, patch: Partial<TrustRecipeParameter>): void {
    onChange({
      ...value,
      parameters: value.parameters.map((entry, i) =>
        i === index ? { ...entry, ...patch } : entry,
      ),
    });
  }
  return (
    <Stack ref={fields} spacing={2}>
      <TextField
        autoFocus
        label="Retrieval profile name"
        value={value.name}
        onChange={(event) => onChange({ ...value, name: event.target.value })}
        {...field("name")}
      />
      <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
        <TextField
          select
          label="Trust format"
          value={value.kind}
          onChange={(event) => {
            const kind = event.target.value;
            if (kind !== "pem" && kind !== "jks" && kind !== "pkcs12") return;
            onChange(withActiveFormat(value, kind));
          }}
        >
          <MenuItem value="pem">PEM</MenuItem>
          <MenuItem value="jks">JKS</MenuItem>
          <MenuItem value="pkcs12">PKCS12</MenuItem>
        </TextField>
        <TextField
          select
          label="Retrieval method"
          value={value.method}
          onChange={(event) => {
            if (event.target.value === "https")
              onChange(
                withActiveFormat(
                  {
                    ...value,
                    method: "https",
                    syntax: "named-v1",
                    https: value.https ?? {
                      authentication: "none",
                      material: { url: "", headers: [], query: [], extraction: { mode: "raw" } },
                      password: { source: value.kind === "pem" ? "none" : "ask" },
                    },
                  },
                  value.kind,
                ),
              );
            else if (event.target.value === "ssh")
              onChange(
                withActiveFormat(
                  {
                    ...value,
                    method: "ssh",
                    syntax: value.ssh?.source === "legacy-tempfile" ? "legacy-v1" : "named-v1",
                    ssh: value.ssh ?? {
                      source: "file",
                      value: "",
                      password: { source: value.kind === "pem" ? "none" : "ask" },
                    },
                  },
                  value.kind,
                ),
              );
          }}
        >
          <MenuItem value="ssh">Remote SSH</MenuItem>
          <MenuItem value="https">HTTPS API</MenuItem>
        </TextField>
      </Stack>
      {value.method === "https" ? (
        <HttpsTrustRecipeFields
          value={value.https}
          binary={value.kind !== "pem"}
          onChange={(https) => onChange({ ...value, https })}
          field={field}
        />
      ) : (
        <>
          <TextField
            select
            label="Material source"
            value={value.ssh.source}
            disabled={value.syntax === "legacy-v1"}
            onChange={(event) => {
              const source = event.target.value;
              if (source === "file" || source === "stdout")
                onChange({ ...value, ssh: { ...value.ssh, source } });
            }}
          >
            <MenuItem value="file">Remote file</MenuItem>
            <MenuItem value="stdout">Command output</MenuItem>
            {value.syntax === "legacy-v1" ? (
              <MenuItem value="legacy-tempfile">Legacy temporary file</MenuItem>
            ) : null}
          </TextField>
          <TextField
            label={value.ssh.source === "file" ? "Remote file" : "Material command"}
            multiline={value.ssh.source !== "file"}
            minRows={value.ssh.source === "file" ? 1 : 3}
            value={value.ssh.value}
            onChange={(event) =>
              onChange({ ...value, ssh: { ...value.ssh, value: event.target.value } })
            }
            {...field("ssh.value")}
            helperText={
              field("ssh.value").helperText ??
              (value.ssh.source === "stdout"
                ? "Return raw certificate or truststore bytes on stdout. Do not redirect to a file or base64-encode the output."
                : value.ssh.source === "file"
                  ? "Absolute path on the SSH server. The file is read directly and is not deleted."
                  : undefined)
            }
          />
          {value.syntax === "named-v1" &&
          value.ssh.source === "stdout" &&
          value.ssh.value.includes("{truststorePath}") ? (
            <Alert severity="warning">
              {
                "{truststorePath} is a legacy output-file placeholder and is not expanded in Command output mode. Remove the redirection so the command returns the truststore bytes on stdout."
              }
            </Alert>
          ) : null}
          {value.kind !== "pem" ? (
            <>
              <TextField
                select
                label="Truststore password source"
                value={value.ssh.password.source}
                onChange={(event) => {
                  const source = event.target.value;
                  if (source === "ask" || source === "command")
                    onChange({
                      ...value,
                      ssh: {
                        ...value.ssh,
                        password: source === "ask" ? { source } : { source, command: "" },
                      },
                    });
                }}
                {...field("ssh.password")}
              >
                <MenuItem value="ask">Ask during acquisition</MenuItem>
                <MenuItem value="command">Remote command</MenuItem>
              </TextField>
              {value.ssh.password.source === "command" ? (
                <TextField
                  label="Password command"
                  multiline
                  minRows={2}
                  value={value.ssh.password.command}
                  onChange={(event) =>
                    onChange({
                      ...value,
                      ssh: {
                        ...value.ssh,
                        password: { source: "command", command: event.target.value },
                      },
                    })
                  }
                  {...field("ssh.password.command")}
                />
              ) : null}
            </>
          ) : null}
          <Typography color="text.secondary" variant="body2">
            Commands are trusted remote instructions. Saving never executes them. Do not put
            passwords or keys in a template.
          </Typography>
        </>
      )}
      <Typography component="h3" variant="subtitle2">
        Parameters
      </Typography>
      {value.parameters.length === 0 ? (
        <Typography variant="body2" color="text.secondary">
          No parameters defined.
        </Typography>
      ) : null}
      {value.parameters.map((entry, index) => (
        <Stack key={index} spacing={1} sx={{ border: 1, borderColor: "divider", p: 1.5 }}>
          <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
            <TextField
              label={`Parameter ${index + 1} key`}
              value={entry.key}
              onChange={(event) => parameter(index, { key: event.target.value })}
              {...field(`parameters[${index}].key`)}
            />
            <TextField
              label={`Parameter ${index + 1} label`}
              value={entry.label}
              onChange={(event) => parameter(index, { label: event.target.value })}
              {...field(`parameters[${index}].label`)}
            />
            <TextField
              select
              label={`Parameter ${index + 1} type`}
              value={entry.type}
              onChange={(event) => {
                const type = TRUST_RECIPE_PARAMETER_TYPES.find(
                  (candidate) => candidate === event.target.value,
                );
                if (type === undefined) return;
                const { defaultValue } = entry;
                const base = { ...entry };
                delete base.defaultValue;
                delete base.choices;
                const updated: TrustRecipeParameter = {
                  ...base,
                  type,
                  ...(type === "secret" || defaultValue === undefined ? {} : { defaultValue }),
                  ...(type === "choice" ? { choices: [] } : {}),
                };
                onChange({
                  ...value,
                  parameters: value.parameters.map((item, i) => (i === index ? updated : item)),
                });
              }}
            >
              {TRUST_RECIPE_PARAMETER_TYPES.map((type) => (
                <MenuItem key={type} value={type}>
                  {type}
                </MenuItem>
              ))}
            </TextField>
          </Stack>
          <TextField
            label={`Parameter ${index + 1} help`}
            value={entry.help ?? ""}
            onChange={(event) => parameter(index, { help: event.target.value })}
            {...field(`parameters[${index}].help`)}
          />
          {entry.type === "choice" ? (
            <TextField
              label={`Parameter ${index + 1} choices (one per line)`}
              multiline
              value={entry.choices?.join("\n") ?? ""}
              onChange={(event) => parameter(index, { choices: event.target.value.split("\n") })}
              {...field(`parameters[${index}].choices`)}
            />
          ) : null}
          {entry.type !== "secret" ? (
            <TextField
              label={`Parameter ${index + 1} default`}
              value={entry.defaultValue ?? ""}
              onChange={(event) => {
                const base = { ...entry };
                delete base.defaultValue;
                onChange({
                  ...value,
                  parameters: value.parameters.map((item, i) =>
                    i === index
                      ? {
                          ...base,
                          ...(event.target.value === ""
                            ? {}
                            : { defaultValue: event.target.value }),
                        }
                      : item,
                  ),
                });
              }}
              {...field(`parameters[${index}].defaultValue`)}
            />
          ) : (
            <Typography variant="body2">
              Secret values are entered only during acquisition, never saved here.
            </Typography>
          )}
          <Stack direction="row" sx={{ justifyContent: "space-between" }}>
            <FormControlLabel
              label={`Parameter ${index + 1} required`}
              control={
                <Checkbox
                  checked={entry.required}
                  onChange={(event) => parameter(index, { required: event.target.checked })}
                />
              }
            />
            <Button
              onClick={() =>
                onChange({ ...value, parameters: value.parameters.filter((_, i) => i !== index) })
              }
            >
              Remove parameter {index + 1}
            </Button>
          </Stack>
        </Stack>
      ))}
      <Button
        disabled={value.parameters.length >= TRUST_RECIPE_LIMITS.parameters}
        onClick={() =>
          onChange({
            ...value,
            parameters: [...value.parameters, { key: "", label: "", type: "text", required: true }],
          })
        }
        sx={{ alignSelf: "flex-start" }}
      >
        Add parameter
      </Button>
      <FormControlLabel
        label="Suggest Kafka OAuth settings"
        control={
          <Checkbox
            checked={value.oauth !== undefined}
            onChange={(event) => {
              const base = { ...value };
              delete base.oauth;
              onChange(
                event.target.checked
                  ? { ...base, oauth: { endpoint: "", clientId: "", scope: "" } }
                  : base,
              );
            }}
          />
        }
      />
      {value.oauth === undefined ? null : (
        <>
          <Typography variant="body2" color="text.secondary">
            Suggestions do not authenticate retrieval or change a profile automatically.
          </Typography>
          <TextField
            label="Suggested OAuth endpoint"
            value={value.oauth.endpoint}
            onChange={(event) => oauth({ endpoint: event.target.value })}
            {...field("oauth.endpoint")}
          />
          <TextField
            label="Suggested OAuth client ID"
            value={value.oauth.clientId}
            onChange={(event) => oauth({ clientId: event.target.value })}
            {...field("oauth.clientId")}
          />
          <TextField
            label="Suggested OAuth scope"
            value={value.oauth.scope}
            onChange={(event) => oauth({ scope: event.target.value })}
            {...field("oauth.scope")}
          />
        </>
      )}
      <Accordion
        expanded={advanced || issue?.path === "recipe.timeoutSeconds"}
        onChange={(_, expanded) => setAdvanced(expanded)}
      >
        <AccordionSummary aria-controls="recipe-advanced-content" id="recipe-advanced-summary">
          Advanced
        </AccordionSummary>
        <AccordionDetails id="recipe-advanced-content">
          <TextField
            label="Execution timeout (seconds)"
            type="number"
            value={value.timeoutSeconds}
            onChange={(event) => onChange({ ...value, timeoutSeconds: Number(event.target.value) })}
            {...field("timeoutSeconds")}
          />
        </AccordionDetails>
      </Accordion>
    </Stack>
  );
}
