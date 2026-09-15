import { Stack, Typography } from "@mui/material";

import {
  HTTPS_TRUST_LIMITS,
  type HttpsTrustGetDefinition,
  type TrustRecipeHttps,
} from "../contracts/https-trust-types";
import {
  StudioButton as Button,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../ui/controls";

interface Properties {
  readonly value: TrustRecipeHttps;
  readonly binary: boolean;
  readonly onChange: (value: TrustRecipeHttps) => void;
  readonly field: (path: string) => { error: boolean; helperText: string | undefined };
}

function RequestFields({
  value,
  label,
  path,
  onChange,
  field,
}: {
  readonly value: HttpsTrustGetDefinition;
  readonly label: "Material" | "Password";
  readonly path: string;
  readonly onChange: (value: HttpsTrustGetDefinition) => void;
  readonly field: Properties["field"];
}): React.JSX.Element {
  return (
    <Stack spacing={1.5}>
      <TextField
        label={`${label} URL`}
        value={value.url}
        onChange={(event) => onChange({ ...value, url: event.target.value })}
        {...field(`${path}.url`)}
      />
      {(["headers", "query"] as const).map((kind) => (
        <Stack spacing={1} key={kind}>
          <Typography component="h3" variant="subtitle2">
            {label} {kind === "headers" ? "headers" : "query parameters"}
          </Typography>
          {value[kind].map((row, index) => (
            <Stack spacing={1} key={index}>
              <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
                <TextField
                  label={`${label} ${kind} ${index + 1} name`}
                  value={row.name}
                  onChange={(event) =>
                    onChange({
                      ...value,
                      [kind]: value[kind].map((entry, i) =>
                        i === index ? { ...entry, name: event.target.value } : entry,
                      ),
                    })
                  }
                  {...field(`${path}.${kind}[${index}].name`)}
                />
                <TextField
                  label={`${label} ${kind} ${index + 1} value`}
                  value={row.value}
                  onChange={(event) =>
                    onChange({
                      ...value,
                      [kind]: value[kind].map((entry, i) =>
                        i === index ? { ...entry, value: event.target.value } : entry,
                      ),
                    })
                  }
                  {...field(`${path}.${kind}[${index}].value`)}
                />
                <Button
                  onClick={() =>
                    onChange({ ...value, [kind]: value[kind].filter((_, i) => i !== index) })
                  }
                >
                  Remove {label.toLowerCase()} {kind} {index + 1}
                </Button>
              </Stack>
            </Stack>
          ))}
          <Button
            disabled={value[kind].length >= HTTPS_TRUST_LIMITS.entries}
            sx={{ alignSelf: "flex-start" }}
            onClick={() =>
              onChange({ ...value, [kind]: [...value[kind], { name: "", value: "" }] })
            }
          >
            Add {label.toLowerCase()} {kind === "headers" ? "header" : "query parameter"}
          </Button>
        </Stack>
      ))}
    </Stack>
  );
}

export function HttpsTrustRecipeFields({
  value,
  binary,
  onChange,
  field,
}: Properties): React.JSX.Element {
  return (
    <Stack spacing={2}>
      <Typography variant="body2" color="text.secondary">
        HTTPS GET only. Credentials and API CA trust belong to the profile, not this template.
        Redirects are never followed.
      </Typography>
      <TextField
        select
        label="API authentication"
        value={value.authentication}
        onChange={(event) => {
          const authentication = event.target.value;
          if (
            authentication === "none" ||
            authentication === "bearer" ||
            authentication === "basic"
          )
            onChange({ ...value, authentication });
        }}
        {...field("https.authentication")}
      >
        <MenuItem value="none">None</MenuItem>
        <MenuItem value="bearer">Bearer token</MenuItem>
        <MenuItem value="basic">Basic</MenuItem>
      </TextField>
      <RequestFields
        value={value.material}
        label="Material"
        path="https.material"
        field={field}
        onChange={(material) =>
          onChange({ ...value, material: { ...value.material, ...material } })
        }
      />
      <TextField
        select
        label="Material response"
        value={value.material.extraction.mode}
        onChange={(event) => {
          const mode = event.target.value;
          if (mode !== "raw" && mode !== "json-pem" && mode !== "json-base64") return;
          const extraction: TrustRecipeHttps["material"]["extraction"] =
            mode === "raw"
              ? { mode }
              : {
                  mode,
                  pointer:
                    value.material.extraction.mode === "raw"
                      ? ""
                      : value.material.extraction.pointer,
                };
          onChange({ ...value, material: { ...value.material, extraction } });
        }}
        {...field("https.material.extraction.mode")}
      >
        <MenuItem value="raw">Raw certificate or truststore bytes</MenuItem>
        {!binary ? <MenuItem value="json-pem">PEM string in JSON</MenuItem> : null}
        <MenuItem value="json-base64">Base64 string in JSON</MenuItem>
      </TextField>
      {value.material.extraction.mode === "raw" ? null : (
        <TextField
          label="Material JSON Pointer"
          value={value.material.extraction.pointer}
          onChange={(event) => {
            if (value.material.extraction.mode !== "raw")
              onChange({
                ...value,
                material: {
                  ...value.material,
                  extraction: { ...value.material.extraction, pointer: event.target.value },
                },
              });
          }}
          {...field("https.material.extraction.pointer")}
        />
      )}
      {binary ? (
        <>
          <TextField
            select
            label="Truststore password source"
            value={value.password.source}
            onChange={(event) => {
              const source = event.target.value;
              if (source === "ask") onChange({ ...value, password: { source } });
              if (source === "https")
                onChange({
                  ...value,
                  password: {
                    source,
                    request: {
                      url: value.material.url,
                      headers: [],
                      query: [],
                      extraction: { mode: "text" },
                    },
                  },
                });
            }}
            {...field("https.password")}
          >
            <MenuItem value="ask">Ask during acquisition</MenuItem>
            <MenuItem value="https">HTTPS GET from the same origin</MenuItem>
          </TextField>
          {value.password.source === "https" ? (
            <>
              <RequestFields
                value={value.password.request}
                label="Password"
                path="https.password.request"
                field={field}
                onChange={(request) => {
                  if (value.password.source === "https")
                    onChange({
                      ...value,
                      password: {
                        ...value.password,
                        request: { ...value.password.request, ...request },
                      },
                    });
                }}
              />
              <TextField
                select
                label="Password response"
                value={value.password.request.extraction.mode}
                onChange={(event) => {
                  const mode = event.target.value;
                  if (value.password.source === "https" && (mode === "text" || mode === "json"))
                    onChange({
                      ...value,
                      password: {
                        ...value.password,
                        request: {
                          ...value.password.request,
                          extraction: mode === "text" ? { mode } : { mode, pointer: "" },
                        },
                      },
                    });
                }}
              >
                <MenuItem value="text">Plain text</MenuItem>
                <MenuItem value="json">String in JSON</MenuItem>
              </TextField>
              {value.password.request.extraction.mode === "json" ? (
                <TextField
                  label="Password JSON Pointer"
                  value={value.password.request.extraction.pointer}
                  onChange={(event) => {
                    if (value.password.source === "https")
                      onChange({
                        ...value,
                        password: {
                          ...value.password,
                          request: {
                            ...value.password.request,
                            extraction: { mode: "json", pointer: event.target.value },
                          },
                        },
                      });
                  }}
                  {...field("https.password.request.extraction.pointer")}
                />
              ) : null}
            </>
          ) : null}
        </>
      ) : null}
    </Stack>
  );
}
