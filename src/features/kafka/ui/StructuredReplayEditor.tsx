import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import type { KafkaExploredMessage } from "../contracts";
import { RECORD_FORMATS } from "../contracts/record-codec";
import {
  StudioButton as Button,
  StudioLabeledControl as FormControlLabel,
  StudioCheckbox as Checkbox,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { replayWriterDrafts, type StructuredReplayDraft } from "./structured-replay-draft";

/** Source metadata suggests rows only; the host decodes and validates the actual original bytes. */
export function StructuredReplayEditor({
  field,
  draft,
  messages,
  disabled,
  onChange,
}: {
  readonly field: "key" | "value";
  readonly draft: StructuredReplayDraft | null;
  readonly messages: readonly KafkaExploredMessage[];
  readonly disabled: boolean;
  readonly onChange: (draft: StructuredReplayDraft | null) => void;
}): React.JSX.Element {
  const label = field === "key" ? "Key" : "Value";
  return (
    <Stack spacing={1}>
      <FormControlLabel
        label={`Transform structured ${field}`}
        control={
          <Checkbox
            checked={draft !== null}
            disabled={disabled}
            onChange={(_e, checked) =>
              onChange(
                checked
                  ? { codec: "auto", patches: "[]", mappings: replayWriterDrafts(messages, field) }
                  : null,
              )
            }
          />
        }
      />
      {draft && (
        <>
          <TextField
            select
            label={`${label} source codec`}
            value={draft.codec}
            disabled={disabled}
            onChange={(e) =>
              onChange({ ...draft, codec: e.target.value as StructuredReplayDraft["codec"] })
            }
          >
            <MenuItem value="auto">Automatic detection</MenuItem>
            {RECORD_FORMATS.map((f) => (
              <MenuItem value={f} key={f}>
                {f === "json" ? "JSON" : f === "avro" ? "Avro" : "Protobuf"}
              </MenuItem>
            ))}
          </TextField>
          <TextField
            label={`${label} JSON Pointer edits`}
            multiline
            minRows={2}
            value={draft.patches}
            disabled={disabled}
            onChange={(e) => onChange({ ...draft, patches: e.target.value })}
            helperText={
              'Up to 16 edits: [{"op":"set","path":"/status","json":"\\"fixed\\""},{"op":"remove","path":"/obsolete"}]. Use decimal strings for large integers. [] translates without edits.'
            }
          />
          <Typography variant="body2">
            Map every source writer to an existing destination subject and explicit version. Null
            keys and tombstones stay null. Unknown framed formats must be selected explicitly; the
            host verifies them.
          </Typography>
          <Table size="small" aria-label={`${label} destination writers`}>
            <TableHead>
              <TableRow>
                <TableCell>Source format / ID</TableCell>
                <TableCell>Destination writer</TableCell>
                <TableCell>Version</TableCell>
                <TableCell>Protobuf message</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {draft.mappings.map((mapping, i) => {
                const change = (patch: Partial<typeof mapping>): void =>
                  onChange({
                    ...draft,
                    mappings: draft.mappings.map((m, n) => (n === i ? { ...m, ...patch } : m)),
                  });
                return (
                  <TableRow key={i}>
                    <TableCell>
                      {mapping.sourceId === null ? (
                        "JSON / no frame"
                      ) : (
                        <>
                          <TextField
                            select
                            label={`${label} source ${mapping.sourceId} format`}
                            value={mapping.format}
                            disabled={disabled}
                            onChange={(e) =>
                              change({ format: e.target.value as "avro" | "protobuf" })
                            }
                          >
                            <MenuItem value="avro">Avro</MenuItem>
                            <MenuItem value="protobuf">Protobuf</MenuItem>
                          </TextField>{" "}
                          ID {mapping.sourceId}
                        </>
                      )}
                    </TableCell>
                    <TableCell>
                      {mapping.sourceId === null && (
                        <TextField
                          select
                          label={`${label} writer ${i + 1} output`}
                          value={mapping.target}
                          disabled={disabled}
                          onChange={(e) =>
                            change({ target: e.target.value as "json" | "registered" })
                          }
                        >
                          <MenuItem value="json">Plain JSON</MenuItem>
                          <MenuItem value="registered">Registered writer</MenuItem>
                        </TextField>
                      )}
                      {mapping.target === "registered" && (
                        <TextField
                          label={`${label} writer ${i + 1} subject`}
                          value={mapping.subject}
                          disabled={disabled}
                          onChange={(e) => change({ subject: e.target.value })}
                        />
                      )}
                    </TableCell>
                    <TableCell>
                      {mapping.target === "registered" && (
                        <TextField
                          label={`${label} writer ${i + 1} version`}
                          value={mapping.version}
                          disabled={disabled}
                          onChange={(e) => change({ version: e.target.value })}
                          sx={{ width: 85 }}
                        />
                      )}
                    </TableCell>
                    <TableCell>
                      {mapping.target === "registered" && (
                        <TextField
                          label={`${label} writer ${i + 1} message type`}
                          value={mapping.messageType}
                          disabled={disabled}
                          onChange={(e) => change({ messageType: e.target.value })}
                          helperText="Blank selects the first declared message."
                        />
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          <Button
            disabled={disabled}
            onClick={() =>
              onChange({ ...draft, mappings: replayWriterDrafts(messages, field, draft.mappings) })
            }
          >
            Refresh {field} source writers
          </Button>
        </>
      )}
    </Stack>
  );
}
