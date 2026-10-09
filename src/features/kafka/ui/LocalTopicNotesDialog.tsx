import { useState } from "react";
import { Stack, Typography } from "@mui/material";

import type { StreamSkopeHost } from "../contracts";
import { KAFKA_TOPIC_CATALOG_LIMITS, type KafkaTopicAnnotation } from "../contracts/topic-catalog";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { useLocalTopicNotes } from "./use-local-topic-notes";

const key = (entry: KafkaTopicAnnotation): string =>
  JSON.stringify([entry.identity.clusterId, entry.identity.topicId]);

export function LocalTopicNotesDialog({
  host,
  connected,
  authorityKey,
  initialTopic,
  currentTopic,
  onClose,
}: {
  readonly host: StreamSkopeHost;
  readonly connected: boolean;
  readonly authorityKey: string;
  readonly initialTopic: string | null;
  readonly currentTopic: string | null;
  readonly onClose: () => void;
}): React.JSX.Element {
  const notes = useLocalTopicNotes({ host, connected, authorityKey, initialTopic });
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [discard, setDiscard] = useState<{ readonly run: () => void }>();
  const selected = notes.selection;
  const draft = selected?.draft;
  const editable = notes.verified && !notes.busy;
  const request = (run: () => void): void => {
    if (notes.busy) return;
    setConfirmDelete(false);
    if (notes.dirty) setDiscard({ run });
    else run();
  };
  return (
    <Dialog
      open
      fullWidth
      maxWidth="sm"
      onClose={() => request(onClose)}
      aria-labelledby="local-topic-notes-title"
    >
      <DialogTitle id="local-topic-notes-title">Local topic notes</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ pt: 1 }}>
          <Typography variant="body2">
            Keep a description, owner, labels and runbook links for a verified Kafka topic. Notes
            stay in this host's local library and do not change Kafka. Do not include credentials or
            secrets.
          </Typography>
          {notes.catalog?.durability === "session" ? (
            <Alert severity="info">This host keeps notes only until it restarts.</Alert>
          ) : null}
          {notes.error ? <Alert severity="error">{notes.error}</Alert> : null}
          {notes.status ? <Typography role="status">{notes.status}</Typography> : null}
          <TextField
            select
            label="Saved topic notes"
            disabled={notes.busy || notes.catalog === undefined}
            value={
              selected?.expected &&
              notes.catalog?.topics.some((entry) => key(entry) === key(selected.expected!))
                ? key(selected.expected)
                : ""
            }
            onChange={(event) => {
              const entry = notes.catalog?.topics.find(
                (candidate) => key(candidate) === event.target.value,
              );
              if (entry !== undefined) request(() => notes.select(entry));
            }}
          >
            <MenuItem value="">Choose saved notes</MenuItem>
            {notes.catalog?.topics.map((entry) => (
              <MenuItem key={key(entry)} value={key(entry)}>
                {`${entry.identity.topic} · ${entry.identity.clusterId} · ${entry.identity.topicId.slice(0, 8)}`}
              </MenuItem>
            ))}
          </TextField>
          <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap", rowGap: 1 }}>
            <Button disabled={notes.busy} onClick={() => void notes.refresh()}>
              Refresh saved notes
            </Button>
            <Button
              disabled={notes.busy || !connected || currentTopic === null}
              onClick={() =>
                request(() => {
                  if (currentTopic !== null) void notes.load(currentTopic);
                })
              }
            >
              Verify current topic
            </Button>
          </Stack>
          <Typography variant="caption">
            {connected && currentTopic !== null
              ? `Current topic: ${currentTopic}. Verification reads metadata only.`
              : "Connect and choose a topic to edit. Saved notes can be reviewed or removed while disconnected, even if the topic no longer exists."}
          </Typography>
          {notes.catalog?.topics.length === 0 && draft === undefined ? (
            <Typography variant="body2">No local topic notes are saved.</Typography>
          ) : null}
          {discard ? (
            <Alert severity="warning">
              Discard this unsaved draft before continuing?
              <Stack direction="row" spacing={1}>
                <Button onClick={() => setDiscard(undefined)}>Keep draft</Button>
                <Button
                  onClick={() => {
                    const action = discard;
                    setDiscard(undefined);
                    action.run();
                  }}
                >
                  Discard draft and continue
                </Button>
              </Stack>
            </Alert>
          ) : null}
          {draft !== undefined ? (
            <>
              <Stack component="section" aria-label="Topic note identity" spacing={0.5}>
                <Typography variant="subtitle2">{draft.identity.topic}</Typography>
                <Typography variant="caption" sx={{ overflowWrap: "anywhere" }}>
                  Cluster: {draft.identity.clusterId}
                </Typography>
                <Typography variant="caption" sx={{ overflowWrap: "anywhere" }}>
                  Topic ID: {draft.identity.topicId}
                </Typography>
              </Stack>
              {!notes.verified ? (
                <Alert severity="info">
                  Review only. Verify the current topic to edit. A replacement topic with the same
                  name has a different identity and does not inherit these notes. Any unsaved draft
                  stays with the identity shown here.
                </Alert>
              ) : null}
              <TextField
                label="Description"
                multiline
                minRows={3}
                maxRows={8}
                value={draft.description}
                onChange={(event) => notes.update({ description: event.target.value })}
                slotProps={{ input: { readOnly: !editable } }}
              />
              <TextField
                label="Owner"
                value={draft.owner}
                onChange={(event) => notes.update({ owner: event.target.value })}
                slotProps={{
                  input: { readOnly: !editable },
                  htmlInput: { maxLength: KAFKA_TOPIC_CATALOG_LIMITS.ownerCharacters },
                }}
              />
              <TextField
                label="Labels"
                value={draft.labels.join(",")}
                helperText="Separate labels with commas. Up to 16 distinct labels."
                onChange={(event) => notes.update({ labels: event.target.value.split(",") })}
                slotProps={{ input: { readOnly: !editable } }}
              />
              <Typography variant="subtitle2">Runbook links</Typography>
              {draft.links.map((link, index) => (
                <Stack key={index} spacing={1}>
                  <TextField
                    label={`Runbook ${String(index + 1)} title`}
                    value={link.title}
                    onChange={(event) =>
                      notes.update({
                        links: draft.links.map((entry, position) =>
                          position === index ? { ...entry, title: event.target.value } : entry,
                        ),
                      })
                    }
                    slotProps={{
                      input: { readOnly: !editable },
                      htmlInput: { maxLength: KAFKA_TOPIC_CATALOG_LIMITS.linkTitleCharacters },
                    }}
                  />
                  <TextField
                    label={`Runbook ${String(index + 1)} HTTPS URL`}
                    value={link.url}
                    onChange={(event) =>
                      notes.update({
                        links: draft.links.map((entry, position) =>
                          position === index ? { ...entry, url: event.target.value } : entry,
                        ),
                      })
                    }
                    slotProps={{
                      input: { readOnly: !editable },
                      htmlInput: { maxLength: KAFKA_TOPIC_CATALOG_LIMITS.linkUrlCharacters },
                    }}
                  />
                  {editable ? (
                    <Button
                      onClick={() =>
                        notes.update({
                          links: draft.links.filter((_entry, position) => position !== index),
                        })
                      }
                    >
                      Remove runbook {String(index + 1)}
                    </Button>
                  ) : null}
                </Stack>
              ))}
              <Button
                disabled={!editable || draft.links.length >= KAFKA_TOPIC_CATALOG_LIMITS.links}
                onClick={() => notes.update({ links: [...draft.links, { title: "", url: "" }] })}
              >
                Add runbook link
              </Button>
              {selected?.expected?.links.map((link) => (
                <Button
                  key={link.title}
                  disabled={notes.busy || notes.dirty}
                  onClick={() => void notes.openLink(link.url)}
                >
                  Open runbook: {link.title}
                </Button>
              ))}
              {notes.dirty && (selected?.expected?.links.length ?? 0) > 0 ? (
                <Typography variant="caption">
                  Save changes before opening edited runbook links.
                </Typography>
              ) : null}
              {selected?.expected !== null ? (
                <Button disabled={notes.busy} onClick={() => setConfirmDelete(true)}>
                  Remove saved notes
                </Button>
              ) : null}
            </>
          ) : null}
          {confirmDelete && selected?.expected ? (
            <Alert severity="warning">
              Remove the saved notes for “{selected.expected.identity.topic}” from this host? Kafka
              is unchanged.
              {notes.dirty ? " The unsaved draft will also be discarded." : ""}
              <Stack direction="row" spacing={1}>
                <Button disabled={notes.busy} onClick={() => setConfirmDelete(false)}>
                  Keep notes
                </Button>
                <Button
                  disabled={notes.busy}
                  onClick={() =>
                    void notes.remove().then((removed) => {
                      if (removed) setConfirmDelete(false);
                    })
                  }
                >
                  Confirm remove notes
                </Button>
              </Stack>
            </Alert>
          ) : null}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button disabled={notes.busy} onClick={() => request(onClose)}>
          Close
        </Button>
        <Button
          variant="contained"
          disabled={!editable || !notes.dirty}
          onClick={() => void notes.save()}
        >
          Save topic notes
        </Button>
      </DialogActions>
    </Dialog>
  );
}
