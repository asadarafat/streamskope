import { useState } from "react";
import Stack from "@mui/material/Stack";

import {
  HOST_PROTOCOL_VERSION,
  kafkaAclIdentity,
  type KafkaAclBinding,
  type StreamSkopeHost,
  type KafkaWriteOutcome,
} from "../contracts";
import {
  aclChangeConfirmation,
  parseTopicAccessInput,
  type AclChangeReview,
  type TopicAccessInput,
  type TopicAccessExplanation,
} from "../contracts/acl-review";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { TopicAccessFields, TopicAccessEvidence } from "./TopicAccessReview";

export interface AclReviewSelection {
  readonly action: "create" | "delete";
  readonly acl: KafkaAclBinding;
}
export function AclReviewDialog({
  host,
  selection,
  canWrite,
  onClose,
  onApplied,
}: {
  readonly host: StreamSkopeHost;
  readonly selection: AclReviewSelection | null;
  readonly canWrite: boolean;
  readonly onClose: () => void;
  readonly onApplied: () => void;
}): React.JSX.Element {
  const acl = selection?.acl;
  const [access, setAccess] = useState<TopicAccessInput>({
    topic:
      acl?.resourceType === "TOPIC" && acl.patternType === "LITERAL" && acl.resourceName !== "*"
        ? acl.resourceName
        : "",
    principal: acl?.principal !== "User:*" ? (acl?.principal ?? "User:") : "User:",
    host: acl?.host !== "*" ? (acl?.host ?? "") : "",
  });
  const [review, setReview] = useState<AclChangeReview>();
  const [explanation, setExplanation] = useState<TopicAccessExplanation>();
  const [outcome, setOutcome] = useState<KafkaWriteOutcome>();
  const [confirmation, setConfirmation] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [attempted, setAttempted] = useState(false);
  const needsAccess = selection === null || acl?.resourceType === "TOPIC";
  let validAccess = !needsAccess;
  try {
    if (needsAccess) {
      parseTopicAccessInput(access);
      validAccess = true;
    }
  } catch {
    /* Keep incomplete input editable. */
  }
  const preview = async (): Promise<void> => {
    setBusy(true);
    setError(undefined);
    setReview(undefined);
    setExplanation(undefined);
    setConfirmation("");
    try {
      if (selection === null) {
        const response = await host.execute({
          command: "acls.access.explain",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: access,
        });
        if (response.ok) setExplanation(response.result.explanation);
        else setError(`${response.error.summary} ${response.error.recovery}`);
      } else {
        const response = await host.execute({
          command: "acls.change.review",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { ...selection, access: needsAccess ? access : null },
        });
        if (response.ok) setReview(response.result.review);
        else setError(`${response.error.summary} ${response.error.recovery}`);
      }
    } catch {
      setError("The host could not complete the review. No change was requested.");
    } finally {
      setBusy(false);
    }
  };
  const apply = async (): Promise<void> => {
    if (!review) return;
    setBusy(true);
    setAttempted(true);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "acls.change.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId, confirmation },
      });
      if (response.ok) {
        setOutcome(response.result.outcome);
        onApplied();
      } else setError(`${response.error.summary} ${response.error.recovery}`);
    } catch {
      setError(
        "The host response is unavailable; the ACL change may have been applied. Refresh the exact binding before a new review. This attempt cannot be resent.",
      );
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog
      open
      fullWidth
      maxWidth="md"
      onClose={() => {
        if (!busy) onClose();
      }}
    >
      <DialogTitle>
        {selection === null ? "Explain topic access" : `Review ACL ${selection.action}`}
      </DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2}>
          {acl && <code style={{ overflowWrap: "anywhere" }}>{kafkaAclIdentity(acl)}</code>}
          {needsAccess ? (
            <TopicAccessFields
              value={access}
              disabled={busy || attempted}
              onChange={(value) => {
                setAccess(value);
                setReview(undefined);
                setExplanation(undefined);
                setConfirmation("");
              }}
            />
          ) : (
            <Alert severity="info">
              Exact binding review is available. Effective-access implications for this resource
              type are not modeled.
            </Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          {explanation && <TopicAccessEvidence label="Current access" value={explanation} />}
          {review && (
            <>
              <Alert severity="info">
                Binding: {review.beforePresent ? "present" : "absent"} →{" "}
                {review.afterPresent ? "present" : "absent"}. Review expires at{" "}
                {new Date(review.expiresAt).toLocaleTimeString()} on {review.connectionName}.
              </Alert>
              {review.beforeAccess && (
                <TopicAccessEvidence label="Before" value={review.beforeAccess} />
              )}
              {review.afterAccess && (
                <TopicAccessEvidence label="After" value={review.afterAccess} />
              )}
              <Alert severity="warning">
                Only this exact binding is approved. Inventory and visible broker policy are checked
                again before dispatch. Kafka has no atomic compare-and-change for ACLs: another
                administrator can still change policy after the check. Broad bindings can affect
                additional readers and topics beyond this example.
              </Alert>
              <TextField
                fullWidth
                disabled={busy || attempted || !canWrite}
                label="Exact change confirmation"
                helperText={`Type: ${aclChangeConfirmation(review.input)}`}
                value={confirmation}
                onChange={(e) => setConfirmation(e.target.value)}
              />
            </>
          )}
          {outcome && (
            <Alert
              severity={
                outcome.state === "acknowledged" && outcome.verification === "verified"
                  ? "success"
                  : "warning"
              }
            >
              {outcome.state} · {outcome.verification}. {outcome.detail}
            </Alert>
          )}
          {selection && !canWrite && (
            <Alert severity="info">Read-only mode permits review and blocks application.</Alert>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button disabled={busy} onClick={onClose}>
          Close
        </Button>
        <Button
          disabled={busy || attempted || !validAccess}
          onClick={() => {
            void preview();
          }}
        >
          {" "}
          {selection === null ? "Explain access" : "Preview ACL change"}
        </Button>
        {selection && (
          <Button
            variant="contained"
            disabled={
              busy ||
              attempted ||
              !canWrite ||
              !review ||
              confirmation !== aclChangeConfirmation(review.input)
            }
            onClick={() => {
              void apply();
            }}
          >
            Apply reviewed ACL change
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
}
