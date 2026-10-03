import Stack from "@mui/material/Stack";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableRow from "@mui/material/TableRow";

import { kafkaAclIdentity } from "../contracts";
import type { TopicAccessInput, TopicAccessExplanation } from "../contracts/acl-review";
import { StudioAlert as Alert, StudioTextField as TextField } from "../../../platform/ui/controls";

export function TopicAccessFields({
  value,
  disabled,
  onChange,
}: {
  readonly value: TopicAccessInput;
  readonly disabled: boolean;
  readonly onChange: (input: TopicAccessInput) => void;
}): React.JSX.Element {
  return (
    <Stack spacing={2}>
      <TextField
        label="Concrete topic"
        value={value.topic}
        disabled={disabled}
        onChange={(e) => onChange({ ...value, topic: e.target.value })}
      />
      <TextField
        label="Kafka principal"
        helperText="Exact mapped identity, for example User:alice; not a login name guess."
        value={value.principal}
        disabled={disabled}
        onChange={(e) => onChange({ ...value, principal: e.target.value })}
      />
      <TextField
        label="Client IP seen by Kafka"
        helperText="Use the broker-observed address, including NAT if applicable. Wildcards and DNS names are not accepted."
        value={value.host}
        disabled={disabled}
        onChange={(e) => onChange({ ...value, host: e.target.value })}
      />
    </Stack>
  );
}
export function TopicAccessEvidence({
  value,
  label,
}: {
  readonly value: TopicAccessExplanation;
  readonly label: string;
}): React.JSX.Element {
  return (
    <Stack spacing={1}>
      <Alert
        severity={
          value.effective === "allowed"
            ? "success"
            : value.effective === "denied"
              ? "error"
              : "warning"
        }
      >
        {label}: topic READ {value.effective}
      </Alert>
      <Table size="small" aria-label={`${label} access evidence`}>
        <TableBody>
          <TableRow>
            <TableCell>ACL decision for a non-superuser</TableCell>
            <TableCell>{value.aclDecision}</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Topic resource bindings</TableCell>
            <TableCell>{value.resourceBindings}</TableCell>
          </TableRow>
          <TableRow>
            <TableCell>StandardAuthorizer on all observed brokers</TableCell>
            <TableCell>
              {value.policy.standardAuthorizer ? "Confirmed" : "Unknown or unsupported"} (
              {value.policy.brokers} brokers)
            </TableCell>
          </TableRow>
          <TableRow>
            <TableCell>Allow when no topic ACL exists</TableCell>
            <TableCell>
              {value.policy.allowIfNoAcl === null
                ? "Unknown or inconsistent"
                : String(value.policy.allowIfNoAcl)}
            </TableCell>
          </TableRow>
          <TableRow>
            <TableCell>This principal is a superuser</TableCell>
            <TableCell>{value.policy.superuser}</TableCell>
          </TableRow>
        </TableBody>
      </Table>
      {value.reasons.map((reason) => (
        <div key={reason}>{reason}</div>
      ))}
      {value.matching.length > 0 && (
        <Table size="small" aria-label={`${label} matching READ bindings`}>
          <TableBody>
            {value.matching.map((acl) => (
              <TableRow key={kafkaAclIdentity(acl)}>
                <TableCell sx={{ overflowWrap: "anywhere" }}>{kafkaAclIdentity(acl)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
      {value.omittedBindings > 0 && (
        <Alert severity="info">
          {value.omittedBindings} matching bindings are omitted from this display; all were
          evaluated.
        </Alert>
      )}
    </Stack>
  );
}
