import { useEffect, useMemo, useState } from "react";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_ACL_OPERATIONS,
  KAFKA_ACL_PATTERN_TYPES,
  KAFKA_ACL_PERMISSIONS,
  KAFKA_ACL_RESOURCE_TYPES,
  kafkaAclIdentity,
  type KafkaAclBinding,
  type KafkaAclSnapshot,
  type StreamSkopeHost,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { ResourcePageHeader, resourcePageGutter } from "./ResourcePageHeader";
import { AclReviewDialog, type AclReviewSelection } from "./AclReviewDialog";
import { WorkbenchIcon } from "./WorkbenchIcons";
import { useHostCommand } from "./use-host-command";

const emptyAcl: KafkaAclBinding = {
  host: "*",
  operation: "READ",
  patternType: "LITERAL",
  permission: "ALLOW",
  principal: "User:",
  resourceName: "",
  resourceType: "TOPIC",
};

export function AclPage({
  connected,
  canWrite = true,
  host,
  snapshot,
}: {
  readonly connected: boolean;
  readonly canWrite?: boolean;
  readonly host: StreamSkopeHost;
  readonly snapshot: KafkaAclSnapshot;
}): React.JSX.Element {
  const [filter, setFilter] = useState("");
  const [draft, setDraft] = useState<KafkaAclBinding>(emptyAcl);
  const [createOpen, setCreateOpen] = useState(false);
  const [reviewing, setReviewing] = useState<AclReviewSelection | null | undefined>();
  const { busy, requestError, run } = useHostCommand(
    host,
    "The application host did not accept the ACL request. Open Activity for diagnostics.",
  );
  const refresh = (): void => {
    void run({
      command: "acls.list",
      id: globalThis.crypto.randomUUID(),
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
  };
  useEffect(() => {
    if (connected) refresh();
  }, [connected, snapshot.connectionName]);
  const normalized = filter.trim().toLocaleLowerCase("en-US");
  const acls = useMemo(
    () =>
      snapshot.acls.filter(
        (acl) =>
          normalized.length === 0 ||
          kafkaAclIdentity(acl).toLocaleLowerCase("en-US").includes(normalized),
      ),
    [normalized, snapshot.acls],
  );
  const inventoryMessage =
    snapshot.state === "unavailable"
      ? "Connect a profile to load ACLs."
      : snapshot.state === "loading"
        ? "Loading ACLs…"
        : snapshot.state === "unsupported"
          ? "This broker does not support Kafka ACL administration."
          : snapshot.state === "denied"
            ? "The connected identity cannot describe Kafka ACLs."
            : snapshot.state === "failed"
              ? "ACL inventory is unavailable."
              : snapshot.acls.length === 0
                ? "No ACL bindings exist."
                : "No ACLs match the current filter.";
  const updateDraft = <K extends keyof KafkaAclBinding>(key: K, value: KafkaAclBinding[K]): void =>
    setDraft((current) => ({ ...current, [key]: value }));
  return (
    <Box component="main" sx={{ minHeight: 0, overflow: "auto" }}>
      <ResourcePageHeader
        action={
          <Stack direction="row" spacing={1}>
            <Button disabled={!connected || busy} onClick={() => setReviewing(null)}>
              Explain access
            </Button>
            <Button
              disabled={!connected || busy}
              onClick={() => setCreateOpen(true)}
              startIcon={<WorkbenchIcon name="add" />}
              variant="contained"
            >
              Create ACL
            </Button>
            <Button
              disabled={!connected || busy}
              onClick={refresh}
              startIcon={<WorkbenchIcon name="refresh" />}
              variant="outlined"
            >
              Refresh
            </Button>
          </Stack>
        }
        description="Explain topic READ and review exact binding changes before applying them."
        title="Access Control Lists"
      />
      <Box sx={{ px: resourcePageGutter, py: 3 }}>
        <TextField
          fullWidth
          onChange={(event) => setFilter(event.target.value)}
          placeholder="Search resource, principal, host, or operation"
          slotProps={{ htmlInput: { "aria-label": "Search ACLs", type: "search" } }}
          value={filter}
        />
        {requestError === undefined ? null : (
          <Alert severity="error" sx={{ mt: 2 }}>
            {requestError}
          </Alert>
        )}
        {snapshot.error === undefined ? null : (
          <Alert severity="error" sx={{ mt: 2 }}>
            {snapshot.error.summary} {snapshot.error.recovery}
          </Alert>
        )}
        <TableContainer
          sx={{ bgcolor: "background.paper", border: 1, borderColor: "divider", mt: 2 }}
        >
          <Table aria-label="Kafka ACLs" size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Resource</TableCell>
                <TableCell>Pattern</TableCell>
                <TableCell>Principal</TableCell>
                <TableCell>Host</TableCell>
                <TableCell>Operation</TableCell>
                <TableCell>Permission</TableCell>
                <TableCell align="right">Action</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {acls.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} role={snapshot.state === "loading" ? "status" : undefined}>
                    {inventoryMessage}
                  </TableCell>
                </TableRow>
              ) : (
                acls.map((acl) => (
                  <TableRow key={kafkaAclIdentity(acl)}>
                    <TableCell>
                      {acl.resourceType} · {acl.resourceName}
                    </TableCell>
                    <TableCell>{acl.patternType}</TableCell>
                    <TableCell>{acl.principal}</TableCell>
                    <TableCell>{acl.host}</TableCell>
                    <TableCell>{acl.operation}</TableCell>
                    <TableCell>{acl.permission}</TableCell>
                    <TableCell align="right">
                      <Button
                        color="error"
                        onClick={() => {
                          setReviewing({ action: "delete", acl });
                        }}
                        variant="text"
                      >
                        Delete
                      </Button>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Box>
      <Dialog
        fullWidth
        maxWidth="sm"
        onClose={() => !busy && setCreateOpen(false)}
        open={createOpen}
      >
        <DialogTitle>Create exact ACL binding</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} sx={{ pt: 0.5 }}>
            <Stack direction="row" spacing={2}>
              <FormControl fullWidth>
                <InputLabel id="acl-resource-type-label">Resource type</InputLabel>
                <Select
                  label="Resource type"
                  labelId="acl-resource-type-label"
                  onChange={(event) => updateDraft("resourceType", event.target.value)}
                  value={draft.resourceType}
                >
                  {KAFKA_ACL_RESOURCE_TYPES.map((value) => (
                    <MenuItem key={value} value={value}>
                      {value}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <FormControl fullWidth>
                <InputLabel id="acl-pattern-label">Pattern</InputLabel>
                <Select
                  label="Pattern"
                  labelId="acl-pattern-label"
                  onChange={(event) => updateDraft("patternType", event.target.value)}
                  value={draft.patternType}
                >
                  {KAFKA_ACL_PATTERN_TYPES.map((value) => (
                    <MenuItem key={value} value={value}>
                      {value}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Stack>
            <TextField
              autoFocus
              fullWidth
              label="Resource name"
              onChange={(event) => updateDraft("resourceName", event.target.value)}
              value={draft.resourceName}
            />
            <TextField
              fullWidth
              label="Principal"
              onChange={(event) => updateDraft("principal", event.target.value)}
              value={draft.principal}
            />
            <TextField
              fullWidth
              label="Host"
              onChange={(event) => updateDraft("host", event.target.value)}
              value={draft.host}
            />
            <Stack direction="row" spacing={2}>
              <FormControl fullWidth>
                <InputLabel id="acl-operation-label">Operation</InputLabel>
                <Select
                  label="Operation"
                  labelId="acl-operation-label"
                  onChange={(event) => updateDraft("operation", event.target.value)}
                  value={draft.operation}
                >
                  {KAFKA_ACL_OPERATIONS.map((value) => (
                    <MenuItem key={value} value={value}>
                      {value}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <FormControl fullWidth>
                <InputLabel id="acl-permission-label">Permission</InputLabel>
                <Select
                  label="Permission"
                  labelId="acl-permission-label"
                  onChange={(event) => updateDraft("permission", event.target.value)}
                  value={draft.permission}
                >
                  {KAFKA_ACL_PERMISSIONS.map((value) => (
                    <MenuItem key={value} value={value}>
                      {value}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Stack>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => setCreateOpen(false)}>
            Cancel
          </Button>
          <Button
            disabled={
              busy ||
              draft.resourceName.trim().length === 0 ||
              draft.principal.trim().length === 0 ||
              draft.host.trim().length === 0
            }
            onClick={() => {
              setCreateOpen(false);
              setReviewing({ action: "create", acl: draft });
            }}
            variant="contained"
          >
            Create ACL
          </Button>
        </DialogActions>
      </Dialog>
      {reviewing !== undefined && (
        <AclReviewDialog
          key={
            reviewing === null
              ? "explain"
              : `${reviewing.action}:${kafkaAclIdentity(reviewing.acl)}`
          }
          host={host}
          selection={reviewing}
          canWrite={canWrite && connected}
          onClose={() => setReviewing(undefined)}
          onApplied={refresh}
        />
      )}
    </Box>
  );
}
