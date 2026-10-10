# Preview and reset consumer offsets

Resetting changes where a consumer group resumes. It can reprocess records or skip
unprocessed records. Stop **every consumer in the group** before applying a reset.
Inspect [consumer lag](operations.md) first and retain the original positions if
you may need to restore them.

1. Open **Consumer Groups**, choose the group, then **Reset offsets…**.
2. Select the topic partitions to change, up to 32. Choose **Explicit offsets**,
   **Earliest retained**, **Current end** or **At or after UTC time**. Explicit offsets
   are integers; a time must be ISO 8601 UTC ending in `Z`. Unselected partitions
   are outside the operation.
3. Choose **Preview reset**. This reads group state, permissions, committed offsets
   and retained bounds; it does not commit offsets.
4. Review the before/after table. The earliest offset is inclusive; the end offset
   is the next position after existing records. Targets must stay within those bounds.
5. Examine the replay upper bounds and up to three sample records. Samples show
   decoded, protected key/value text, at most 512 characters each, through the
   same codec and masking pipeline as the message grid. No original-byte side
   channel is returned. An unavailable sample is not proof of an empty partition
   or permission to commit.
6. Type the exact group ID and choose **Apply reviewed reset**. Read-only mode
   permits preview and blocks apply. Masking applies to sampled records.
7. Inspect every partition result and refresh the group before restarting consumers.

## What the preview establishes

Replay exposure is an offset distance from the proposed position back to the prior
commit, limited by the sampled end offset. With no previous commit it uses the
sampled end. It is an **upper bound**, not a record count: compaction, retention,
aborted transactions and gaps change how many records can actually be reprocessed.
Moving forward can skip data. Later records are outside this snapshot.

Group READ authorization is requested from Kafka; older brokers may report it as
unknown. Topic permissions remain authoritative at dispatch. Preview cannot reserve
permission or prevent another operator from changing offsets.

## Stale plans and partial results

Earliest, end and time selectors resolve to explicit offsets during preview. Those
positions remain frozen: apply does not resolve the selector again. A time with no
retained matching record refuses the preview; it does not silently select the end.
Choosing **Current end** skips records before the reviewed positions, and records
may arrive after the snapshot.

A review expires after two minutes and is tied to its connection and cluster/topic
UUIDs. A replaced topic refuses the reset. Before each
partition is sent, the host checks that the group is still inactive and every
selected committed offset matches the expected baseline, including positions
already acknowledged by this operation. A changed baseline, denied permission or
invalid retained bound stops the remaining work. Kafka does not offer an atomic
compare-and-reset API: a consumer or another operator can still race the final check.

| Result                   | Meaning                                                              | Next action                                                     |
| ------------------------ | -------------------------------------------------------------------- | --------------------------------------------------------------- |
| Acknowledged, verified   | Kafka accepted the position and read-back matched.                   | Refresh before restarting consumers.                            |
| Acknowledged, unverified | Kafka accepted it; read-back failed or differed.                     | Inspect committed offsets; do not resend to retry verification. |
| Rejected                 | Kafka explicitly refused the partition, or dispatch could not begin. | Correct the cause and create another preview.                   |
| Unknown                  | The request may have reached Kafka.                                  | Reconcile the actual committed position before another attempt. |
| Unsent                   | This partition was never dispatched by the plan.                     | Review why execution stopped.                                   |

Cleanup is shown separately from Kafka acceptance and readback. Unresolved cleanup
fences further work through the original owner; reconnect must drain that owner.
An acknowledged operation is not turned into a rejection by a readback or close failure.

The host stops after the first failed, unverified or unresolved-cleanup partition and starts no new
dispatch after 60 seconds. In-flight requests settle within their transport timeout.
Repeating the same plan returns its recorded result; it does not commit again.
Raw logs retain per-partition outcomes. This is not an atomic, all-partition rollback.

## Delete an inactive consumer group

Deletion removes a group's committed offsets; it does not delete its topic messages.
A consumer can recreate the group and then follows its configured offset-reset policy.

1. Stop every consumer. Open the group and choose **Delete group…**.
2. Choose **Review group deletion**. The host reads cluster identity, consumer
   coordination protocol, state, members, delete permission and a complete
   fingerprint of committed offsets, bounded to 4096 partitions. Active groups,
   denied access and unsupported protocols are refused.
3. Review the group and connection, then type **DELETE GROUP** followed by the
   exact group ID as shown. Choose **Delete reviewed group**.
4. Inspect acceptance, readback and cleanup separately. Close the result to refresh
   the inventory. Refreshing is safe; an unknown deletion must not be resent blindly.

The two-minute review is tied to its connection. A fresh check immediately before
admission refuses changed offsets, membership, permissions or identity. Duplicate
apply joins the same attempt. Direct `GROUP_ID_NOT_FOUND` readback verifies absence. Brokers that directly
describe a nonexistent group as `Dead` are verified only after a separate exact
group-offset read confirms that no committed offsets remain. A denied read or
empty inventory does not establish deletion.

Kafka groups have no UUID. A group concurrently deleted and recreated with an
identical baseline cannot be distinguished. Keep all consumers stopped throughout
review and reconciliation. Kafka rejects a group that becomes nonempty before its
delete request is processed. These operations use the client's supported consumer
coordination decoder, including empty groups that retain offsets with an empty
protocol type. Connect worker coordination is unsupported.
