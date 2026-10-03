# Copy or replay records

Replay produces new records from a frozen selection. It does not delete source
records or commit consumer offsets. Start with a [bounded read](messages.md), stop
to inspect the records you intend to copy, and confirm that replay is appropriate
for the destination consumers.

1. In the topic's message toolbar choose **Replay…**. The dialog freezes up to the
   first 50 displayed records, putting the selected record first.
2. Select the exact source records. Records without complete original bytes cannot
   be replayed. The limit is 16 KiB per record and 512 KiB total, including headers.
3. Choose **Active connection** or a saved destination profile, then the existing
   topic and one partition. Set the rate, from 1 to 10 records per second.
4. Leave transformations unchanged for a faithful byte copy. Optional operations
   replace the key, remove every matching header name, append ordered headers or
   replace literal text in UTF-8 values. Header input uses a JSON array such as
   `[{"name":"origin","value":"recovery"}]`; a null header value stays null.
5. Choose **Preview replay**. No records are produced. Inspect each source identity
   and its exact before/after Base64 bytes, the destination name, cluster ID, topic
   identity and partition. Review expires after two minutes.
6. Type the displayed destination confirmation and choose **Apply reviewed replay**.
7. Inspect each receipt, read-back result and the unsent count before another attempt.
   **Cancel replay** prevents further sends after the current request settles.

## Fidelity and destination behavior

| Field                  | Behavior                                                                                                                                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Key/value              | Original complete bytes, including empty versus null, unless explicitly transformed.                                                                                                                   |
| Tombstone              | A null value remains null, including when a text transformation is selected.                                                                                                                           |
| Headers                | Original order and duplicate names are retained; removals match complete names and additions append in order.                                                                                          |
| Timestamp              | Original timestamp is requested when known. A broker configured for LogAppendTime can replace it; byte read-back does not verify timestamp policy. Unknown timestamps use the producer's current time. |
| Topic/partition/offset | All records go to the reviewed topic/partition and receive new offsets. The preview retains each original identity.                                                                                    |
| Encoding/schema        | A byte copy does not translate Confluent schema IDs or Registry subjects. Ensure the target Registry understands the original IDs before cross-cluster copying.                                        |

Text replacement is literal, replaces all matches and rejects invalid UTF-8. It
does not evaluate scripts, regexes or schema rules. Transformations must remain
within the same record/batch bounds. Review output before applying: editing text
inside a serialized schema format can invalidate that format.

Saved-profile destinations open a separate connection. They do not switch or stop
the current source reader. Only already stored connection credentials are used;
no plugin installation, capture resume or trust acquisition is triggered. Prepare
a managed destination before using it here. Temporary connections close on finish,
cancel, expiry, source change and application shutdown.

## Freshness, cancellation and duplicate risk

The host pins source connection generation, saved-profile revision, destination
cluster ID, topic ID and partition count. A changed input stops dispatch. The
destination identity is rechecked before each send, but Kafka has no atomic
compare-and-produce API: a concurrent administrative change can still race it.
Old brokers that cannot return stable cluster/topic identities cannot use replay.

At most one replay runs at once. The host starts no new send after 60 seconds.
Cancellation waits for the current transport request; it does not undo an accepted
record. The result reports every acknowledgement, rejection, unknown result and
unsent record. An acknowledgement survives failed read-back or client cleanup.

An unknown result may already exist at the destination. Repeating the **same plan**
returns its recorded outcome without resending. Creating a **new plan** can duplicate
previous writes; reconcile receipts and consumer effects first. This is bounded
at-least-once recovery, not an exactly-once transaction or an automatic retry queue.
Copying back to the same topic can also create a consumer processing loop.

Read-only mode blocks publication at the host. Masking blocks replay review so an
original-byte copy cannot bypass disclosure controls. See
[security and permissions](security.md) and [offset recovery](offset-recovery.md).
