# Replay behavior and limits

The [copy/replay guide](../guide/record-replay.md) owns selection, transformation,
review and repair-history procedures. For a failed-record recovery decision, use
[Under the Skope](../skope/replay-record.md).

## Fidelity and destination behavior

| Field                  | Behavior                                                                                                                                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Key/value              | Original complete bytes, including empty versus null, unless explicitly transformed.                                                                                                                   |
| Tombstone              | A null value remains null, including when text or structured transformation is selected.                                                                                                               |
| Headers                | Original order and duplicate names are retained; removals match complete names and additions append in order.                                                                                          |
| Timestamp              | Original timestamp is requested when known. A broker configured for LogAppendTime can replace it; byte read-back does not verify timestamp policy. Unknown timestamps use the producer's current time. |
| Topic/partition/offset | All records go to the reviewed topic/partition and receive new offsets. The preview retains each original identity.                                                                                    |
| Encoding/schema        | Framed cross-profile records require explicit destination writers. IDs are scoped to their Registry; matching numbers do not establish identity.                                                       |

Text replacement is literal, replaces all matches and rejects invalid UTF-8. It
does not evaluate scripts, regexes or schema rules. Transformations must remain
within the same record/batch bounds. Review output before applying: editing text
inside a serialized schema format can invalidate that format.

## Structured destination checks

The structured admission deadline is 30 seconds; each send's fresh writer check
has a 15-second deadline within the overall replay limit. Destination validation
can reject unsupported server/schema formats. This does not claim that every
Registry implementation supports every reference type.

Saved-profile destinations open a separate connection. They do not switch or stop
the current source reader. Only already stored connection credentials are used;
no plugin installation, capture resume or trust acquisition is triggered. Prepare
a managed destination before using it here. Temporary connections close on finish,
cancel, expiry, source change and application shutdown.

## Repair journal

Each job contains at most 50 records. The journal holds at most 32 jobs within a
4 MiB plaintext bound; capacity or unavailable protection refuses publication
before sending. Existing history is retained when it cannot be read or updated.
Preserve complete application-data backups, including the encrypted journal.
Older browser builds that cannot read this storage refuse rollback; restore the
verified complete predecessor backup rather than deleting repair history.

Before every send the host records a dispatch intent. It then records the actual
broker outcome before starting another record. An intent without a durable receipt
is **uncertain**, including when the app closes after Kafka accepted the record.
Definitely unsent records are counted separately. A receipt-storage failure stops
further sends and reports journal uncertainty while preserving any acknowledgement
in the immediate response. Reopening history never sends or retries a record.

## Freshness and outcomes

The host pins source connection generation, saved-profile revision, destination
cluster ID, topic ID and partition count. A changed input stops dispatch. The
destination identity is rechecked before each send, but Kafka has no atomic
compare-and-produce API: a concurrent administrative change can still race it.
Old brokers that cannot return stable cluster/topic identities cannot use replay.

At most one replay runs at once. The host starts no new send after 60 seconds.
Cancellation waits for the current transport request; it does not undo an accepted
record. The result reports every acknowledgement, rejection, unknown result and
unsent record. An acknowledgement survives failed read-back or client cleanup.

If destination cleanup is unavailable, new replay reviews and unstarted plans
are blocked for that host session. Existing outcomes remain available. Inspect
the destination and reconcile receipts, then restart the app or browser host
before reviewing another replay. Reconnecting a profile or refreshing the browser
does not clear an unconfirmed cleanup.

An unknown result may already exist at the destination. Repeating the **same plan**
returns its recorded outcome without resending. Creating a **new plan** can duplicate
previous writes; reconcile receipts and consumer effects first. This is bounded
at-least-once recovery, not an exactly-once transaction or an automatic retry queue.
Copying back to the same topic can also create a consumer processing loop.

Read-only mode blocks publication at the host. Masking blocks replay review so an
original-byte copy cannot bypass disclosure controls. See
[security and permissions](../guide/security.md) and [offset recovery](../guide/offset-recovery.md).
