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
4. Leave transformations unchanged for a faithful byte copy within the active
   connection. Framed records sent to another profile require explicit structured
   translation below. Optional byte/text operations
   replace the key, remove every matching header name, append ordered headers or
   replace literal text in UTF-8 values. Header input uses a JSON array such as
   `[{"name":"origin","value":"recovery"}]`; a null header value stays null.
5. Choose **Preview replay**. No records are produced. Inspect each source identity
   and its exact before/after Base64 bytes, the destination name, cluster ID, topic
   identity and partition. Structured output also shows the verified source and
   destination writers. Review expires after two minutes.
6. Type the displayed destination confirmation and choose **Apply reviewed replay**.
7. Inspect each receipt, read-back result and the unsent count before another attempt.
   **Cancel replay** prevents further sends after the current request settles.

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

## Structured edits and destination writers

Enable **Transform structured key** or **Transform structured value** for the
fields to decode, edit and encode. **Automatic detection** uses complete original
bytes and the source profile's Registry; manual JSON, Avro and Protobuf choices
must agree with the actual writer. Renderer previews never become replay input.
Malformed records, unavailable schemas and unmapped writers refuse review.

1. Select the records before enabling the field. Use **Refresh value source
   writers** or **Refresh key source writers** after changing the selection.
2. Enter up to 16 JSON Pointer edits. `[]` translates without edits. A set replaces
   or adds an object field under an existing parent; remove deletes an existing
   object field or array element. Array sets replace existing positions. Missing
   parents, invalid pointers and prototype paths are refused. For example:

    ```json
    [
        { "op": "set", "path": "/status", "json": "\"fixed\"" },
        { "op": "remove", "path": "/obsolete" }
    ]
    ```

    An empty pointer sets the whole projection. Use decimal strings for exact large
    integers; unsafe numeric edits are rejected. Null keys and tombstones remain
    null. Structured and byte/text replacement cannot edit the same field.

3. Map every source format/writer ID to an **existing destination subject and
   explicit version**. JSON may remain plain JSON or use a registered JSON, Avro
   or Protobuf writer. Binary source fields require a registered destination
   writer. Enter the Protobuf message name when needed; blank selects the first
   declared message. A reference graph uses destination subject versions and
   declared import names, never source Registry IDs.
4. For a missing writer, first connect to the destination profile. Use
   [Schema Registry's reviewed creation/evolution workflow](schema-registry.md#inspect-and-evolve-a-schema)
   to register dependencies and then the root with pinned references. Inspect its
   acknowledged registration and fresh readback, return to the source profile,
   and select that subject/version in replay. Replay preview does not register
   schemas, change compatibility policy or modify references.
5. Inspect **Verified writer mappings** and every exact output. The host validates
   the transformed projection with the shared bounded authoring worker. It pins
   the destination ID, definition, references and graph fingerprint. Changed,
   deleted or inaccessible writers stop further dispatch before the next send,
   while preserving previous receipts and the remaining unsent count.

The structured admission deadline is 30 seconds; each send's fresh writer check
has a 15-second deadline within the overall replay limit. Destination validation
can reject unsupported server/schema formats. This does not claim that every
Registry implementation supports every reference type.

Saved-profile destinations open a separate connection. They do not switch or stop
the current source reader. Only already stored connection credentials are used;
no plugin installation, capture resume or trust acquisition is triggered. Prepare
a managed destination before using it here. Temporary connections close on finish,
cancel, expiry, source change and application shutdown.

## Freshness, cancellation and duplicate risk

### Protected repair history

Open **Repair history** in Connection Profiles or the message toolbar to inspect jobs and ordered broker
receipts after navigation or restart. Installed desktop storage uses the operating
system credential service; the browser host uses its unlocked vault. The
development host reports **Session only**, which does not survive a host restart.
The history view contains destination metadata and receipts, without record values
or connection credentials. Exact reviewed inputs remain in protected host storage.

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

### Continue an interrupted attempt

1. Connect using current credentials, then open **Repair history → Recovery controls**.
2. Choose the active connection or a current saved destination. It must identify the
   original cluster, topic and partition count; a replacement topic is refused.
3. Choose **Review definitely unsent records**. The new linked attempt contains
   only the frozen suffix never dispatched by the parent. Acknowledged, rejected
   and uncertain records are skipped; their original outcomes remain unchanged.
4. Inspect the exact continuation bytes, skipped counts and destination. Type the
   displayed confirmation and choose **Apply reviewed continuation**.
5. Refresh history and inspect the child attempt's receipts before continuing again.

Each parent can reserve one child, durably before sending. Duplicate or stale
reviews cannot create a second child. If the host stops after reserving the child,
recover from that child with a fresh review. Current profile credentials,
permissions, masking and read-only settings apply; credentials are never copied
into the journal. Rejected records are not retried by continuation; investigate
their failure before a separate deliberately selected replay. Structured
continuation reuses the exact frozen output and writer evidence, without requiring
the old source Registry or applying transformations again. It still rechecks the
current destination writer. Legacy cross-profile framed jobs without writer
evidence cannot continue; preserve and reconcile their history before considering
a separate translated replay.

### Inspect uncertainty and archive known chains

In **Recovery controls**, enter the record's one-based index in that attempt and a
destination offset to inspect. The bounded read checks actual cluster/topic
identity and compares complete key, value and ordered header bytes. History keeps
the timestamped result: **equivalent**, **different**, **not-observed** or
**unavailable**, plus cleanup status. Absence requires complete single-offset
coverage; permission failures, masking, replacement topics and incomplete reads
are unavailable evidence. Each job retains at most 128 observations.

Equivalent bytes are not proof that this attempt produced that record. The check
does not change acknowledgements or clear uncertainty, and it does not justify an
automatic retry. No reader or isolated destination is released until its owned
cleanup is confirmed; unresolved cleanup blocks further recovery.

**Archive confirmed chain** is available only for an inactive root and descendants
with no pending or unknown dispatch. Back up application data first, then confirm
the exact root ID. Archiving removes all linked attempts, receipts and any unsent
records from history; a changed revision refuses the operation. An uncertain chain
cannot be deleted through this control, including after an equivalent observation.

The first explicit journal mutation upgrades legacy format 1 or 2 to format 3 and
keeps its exact encrypted predecessor at
`history/kafka-repair-jobs.json.pre-repair-v1` or
`history/kafka-repair-jobs.json.pre-repair-v2`. Existing predecessors remain intact.
Listing legacy history does not rewrite it. Older hosts refuse the new format;
rollback requires a verified complete predecessor backup, including vault metadata
and credentials. Restoring only the journal can lose newer receipts or mismatch
its protection key. Preserve changed data during complete restoration.

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
[security and permissions](security.md) and [offset recovery](offset-recovery.md).
