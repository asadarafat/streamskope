# Decode a structured record

StreamSkope prepares one key/value interpretation for every record before it reaches
the workbench. The grid, inspector, filters, live rules, comparison, correlation
tracing and export share that interpretation. Each record keeps its own writer
schema identity and decoding outcome, including mixed-schema topics.

For the distinction between a position, bytes and a projection, read
[Records, decoding and original bytes](../concepts/records.md).

## Choose how records are interpreted

1. For Avro or Protobuf, configure the profile's
   [Schema Registry endpoint and authentication](schema-registry.md#connect-the-service).
2. Disconnect Kafka and open **Preferences → Records**.
3. Choose **Default key encoding** and **Default value encoding**, then select
   **Save record encodings**. **Detect encoding** is the default for both.
4. Reconnect, load messages and select a record. **Message details → Decoded**
   shows its captured interpretation immediately. Use **Record part** to switch
   between key and value.

These are host-wide Kafka preferences, saved across restarts when preference
storage is durable. They apply to every profile and topic on that host; they are
not per-topic settings. Changing them does not reinterpret an old retained sample.
Reload records after saving. The source CLI uses its own configuration rather
than inheriting desktop preferences.

Check the encoding and writer identity in **Decoded**, and use **Original** to
inspect retained bytes and ordered headers. For the detection rules, supported
formats and representation of each encoding, see the
[record format reference](../reference/record-formats.md#detection-and-formats).
A failed interpretation leaves its retained original bytes unchanged.

## When decoding is unavailable

- **Missing schema:** check the configured Registry, its permissions and the
  record's schema ID. A schema ID is meaningful only within its Registry.
- **Malformed input or wrong format:** check the producer's serializer and framing.
  An explicit override applies to subsequent reads. A failed record does not
  prevent inspecting other records.
- **Missing, cyclic or ambiguous reference:** repair the Registry definitions.
  Imports never fetch arbitrary URLs or local files. Common bundled Google
  Protobuf definitions are supported; other imports require declared references.
- **Incomplete original:** a truncated preview cannot reconstruct the source.
- **Masking enabled:** inspect the protected projection. Original bytes are
  withheld, and direct original-byte decode requests remain blocked by the host.
- **Disconnected or stale record:** reconnect and reload from the intended
  cluster before relying on a new interpretation.

A decoding error is not a tombstone or a negative field match. JSON field filters
and tracing count records whose fields cannot be evaluated as unavailable. Text
and byte modes have no JSON fields. Tombstones, zero bytes and the JSON value
`null` remain distinct.

## Bounds

See [decoding bounds](../reference/record-formats.md#decoding-bounds) for input,
schema, worker and projection limits. A bound failure is unavailable evidence,
not an empty field. [Inspect original evidence](messages.md#inspect-a-record)
when it is retained and disclosure is permitted.

## Compare two records

In **Message details → Compare**, select **Pin as baseline**, then select another
record. The baseline is a snapshot: rolling-window eviction and topic navigation
in the same connection do not change it. Changing connection, encoding or
protection settings clears loaded content; saved positions can be reloaded explicitly.

Choose **Value**, **Key** or **Ordered headers**, then **Compare records**. The
**Protected projection** uses the same interpretation and protection as the grid;
it does not decode each record again. Different writer schemas can be compared
without selecting one encoding for both records. **Original bytes (Base64)**
compares exact bytes only when both complete original envelopes are available.

A Kafka null remains distinct from encoded JSON null; missing properties differ
from explicit null values. Arrays and duplicate headers retain their order.
Comparison never writes to Kafka. Masked fields cannot establish equality of the
underlying secret values. A decoding error or unavailable original is reported
instead of treating missing evidence as equal.

Read the partial-result notice before treating the comparison as complete.
The [comparison bounds](../reference/record-formats.md#comparison-bounds) specify
what can be omitted. To decide what a difference means, use
[Understand a changed payload](../skope/changed-payload.md).

## Generate schema-valid samples

In **Schema Registry**, select an exact subject version and **Generate samples**.
Set **Seed**, **Sample count** and (for Protobuf) an optional fully qualified
**Protobuf message type**. **Generate preview** produces the same records for the
same schema, seed and application version. It performs no Kafka writes and does
not register schemas. Keys are null and headers empty.

| Schema      | Generated representation                          | Supported constraints and limits                                                                                                                 |
| ----------- | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Avro        | Confluent writer-ID framing                       | Records, declared references, defaults, enums, unions, arrays, maps, fixed and primitives; logical types are explicitly unsupported              |
| Protobuf    | Confluent writer ID plus selected message indexes | Declared imports, nested messages, defaults, enums, oneof, repeated and map fields                                                               |
| JSON Schema | Plain UTF-8 JSON, without a Confluent header      | Draft-07 types, declared references/JSON Pointer fragments, defaults, enum/const, unions, numeric/string/array bounds; final validation required |

Unsupported constraints fail explicitly. For example, JSON Schema regex patterns,
formats, conditional schemas and `allOf` are unsupported; reference URI resolution
requires an exact declared name. No arbitrary URL or file is read. Recursive
schemas may exceed the depth limit. Generation never substitutes an unchecked
payload when constraints cannot be satisfied. Logical Avro types can still be
inspected in **Decoded**, but cannot generate samples.

JSON sample schema numbers must be finite, and integer constraints and defaults
must fit the JavaScript safe integer range. Larger integers fail explicitly;
sample generation never rounds them into a different constraint or default.

Generation shares the isolated decoder's two-worker limit, three-second parsing
budget and bounded Registry lookup. A request accepts at most 50 records, 16 KiB
each and 512 KiB total; generated structures stop at eight levels and 128 fields
per object. JSON arrays have at most eight generated items. A failing constraint
or limit returns an error, not a partial batch.

### Review and publish a batch

Choose a **Destination topic**, **Destination partition** and **Maximum records
per second**, then **Review batch destination**. Review checks the destination
without producing. Read the connection, exact topic, partition, count and rate;
type the topic to confirm and select **Publish reviewed batch**. Changing the
destination invalidates the review. Read-only mode blocks publishing at the host.

Publishing is sequential, at most ten records per second. Reviews expire after
two minutes and are tied to one connection. Duplicate confirmation of the same
retained review returns its existing result; it never resends the batch. This is
not broker-level exactly-once delivery across application restarts.

**Cancel remaining records** stops future sends after an in-flight record settles.
Closing the workspace or changing connections also cancels the remaining batch.
No new send starts after 60 seconds; an in-flight request may settle later. The
result and Activity entry separately count acknowledged, rejected, uncertain and
unsent records. The first rejected or uncertain write stops publication. Inspect
Kafka before creating a new review after uncertainty: do not assume cancellation
undoes a record already dispatched.

## Trace a correlation ID across topics

Open a topic's **Messages** workspace and select **Trace correlation**. Enter one
to eight explicit **Trace topics** and an **Exact correlation value**, then choose:

| Correlation source | Selector                                       | Matching behavior                                                                                                      |
| ------------------ | ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Header (UTF-8)     | Case-sensitive header name                     | Any duplicate header with that name and exact UTF-8 value can match                                                    |
| Key                | Entire interpreted key                         | Exact, case-sensitive equality using the saved key encoding; a null key does not match                                 |
| Payload field      | JSON Pointer such as `/metadata/correlationId` | Compare a scalar in the same protected JSON projection used by filters; each record can have a different writer schema |

JSON Pointer escapes `/` as `~1` and `~` as `~0`; an empty pointer selects a root
scalar. Unsafe JSON integers use exact decimal strings, so a large integer ID
never rounds to a neighboring value. Strings and boolean text can also match;
null, missing paths and compound values do not. Avro unions retain their decoded
branch wrappers. Avro/Protobuf use the active profile's Registry and the saved encoding
preferences above. **Detect encoding** supports mixed JSON, Avro and Protobuf
records in one trace. Tracing has no separate encoding setting that can disagree
with the grid.

Use **Last 2 minutes** or an explicit time interval, then **Start trace**. Time is
start inclusive and end exclusive, based on Kafka record timestamps. The result
keeps each topic, partition, offset and timestamp and a bounded preview. Records
with the same value at different offsets remain separate. Topic scan order and
matching IDs do not establish causality or a global event order.

Read the coverage for **every** selected topic:

- **searched:** the captured retained offset ranges were reached and every returned
  candidate was evaluated. Zero matches applies only to this range and selector.
- **partial:** a record was unreadable or a time, byte, count or cancellation limit
  stopped evaluation. Missing schemas and malformed payloads remain unavailable,
  never evidence of a non-match.
- **denied / failed:** access or another read failure prevented qualification.
- **not-searched:** an earlier bound or cancellation prevented this topic's start.

Check [trace bounds](../reference/record-formats.md#trace-bounds) when coverage is
partial. Tracing is read-only and does not commit consumer offsets. It applies
host masking before matching; a protected field cannot reveal its original value.

**Cancel trace** returns partial evidence after cleanup. Editing the request clears
old results. Retention and compaction still apply: timestamp lookup cannot restore
deleted data or prove that matching events never existed outside the searched range.
