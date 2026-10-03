# Decode a structured record

Use **Message details → Decoded** when the producer uses JSON, Avro or Protobuf.
The selected encoding describes the writer's bytes; a subject name alone does
not establish the encoding of every record in a topic.

## Decode the key or value

1. Connect the record's Kafka cluster. For Avro or Protobuf, configure the profile's
   [Schema Registry endpoint and authentication](schema-registry.md#connect-the-service).
2. Load a bounded set of messages and select a record.
3. Open **Decoded**, choose **Record part** and **Writer encoding**, then select
   **Decode record**.
4. Check the writer schema ID, Protobuf message type when present, and the
   representation note below the JSON projection.

**You should see:** a decoded projection or an error for that individual record.
The **Original** tab keeps the unchanged key, value and ordered headers. Decoding
does not register a schema, produce a message or change the message export format.
It does not change table filters, live-rule evaluation or the UTF-8 **Raw** view.

| Encoding           | Supported input                                                                           | Representation                                                                                                                                                |
| ------------------ | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| UTF-8 JSON         | A complete JSON document; no Registry required                                            | Unsafe integer values are exact decimal strings; JSON null remains distinct from a Kafka null payload                                                         |
| Confluent Avro     | Magic byte 0, four-byte schema ID and Avro binary payload; declared Registry references   | Avro JSON, including union branch wrappers; longs use decimal strings, bytes/fixed use Avro JSON byte strings, logical types retain underlying storage values |
| Confluent Protobuf | Magic byte 0, four-byte schema ID, message indexes and Protobuf payload; declared imports | Schema field spelling, decimal strings for 64-bit integers and Base64 for bytes; absent fields stay absent                                                    |

This implements the [Confluent schema-ID wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format).
Raw Avro/Protobuf, Avro container files, Confluent JSON Schema framing, GUID-header
framing and custom codecs are unsupported. Protobuf unknown fields remain in the
original bytes but are omitted from the decoded projection. Schema relationships
and Registry support still depend on the vendor and version.

## When decoding is unavailable

- **Missing schema:** check the configured Registry, its permissions and the
  record's schema ID. StreamSkope does not guess a schema from the topic name.
- **Malformed input or wrong format:** check the producer's serializer and framing.
  A failed record does not prevent inspecting other records.
- **Missing, cyclic or ambiguous reference:** repair the Registry definitions.
  Imports never fetch arbitrary URLs or local files. Common bundled Google
  Protobuf definitions are supported; other imports require declared references.
- **Incomplete original:** a truncated preview cannot reconstruct the source.
- **Masking enabled:** original-byte decoding is blocked by the host, including
  direct requests. Continue inspecting the masked retained view.
- **Disconnected or stale record:** reconnect and reload from the intended
  cluster before decoding. Schema IDs belong to their Registry.

## Bounds

One decode accepts at most 256 KiB of original bytes. Each schema is limited to
256 KiB, with at most 32 schema entries, eight reference levels and 1 MiB of
resolved schema input. The connection's schema cache has at most 32 entries and
2 MiB and is cleared when the connection changes. Decoded JSON is limited to
256 Ki characters, 20,000 nodes and 32 levels.

At most two decodes run concurrently. Parsing runs in an isolated worker with
bounded heap and a three-second deadline; the entire lookup/decode request has a
15-second deadline. A limit failure leaves original bytes unchanged.

[Inspect original record evidence →](messages.md#inspect-a-record)

## Compare two records

In **Message details → Compare**, select **Pin as baseline**, then select another
record in the same topic. The baseline is a snapshot: rolling-window eviction does
not change it. Changing topic, connection or protection settings clears it.

Choose **Value**, **Key** or **Ordered headers**, then **Compare records**. Original
bytes compare the exact Base64 representation. JSON, Avro and Protobuf compare the
decoded projection using the active Registry. A Kafka null remains distinct from
an encoded JSON null; missing properties differ from explicit null values. Arrays
and duplicate headers retain their order. Comparison never writes to Kafka.

Both records require complete, unmasked original bytes. A retained preview cannot
prove equality. The table labels additions, removals and changes using JSON Pointer
paths; individual cells show at most 512 characters. Comparison stops at 500 changes,
20,000 visited nodes, depth 32 or 512 KiB of text per input and labels a partial
result. **No differences** applies only to the selected representation, not to the
record's timestamp, offset or other unselected fields.

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

| Correlation source | Selector                                                                    | Matching behavior                                                                                         |
| ------------------ | --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Header (UTF-8)     | Case-sensitive header name                                                  | Any duplicate header with that name and exact UTF-8 value can match                                       |
| Key (UTF-8)        | Entire key                                                                  | Exact, case-sensitive UTF-8 equality; a null key does not match                                           |
| Payload field      | JSON Pointer such as `/metadata/correlationId` and explicit writer encoding | Compare scalar text in JSON, Confluent Avro or Protobuf; choose the same encoding for all selected topics |

JSON Pointer escapes `/` as `~1` and `~` as `~0`; an empty pointer selects a root
scalar. Numeric JSON tokens use their exact source text, so a large integer ID
never rounds to a neighboring value. Strings and boolean text can also match;
null, missing paths and compound values do not. Avro unions retain their decoded
branch wrappers. Avro/Protobuf use the active profile's Registry and decoding
rules above. Mixed encodings require separate traces.

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

Each topic admits at most 1,000 candidates; the whole trace retains at most 200
matches, evaluates at most 32 MiB and has a 30-second deadline. The underlying
finite reader also has its per-topic fetch/scan limits. Streams are closed on
completion, cancellation or connection changes. Cleanup and an in-flight broker
operation may settle after the deadline; no new topic starts afterward. The
trace is independent of the current message reader and does not commit consumer
offsets. It is read-only; masking blocks tracing at the host.

**Cancel trace** returns partial evidence after cleanup. Editing the request clears
old results. Retention and compaction still apply: timestamp lookup cannot restore
deleted data or prove that matching events never existed outside the searched range.
