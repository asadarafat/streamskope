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
