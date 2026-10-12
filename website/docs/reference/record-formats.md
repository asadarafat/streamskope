# Record formats and decoding limits

Use [Decode and compare records](../guide/structured-events.md) for the operating
steps, or [Records and original bytes](../concepts/records.md) for the model behind
the views. These bounds describe Kafka record handling.

## Detection and formats

Detection examines each key and value separately. A Confluent magic byte and
writer schema ID select the type declared by the configured Registry. Recognized
JSON is parsed as JSON; other valid UTF-8 is text. Malformed JSON objects or
arrays, invalid UTF-8, unreadable schemas and bad framing remain explicit errors.
Ambiguous non-JSON text stays text until an explicit JSON override is selected.
Detection does not guess from a topic or subject name or silently try a different structured codec
after a decoding failure. Choose an explicit encoding to override detection;
**Bytes (Base64)** is available for binary or unsupported data.

| Encoding           | Supported input                                                                           | Representation                                                                                                                                                |
| ------------------ | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| UTF-8 text         | Strict UTF-8; no Registry required                                                        | Text, without JSON interpretation                                                                                                                             |
| UTF-8 JSON         | A complete JSON document; no Registry required                                            | Unsafe integer values are exact decimal strings; JSON null remains distinct from a Kafka tombstone                                                            |
| Confluent Avro     | Magic byte 0, four-byte schema ID and Avro binary payload; declared Registry references   | Avro JSON, including union branch wrappers; longs use decimal strings, bytes/fixed use Avro JSON byte strings, logical types retain underlying storage values |
| Confluent Protobuf | Magic byte 0, four-byte schema ID, message indexes and Protobuf payload; declared imports | Schema field spelling, decimal strings for 64-bit integers and Base64 for bytes; absent fields stay absent                                                    |
| Bytes (Base64)     | Any retained complete bytes                                                               | Base64 text, without schema lookup or JSON interpretation                                                                                                     |

This implements the [Confluent schema-ID wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format).
Raw Avro/Protobuf, Avro container files, Confluent JSON Schema framing, GUID-header
framing and custom codecs are unsupported. Protobuf unknown fields remain in the
original bytes but are omitted from the decoded projection. Schema relationships
and Registry support still depend on the vendor and version.

## Decoding bounds

One decode accepts at most 256 KiB of original bytes. Each schema is limited to
256 KiB, with at most 32 schema entries, eight reference levels and 1 MiB of
resolved schema input. Schema caches are bounded to 32 entries and 2 MiB and
cleared when the connection changes. Decoded JSON is limited to 256 Ki characters,
20,000 nodes and 32 levels.

At most two worker decodes run concurrently. Structured binary parsing runs in an
isolated worker with bounded heap and a three-second deadline. A limit failure
leaves original bytes unchanged. Record, batch and retained-history limits still
apply after adding the decoded representation.

## Comparison bounds

The table labels additions, removals and changes using JSON Pointer paths;
individual cells show at most 512 characters. Comparison stops at 500 changes,
20,000 visited nodes, depth 32 or 512 KiB of text per input and labels a partial
result. **No differences** applies only to the selected representation, not to the
record's timestamp, offset or other unselected fields.

## Trace bounds

Each topic admits at most 1,000 candidates; the whole trace retains at most 200
matches, evaluates at most 32 MiB and has a 30-second deadline. The underlying
finite reader also has its per-topic fetch/scan limits. Streams are closed on
completion, cancellation or connection changes. Cleanup and an in-flight broker
operation may settle after the deadline; no new topic starts afterward. The
trace is independent of the current message reader and does not commit consumer
offsets. It is read-only and applies the same host masking policy before matching.
A protected field cannot reveal its original value through a trace result.
