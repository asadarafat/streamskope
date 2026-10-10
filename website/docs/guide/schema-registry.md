# Inspect a schema

Connect a Schema Registry and read a subject's schema version. You need the
Registry endpoint and permission to read its subjects, as well as a Kafka profile.

Browsing a definition is separate from decoding a record. Use
[Message details → Decoded](structured-events.md) for explicit JSON, Confluent
Avro or Protobuf inspection with original bytes preserved. See the
[compatibility matrix](../start/compatibility.md) for supported framing.

## Connect the service

1. Open **Connection Profiles** and edit your Kafka profile.
2. Enter the **Schema Registry URL**. Choose **Schema Registry authentication**:
   **No HTTP authorization**, **Profile OAuth bearer token**, **HTTP Basic**,
   **Bearer token** or **Separate OAuth client**. Basic uses its own username/password;
   separate OAuth uses its own token endpoint, client ID, secret and optional scope.
3. Choose **Schema Registry certificate trust**: system certificate authorities,
   broker certificate trust, or a separate CA/truststore. If required, configure
   the Registry's own mutual TLS certificate and private key.
4. Test the profile, save and connect it.
5. Open **Schema Registry** in the left navigation.

**You should see:** the subjects you can access. The included AIO fixture configures
Karapace, a Confluent-compatible Registry, in its local profile and provides a
`test-value` Avro subject.

Registry access is separate from broker access. If Kafka connects but subjects
are unavailable, check the Registry URL, authentication and **Raw logs**.
Only use the profile's OAuth token when the Registry accepts it.
The Registry's selected trust and client identity also apply to its separate OAuth
token request. It never borrows the broker's client private key. If its token
endpoint and Registry use different issuing CAs, include both in the selected
service bundle. See [service trust selection](tls-trust.md#which-trust-settings-apply).

## Inspect and evolve a schema

1. Search for a subject and select it. With the local fixture, use `test-value`.
2. Select a version and read the schema.
3. Check the version and subject name before using the schema to interpret a record.

**You should have:** a schema definition and its version within the selected
subject. A subject's naming strategy determines how it relates to topic records.

To create a new subject, choose **Create subject** and enter its definition.
To evolve an existing subject, select its latest version and choose **Evolve selected
schema**. Historical selections cannot authorize a change to a newer writer.

1. Edit **Proposed schema** and **Pinned references** (a JSON array of name, subject
   and exact version). The complete draft is limited to 128 KiB UTF-8.
2. Choose **Review schema change**. Inspect the structural/source diff, exact prior
   writer, current effective policy, connection and expiry. Editing any draft field
   clears the review and confirmation.
3. Type the exact subject to confirm **Register reviewed schema**. The host rechecks
   writer, references, relevant history and policy, and checks compatibility again
   before sending one registration attempt. It rejects a changed baseline.
4. Read the acknowledged ID and verification result. Choose **Refresh subject** to
   inspect the actual registered version; then **Author record** to validate and
   explicitly publish a value using that exact writer.

A review expires after two minutes and is bound to its connection. Registry reads
have a 15-second deadline. Review snapshots are limited to 512 KiB, 32 schema nodes
and eight reference levels; oversized or unreadable state blocks the change.
Transitive compatibility snapshots visible history and checks every captured
version, rather than reducing the policy to a latest-only check. Missing or denied
compatibility APIs block the review. Advanced aliases, metadata/rules and other
non-basic configuration are outside reviewed-change support and are refused.

The displayed policy comes from the Registry's subject configuration, falling back
to its global configuration. StreamSkope checks that policy; it does not edit it.
A new subject reports **no existing writer**; this is not proof of valid schema
syntax. The Registry validates the new definition during registration.

Registration is one reviewed attempt, with repeated calls to the same review ID
returning its retained result. The Registry API provides no compare-and-swap:
another client can still change state between the final recheck and the write.
Readback confirms the acknowledged writer ID is latest at that moment, not an
exclusive lock or proof of deployed consumer compatibility.

References name another subject and an exact version; their supported formats
depend on the Registry. The isolated authoring fixture pins Karapace 6.1.0, including Avro and
Protobuf references ([vendor support](https://aiven.io/docs/products/kafka/karapace)).
JSON Schema validation resolves declared references in the worker; that does not
claim JSON reference registration support in this Registry vendor. The AIO
development fixture remains Karapace 5.0.3, which supports Protobuf references
and Avro compatibility but cannot register Avro reference graphs.

If registration or deletion is acknowledged but the follow-up read fails, Activity
retains the successful write and warns that refresh is unavailable. Refresh the
inventory; do not repeat the write to refresh it. If no acknowledgement arrived,
inspect the subject and its versions before another attempt: a lost response does
not prove that nothing changed.

Soft deletion hides a subject/version; permanent deletion removes it after the
soft-delete step, including when it was already soft-deleted. Dependent schemas
can prevent deletion. Resolve those dependencies deliberately before retrying;
StreamSkope never cascades deletion through other subjects.

Registration does not rewrite existing Kafka messages. Review the exact subject
and version before deletion; permanent deletion can break readers that need it.

## Next: read the record

[Inspect a message's value and headers →](messages.md#inspect-a-record)

## Compare versions and follow references

Select a subject and version. **Version history** opens a specific version; it does
not silently move to latest. Select **Compare from version**, then **Compare schema
versions** to see additions, removals and changes against the displayed version.
The comparison names both subject/version pairs and schema IDs. Avro and JSON
Schema compare structure by default; Protobuf uses source lines. **Compare source
text** also exposes formatting changes. This view does not replace the Registry's
compatibility check or prove compatibility for deployed consumers.

**Show reference tree** reads the references declared by that exact version and
follows their pinned subject/version identities. Follow **Inspect referenced
version** to open a dependency. A cycle is labelled and not traversed again;
an unavailable edge may mean a missing version or denied access. A graph can stop
at 32 nodes, 64 edges, eight levels, 1 MiB of schema data or a 15-second deadline.
Limits and failures remain visible rather than appearing as an empty dependency
list. These are schema relationships, not evidence of events flowing through
producers, topics or consumers.

For a proposed change, [Relationships → Potential schema impact](relationships.md#before-changing-a-schema)
adds bounded reverse references and known topic, consumer-group and connector
links for the selected exact version. Its source coverage and inferred links stay
visible. It can miss historical versions, externally configured clients and unseen
records; an empty impact view is not permission to change or delete a schema.

## Generate samples

Select an exact schema version, then **Generate samples** to preview seeded,
schema-valid records without writing to Kafka. Publishing uses a separate
destination review and confirmation. See [supported schema forms, batch limits
and cancellation outcomes](structured-events.md#generate-schema-valid-samples).

## Generate a validating client

For a supported JSON Schema version, use **Generate JavaScript client** to create a Node 24 record codec and producer helper with pinned Registry framing and provenance. See [Generate a schema client](schema-clients.md) for supported keywords, runtime installation and limits.

## Author a record

Select an exact registered schema version and choose **Author record**. Edit
**Record payload JSON**, or use **Start from one sample** as an editable starting
point. For Protobuf, choose the fully qualified message type; leaving it empty
selects the first top-level writer message.

1. Choose **Validate payload**. Validation reads the registered writer and its
   pinned references, then compiles and encodes the value in an isolated worker.
   It does not register a schema or publish a Kafka record.
2. Review the **Encoded projection**, writer schema ID and encoding. Avro and
   Protobuf use their Confluent wire headers. JSON Schema produces UTF-8 JSON
   without a wire header. JSON `null` is an encoded value, not a Kafka tombstone.
3. Enter the destination topic, partition and maximum publication rate, then
   choose **Review batch destination**. Type the exact topic name to confirm
   **Publish reviewed batch**. Review acknowledged, rejected, uncertain and
   unsent counts before considering another attempt.

Editing the payload invalidates validation and destination review. Changing the
destination invalidates its review. Reconnecting or locking clears the current
validation. If the selected writer ID changed after deletion/recreation, reload
the subject before authoring. Schema IDs from another Registry cannot be assumed
to identify the same writer; publication currently uses the active connection.

Values are limited to 16 KiB UTF-8 and 16 KiB encoded bytes, with bounded depth,
reference resolution and worker time. Keys are null and headers empty in this
authoring workflow. Use decimal strings for 64-bit fields; unsafe integer JSON
numbers are rejected. Avro uses named union branches and byte strings, with
logical types expressed through underlying storage values. Protobuf uses Base64
bytes and declared enum names, and rejects unknown fields or conflicting oneof
members. JSON Schema validation supports draft-07 with declared references and
no external URL fetching. Unsupported schemas produce explicit validation errors
rather than substituting an unvalidated value.
