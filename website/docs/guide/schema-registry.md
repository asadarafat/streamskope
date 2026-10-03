# Inspect a schema

Connect a Schema Registry and read a subject's schema version. You need the
Registry endpoint and permission to read its subjects, as well as a Kafka profile.

Browsing a definition is separate from decoding a record. Use
[Message details → Decoded](structured-events.md) for explicit JSON, Confluent
Avro or Protobuf inspection with original bytes preserved. See the
[compatibility matrix](../start/compatibility.md) for supported framing.

## Connect the service

1. Open **Connection Profiles** and edit your Kafka profile.
2. Enter the **Schema Registry URL** and choose **No HTTP authorization** or **Profile OAuth bearer token** authentication. OAuth reuses the Kafka profile's token; HTTP Basic credentials are not supported.
3. Save the profile and connect it.
4. Open **Schema Registry** in the left navigation.

**You should see:** the subjects you can access. The included AIO fixture configures
Karapace, a Confluent-compatible Registry, in its local profile and provides a
`test-value` Avro subject.

Registry access is separate from broker access. If Kafka connects but subjects
are unavailable, check the Registry URL, authentication and **Raw logs**.
Only use the profile's OAuth token when the Registry accepts it.
With a TLS Kafka profile, the Registry uses the same CA bundle as the broker.
Include its issuing CA in that bundle even if Kafka already connects. See
[service trust selection](tls-trust.md#which-trust-settings-apply).

## Inspect and evolve a schema

1. Search for a subject and select it. With the local fixture, use `test-value`.
2. Select a version and read the schema.
3. Check the version and subject name before using the schema to interpret a record.

**You should have:** a schema definition and its version within the selected
subject. A subject's naming strategy determines how it relates to topic records.

If you need to change a schema:

1. Prepare the proposed schema for that subject.
2. Check compatibility and review the result for this exact draft. Changing the schema, type, subject or references requires a new check.
3. Register the new version only when the change is appropriate for its readers.
4. Reopen the subject and verify the newly registered version.

The displayed policy comes from the Registry's subject configuration, falling back
to its global configuration. StreamSkope checks that policy; it does not edit it.
A new subject reports **no registered version**, not compatibility with an existing
schema. Registration checks compatibility again before sending the write.

References name another subject and an exact version; their supported formats
depend on the Registry. The Karapace 5.0.3 fixture qualifies Protobuf references
and Avro compatibility. It does not support Avro references; newer Karapace versions
add that capability ([vendor support](https://aiven.io/docs/products/kafka/karapace)).

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

## Generate samples

Select an exact schema version, then **Generate samples** to preview seeded,
schema-valid records without writing to Kafka. Publishing uses a separate
destination review and confirmation. See [supported schema forms, batch limits
and cancellation outcomes](structured-events.md#generate-schema-valid-samples).

## Generate a validating client

For a supported JSON Schema version, use **Generate JavaScript client** to create a Node 24 record codec and producer helper with pinned Registry framing and provenance. See [Generate a schema client](schema-clients.md) for supported keywords, runtime installation and limits.
