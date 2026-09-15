# Inspect a schema

Connect a Schema Registry and read a subject's schema version. You need the
Registry endpoint and permission to read its subjects, as well as a Kafka profile.

## Connect the service

1. Open **Connection Profiles** and edit your Kafka profile.
2. Enter the **Schema Registry URL** and choose its authentication method.
3. Save the profile and connect it.
4. Open **Schema Registry** in the left navigation.

**You should see:** the subjects you can access. The included AIO fixture configures
Karapace, a Confluent-compatible Registry, in its local profile and provides a
`test-value` Avro subject.

Registry access is separate from broker access. If Kafka connects but subjects
are unavailable, check the Registry URL, authentication and **Raw logs**.
Only use the profile's OAuth token when the Registry accepts it.

## Inspect and evolve a schema

1. Search for a subject and select it. With the local fixture, use `test-value`.
2. Select a version and read the schema.
3. Check the version and subject name before using the schema to interpret a record.

**You should have:** a schema definition and its version within the selected
subject. A subject's naming strategy determines how it relates to topic records.

If you need to change a schema:

1. Prepare the proposed schema for that subject.
2. Check compatibility and review the result.
3. Register the new version only when the change is appropriate for its readers.
4. Reopen the subject and verify the newly registered version.

Registration does not rewrite existing Kafka messages. Review the exact subject
and version before deletion; permanent deletion can break readers that need it.

## Next: read the record

[Inspect a message's value and headers →](messages.md#inspect-a-record)
