# Records, decoding and original bytes

A Kafka record has several useful representations. A position identifies where it
was read, original bytes describe what Kafka stored, and a decoded projection
makes its fields usable. Each answers a different question.

## Identity is more than a payload

Two records can have identical values at different offsets. A topic name can also
be reused after deletion. Source cluster and topic identities, partition and offset
matter when reloading a bookmark or checking a replay destination. Equal payloads
do not establish that two observations refer to the same record.

A saved position is a way to read again. It does not keep the old record alive
through retention, compaction or topic replacement. An exported record is a
separate copy with its own disclosure and storage considerations.

## Decoding preserves the original

The workbench uses one captured key/value interpretation across the grid, filters,
comparison, tracing and export. A topic can contain records with different writer
schemas; a schema belongs to the individual record, not just the topic name.
Writer IDs are scoped to their Schema Registry, so matching numeric IDs from two
Registries do not establish the same schema.

Decoding does not edit original bytes. A readable projection is useful for field
comparisons, but it can omit wire details. Comparing original bytes answers a
different question from comparing decoded fields. Neither comparison establishes
that a downstream consumer processed the record successfully.

## Unavailable does not mean empty

A Kafka tombstone has a null value. An empty byte sequence, an encoded JSON
`null`, an absent JSON property and a decoding error are different states.
Unreadable fields cannot establish a negative match in a field filter or trace.
Ordered headers also preserve duplicate names; converting them into an ordinary
object would lose evidence.

Masking changes what the workbench may disclose. Original bytes are withheld
when masking applies, and the protected projection is used by the supported read
workflows. Matching masked placeholders do not prove the underlying secrets are
equal. A byte-copy operation must not bypass that protection; replay review is
blocked while masking applies.

## Use the appropriate view

- [Decode and compare records](../guide/structured-events.md) gives the controls
  for selecting encodings and comparing evidence.
- [Understand a changed payload](../skope/changed-payload.md) connects those
  controls to a diagnostic decision.
- [Record formats](../reference/record-formats.md) lists supported encodings,
  representations and bounds.
- [Data and exports](../guide/data-handling.md) describes what leaves the host.
