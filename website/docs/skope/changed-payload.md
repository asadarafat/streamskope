# Understand a changed payload

Two events for the same kind of operation look different. Determine whether the
difference is in the data, its encoding, the writer schema or an unavailable field.

## Watch the walkthrough

A 42-second recording of the actual workbench with on-screen instructions and no
audio. Compare two illustrative JSON events on the local AIO Kafka broker, including their ordered headers.

<video class="skope-walkthrough" controls muted playsinline preload="metadata"
       poster="../../assets/skope/changed-payload.png"
       aria-label="Walkthrough: Understand a changed payload"
       aria-describedby="changed-payload-video-summary">
<source src="../assets/skope/changed-payload.mp4" type="video/mp4">
  <track kind="captions" src="../assets/skope/changed-payload.vtt" srclang="en" label="English instructions">
  <a href="../assets/skope/changed-payload.mp4">Download the walkthrough</a>.
</video>

<p id="changed-payload-video-summary">Follow the actions and outcomes below. Pause or use
fullscreen to inspect the controls; the written guide covers prerequisites and
the decisions behind each step.</p>

<details markdown="1">
<summary>Read the walkthrough transcript</summary>

- **00:00 — Select the earlier record.** Two illustrative AIO records. Start with a bounded read.
- **00:07 — Pin it as the baseline.** Open Compare and pin the record before selecting another.
- **00:14 — Select the later record.** Before stays pinned. Your next selection becomes After.
- **00:21 — Compare interpreted values.** Protected projection keeps the inspector’s interpretation and masking.
- **00:28 — Check ordered headers.** Matching payloads would not prove matching headers.
- **00:35 — Keep the conclusion precise.** A field difference does not establish downstream processing success.

</details>

## Before you start

Connect the intended Kafka profile and read two relevant records. Stop the read
to keep the selection stable. Use the correct Registry settings for framed data
and inspect **Decoded** for each record's encoding and writer identity.

Choose [encoding preferences](../guide/structured-events.md#choose-how-records-are-interpreted)
before comparing. Do not force an encoding merely to make an error disappear.

## Compare the records

1. Choose records that answer the same question: source, operation and relevant
   interval. Matching keys alone do not establish that they represent the same event.
2. Follow [Compare two records](../guide/structured-events.md#compare-two-records)
   to pin the baseline and compare the selected value, key or ordered headers.
3. Read **Protected projection** to compare the same fields shown by the grid and
   inspector. Check added, removed and changed paths, plus any partial-result notice.
4. When both complete originals are available, switch to **Original bytes (Base64)**
   to check wire-byte differences. A text view alone cannot establish byte equality.
5. If the writer schemas differ, use [schema version comparison](../guide/schema-registry.md)
   to inspect the relevant definitions and references. A Registry ID is meaningful
   within its Registry; equal numbers from different Registries do not prove equality.

For repeated checks, [bookmark the records](../guide/saved-views.md#bookmark-and-reload-a-record).
Bookmarks retain positions, not a durable copy of their payloads. The transient
comparison baseline survives topic navigation within the connection. Changing
connection, encoding or protection clears loaded content.

## Interpret the differences

| Result                                    | Interpretation                                                                                          |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| A field was added, removed or changed     | The selected interpreted representations differ at that path; check whether the consumer uses it        |
| Projection matches, original bytes differ | Encoding, framing or another byte-level detail differs; inspect the writer and selected bytes           |
| A value is null or a field is absent      | Distinguish a Kafka tombstone, encoded JSON null and a missing property; they are different states      |
| A field is masked or cannot decode        | Its underlying value is unavailable for this comparison, not proven equal                               |
| No differences, with complete coverage    | Only the selected representation matches; unselected fields and downstream processing were not compared |

Duplicate headers and arrays keep their order. Check
[comparison limits](../reference/record-formats.md#comparison-bounds) before
treating the displayed changes as complete.

## Finish and share

[Share the record positions and your findings](share-findings.md), including the
representation compared, writer identities and any unavailable fields. If a
schema change is being considered, [review observed relationships](../guide/relationships.md#before-changing-a-schema)
for possible impact. That graph is partial evidence, not an inventory of every
consumer. Comparing records does not change data or authorize a schema update.

## Test the expected payload

For an agreed schema, [generate a client scaffold](../guide/schema-clients.md) or
[run a consumer sandbox](../guide/developer-sandbox.md) before changing a real
consumer. The [source workbench](../start/development.md) provides development
fixtures. A fixture result does not prove production compatibility.
