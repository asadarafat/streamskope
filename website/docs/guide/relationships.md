# Trace relationships and potential schema impact

Connect a profile, then open **Relationships**. Enter one to three visible topic
names, separated by commas. Optionally enter an **Impact subject** and its **Exact
subject version**. Choose **Discover relationships**. Discovery performs reads
only; it does not register/delete schemas or change connectors, groups or topics.

The graph is a bounded snapshot of the endpoints configured in this profile.
Select a node to filter the evidence table; **Show all relationships** restores
all edges. Every edge includes a source, observation time and interpretation.

| Evidence | Graph style | What it establishes                                                                                                                                      |
| -------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Observed | Solid       | An API returned an assignment, committed offset or tracked connector/topic relationship. It does not prove current record flow or successful processing. |
| Declared | Dashed      | A schema declares a reference, Registry associates an ID with a subject version, or connector configuration names a topic.                               |
| Inferred | Dotted      | A subject name matches `topic-key`/`topic-value`, or a sampled record resembles Confluent magic-byte/schema-ID framing. These mappings can be wrong.     |

Arrows follow the relation named in the table. Some represent dependencies or
metadata associations, so they do not all show the direction of record flow.

## Sources, coverage and limits

| Source                     | Discovery scope                                                                                                         | Unknowns that remain                                                                                                                                                                                      |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Kafka                      | Selected topics; at most 20 API-visible consumer/empty groups in name order                                             | Non-consumer coordination protocols are omitted. Permissions can hide groups. Commits can be historical/manually reset; assignments do not prove processing. General producer identities are unavailable. |
| Kafka Connect              | At most 10 connectors on the profile's endpoint                                                                         | The Connect cluster ID must match Kafka. Tracking may be disabled and reports use since creation/reset, not current traffic. Other Connect clusters and regex subscriptions are not expanded.             |
| Schema Registry            | First 20 visible subjects' latest versions, the exact requested version and bounded references; at most 24 schema reads | Historical dependent versions, hidden subjects, external consumers and reader schemas are not exhaustively discovered. Unresolved references remain visible as declared nodes.                            |
| Optional protected records | Preceding minute per topic; at most 200 records, 2 MiB and five seconds                                                 | Capped reads can favour partitions. Missing/masked originals cannot reveal IDs. Header candidates are not decoded or verified as valid payloads; GUID framing and custom encodings are unsupported.       |

The entire discovery has a **30-second deadline**, with at most **80 nodes and
160 edges**. Limits and failed/not-configured sources appear in **Coverage and
unknowns**. Complete coverage refers only to that bounded API-visible scope.
Discovery is explicit; there is no background scanning. **Cancel discovery**,
navigation away or a changed connection discards pending results. Graphs remain
in this page's memory and are not persisted. Raw records, member identities,
connector credentials and schema bodies are not returned in the graph.

A snapshot is stale after **60 seconds**. Its lineage remains inspectable, labelled
stale, while potential impact is withheld until a fresh discovery succeeds.

## Before changing a schema

1. Enter the exact subject/version whose users you need to investigate.
2. If permitted, enable **Inspect protected record headers for possible schema IDs**
   to supplement naming conventions with bounded framing evidence.
3. Inspect **Potential schema impact**. It follows declared schema dependents,
   ID/naming mappings to selected topics, then adjacent groups and connectors.
   Each potentially affected resource includes its evidence path.
4. Resolve unknown sources and ask downstream owners about reader schemas,
   other topics, older dependent versions and custom naming strategies.
5. Run the separate [Schema Registry compatibility check](schema-registry.md) before
   reviewing a change. Discovery itself never approves a change or deletion.

**No discovered impact means unknown impact, not zero impact.** Even observed
API edges do not establish that a new schema is compatible with a consumer.

Connect tracking semantics follow the [Apache Kafka Connect REST API](https://kafka.apache.org/43/kafka-connect/user-guide/).
