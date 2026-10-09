# Data, exports and limits

Use this reference when collecting incident evidence, backing up the workbench or
removing local data. Profile secret protection does not encrypt every stored file
or exported message.

## Stored data

Desktop paths below are relative to the [application-data directory](recovery.md#find-your-application-data).

| Data                                   | Location                                         | Protection and lifetime                                                                                                                                                                                                                                                    |
| -------------------------------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Kafka profiles and EDA resume metadata | `profiles/kafka-profiles.json`                   | Profile metadata is readable JSON; protected values such as trust material and secrets use OS-backed encryption. Persists across restarts                                                                                                                                  |
| NATS connection profiles               | `profiles/nats-profiles.json`                    | Profile metadata is readable JSON; token and trust values use OS-backed protection. Persists across restarts                                                                                                                                                               |
| Migration snapshots                    | `profiles/kafka-profiles.json.pre-*`             | Retain the original profile document and its protection. Persist until deliberately archived or removed                                                                                                                                                                    |
| Retrieval recipes and legacy templates | `templates/`                                     | Ordinary JSON containing endpoints, commands, parameter definitions and bindings; do not put secrets in recipe defaults                                                                                                                                                    |
| Rules                                  | `rules/kafka-rules.json`                         | Ordinary JSON; persists across restarts                                                                                                                                                                                                                                    |
| Saved investigation views and local topic notes | `queries/kafka-queries.json`                     | Ordinary JSON with selected topic task/group, query settings, layout, optional local profile IDs and selected/comparison/bookmark locators, plus topic descriptions, owners, labels and HTTPS links keyed by cluster ID/topic UUID. No record bodies, original bytes or headers; filter literals and resource positions can be sensitive. Persists across restarts |
| Topic configuration history            | `history/kafka-topic-configuration-history.json` | Ordinary JSON with recorded configuration-change evidence; not a broker audit log                                                                                                                                                                                          |
| Kafka observations                     | `history/kafka-observations.json`                | Unencrypted JSON with cluster/topic/group identities, offset and health samples, optional record-size/key-frequency aggregates and example partition/offset locators; no raw keys, headers or payloads. Private file permissions on POSIX                                  |
| Relationship graphs                    | Workbench page memory                            | Bounded identities and timestamped evidence; no saved graph, raw records, schema definitions or connector credentials                                                                                                                                                      |
| Operational preferences                | `workbench/kafka-operational-preferences.json`   | Ordinary JSON, including host-wide key/value encoding choices; persists across restarts                                                                                                                                                                                    |
| Installed plugins                      | `plugins/`                                       | Verified code, manifests and selection state; removed through Preferences → Plugins                                                                                                                                                                                        |
| Cached plugin packages                 | `plugins/.packages/`                             | Verified original archives and provenance; private file permissions on POSIX, not encrypted. Up to four archives of 48 MiB each; unused copies may be evicted. Removing a plugin retains cached delivery bytes for offline reinstallation                                  |
| Plugin download settings               | `plugins/network.json`                           | Proxy endpoint, mode and offline preference are readable JSON. Proxy credentials use OS-backed encryption when available; otherwise credentials are session-only and omitted from disk. Independent of provider connection settings.                                       |
| NSP recovery identifiers               | `plugins/.recovery/streamskope.nsp.json`         | Non-secret API/account and request/execution identifiers; survive restart and plugin version changes until confirmed cleanup clears them                                                                                                                                   |
| Browser-engine state                   | Other Electron files under application data      | Runtime caches/state; include in a same-machine full backup, but do not treat them as a message archive                                                                                                                                                                    |
| Read messages and activity history     | Bounded workbench memory                         | No durable message archive; closing/replacing a view or process can discard it                                                                                                                                                                                             |
| Downloaded exports and copied text     | User-selected file or OS clipboard               | Plaintext; remains outside the profile store and is not removed by uninstalling a plugin                                                                                                                                                                                   |

Releases through v0.7.0 use the legacy `preferences/` location. On upgrade, valid legacy
`preferences/` directories migrate into `workbench/` without changing
read-only or masking settings. Original directories are retained under
`workbench/migrations/preferences-*/preferences/`; include these in backups.
Chromium owns the separate `Preferences` file, which StreamSkope does not reset or
replace. Corrupt workbench preferences block connection until restored or deliberately
[reset while disconnected](recovery.md#operational-preference-recovery);
the browser file is never treated as workbench configuration. Operational
preferences remain ordinary JSON, separate from OS-backed credential encryption.

Saving record encoding preferences adopts workbench preference format 2. Before
migrating format 1, the host keeps an exact
`kafka-operational-preferences.json.pre-codecs-v1` backup; existing backups are not
overwritten. Keep it with the full application-data backup. Earlier hosts cannot
read format 2, and browser maintenance preflight rejects an incompatible rollback
before changing the running host or data. Restore the matching complete backup
when returning to an older host; do not edit preference format numbers manually.

Saved record positions identify a Kafka cluster, topic UUID/name, partition,
offset and original record-batch epoch. They are references, not a message archive.
Opening a saved view restores unloaded positions; explicit reload reads the
current broker through the host's current encoding and protection settings.
Retention, compaction, topic replacement or access changes can make a position
unavailable. Masking does not erase existing filter literals or locator metadata
from saved views and backups. Query-only share/import documents exclude these positions.
See [saved-view recovery](recovery.md#saved-view-library-recovery) for version-4
storage and exact predecessor backups.

Browser development profiles are session-only. Its installed plugins live in
`.cache/development-plugins`; a development checkout is not a desktop backup.
Keep approved portable files separately from the plugin cache. Cache metadata alone
does not authorize executable code; retained archives are verified again before use.
Development browser observation history also lasts only for the host session. Desktop
observation history is limited to eight identities, 240 samples per identity,
24 hours and 4 MiB; retention is enforced when the store is used, not by a
background erasure service. **Observed health → Clear all observation history** removes all
of it, including other profiles' observations. Enabling masking later does not
erase earlier aggregates. Topic names, group names, frequencies and locators can
still be sensitive operational information; protect backups accordingly.
EDA credentials used for a capture must be supplied again to resume after restart.
The cluster broker's temporary storage has a separate [EDA lifecycle](../plugins/eda.md#stop-update-and-resume).
The [NSP plugin](../plugins/nsp.md) also requires confirmed execution cleanup before
removal. Its workflow output temporarily exists on the NSP server; deleting an
execution does not establish erasure from server logs, exports or backups.

For removal, first stop owned EDA work, complete pending NSP cleanup and quit StreamSkope. Preserve anything you
need, then remove the identified app-data directory using your OS file manager.
Review exports, clipboard history and backups separately. Removing the application
binary or the desktop plugin does not establish that all copies of your data are
gone. Do not remove shared OS credential-service files to clean up one application.

## Production browser data

The [browser host](../start/containerlab.md) mounts its private data directory
at `/data`. `vault.json` holds authenticated vault metadata; `kafka-profiles.json`
and `nats-profiles.json` are at this directory's root. Their protected credentials
including SASL/HTTP credentials, TLS private keys and trust values use passphrase-derived authenticated encryption, with the key
held only by the unlocked host. This is separate from Electron's OS-backed keys.

The host also stores `rules/`, `queries/`, `templates/`, `history/`, `workbench/`
and `plugins/`, including durable offline-download policy, package cache and
recovery identifiers. These files, endpoint names and profile metadata are
ordinary filesystem data, not an encrypted disk. Browser observation history is
durable under the same retention bounds as the desktop store. Read messages stay
in bounded workbench memory; browser downloads remain outside this directory.

An installer-managed host keeps this data in
`/var/lib/streamskope/browser/streamskope-data`. Its parent directory also contains
private deployment records: `installation.json`, the pinned topology, image
manifest and checksums. These records retain the release, owner and port used to
resume the installation. They are separate from the vault and must be backed up
with it. Manual hosts use the directory selected in their release topology.

Back up the complete stopped deployment with its ownership and permissions
preserved, and preserve the passphrase separately. There is no passphrase recovery
or automatic desktop credential migration. Follow
[browser backup and restore](browser-host.md#back-up-and-restore) for the distinct
installer-managed and manual procedures.

## Message limits

These are workbench bounds, not Kafka retention settings
or a throughput guarantee. Both the record-count and byte limits apply.

| Boundary                         | Limit                                          | What happens                                                                                                   |
| -------------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Retained message count           | 1,000                                          | Older records leave the view as the window advances                                                            |
| Retained message bytes           | 64 MiB                                         | Retention can reach this limit before the record-count limit                                                   |
| Full record content              | 1 MiB                                          | Combined display text, headers, previews and original-byte envelope; over-limit display content is truncated   |
| Original record bytes            | 256 KiB                                        | Key, value and ordered headers are retained as Base64 within this bound; otherwise explicitly unavailable      |
| Value preview                    | 8 KiB                                          | Shows a bounded prefix, with original-size/truncation information                                              |
| Maximum bounded fetch count      | 1,000                                          | A bounded read does not imply a complete topic export                                                          |
| Serialized export record content | 8 MiB                                          | Current-page JSON fails when oversized; narrow the filters and retry                                           |
| Complete JSON export document    | 16 MiB                                         | Current-page JSON including formatting and metadata; separate final size check                                 |
| Default recent time window       | 2 minutes                                      | Resolved before Load messages; Custom interval accepts explicit start/end with a time zone                     |
| Broker search pass               | 10,000 records / 32 MiB / 30 seconds           | The first reached budget stops the pass with partial coverage; continuation keeps the original captured ranges |
| Read continuation                | Latest checkpoint / 30 minutes / 10,000 passes | Single-use, host-memory checkpoint for the same connection and record settings; unavailable after restart      |
| Saved view and topic-note library | 100 views / 256 annotated topics / 1 MiB                              | Unreadable or unsupported files are preserved for recovery                                                     |
| Saved bookmarks                  | 32 per view / 256 per library                  | Settings and locator metadata only; the shared library byte limit still applies                                |
| Portable view document | 128 KiB | Reviewed settings and record positions only; no local IDs, topic notes or payloads |
| Local topic notes | 4,096 UTF-8 description bytes / 128 owner characters / 16 labels / 8 links | The shared 1 MiB library limit still applies; no automatic pruning |
| Portable query document          | 32 KiB                                         | Versioned settings only; import requires review and explicit opening                                           |

Range exports use independent host bounds:

| Boundary                | Limit                             | What happens                                                              |
| ----------------------- | --------------------------------- | ------------------------------------------------------------------------- |
| Range export records    | 100,000                           | Maximum written matches; a lower user limit can stop first                |
| Range export scan       | 1,000,000 records / 1 GiB         | Scan limits include non-matching records                                  |
| Range export output     | 256 MiB                           | Stops before writing a row that would exceed the limit                    |
| Range export duration   | 5 minutes / 1,000 passes          | The first reached budget stops the captured range                         |
| Range export downloads  | 15 minutes / 2 simultaneous reads | One ready artifact; data and receipt can be read together                 |
| Range download duration | 5 minutes                         | Each transfer must finish before its own deadline and the artifact expiry |
| Range export receipt    | 1 MiB                             | Separate bounded JSON evidence document                                   |

Check [Monitor](operations.md#stream-monitor) for historical display omissions and
current pressure. Ordinary retention eviction as the selected window advances is
separate from overload loss. Stopping can also omit queued records when bounded
terminal publication reaches its budget or the transport is paused.
Filters operate on the records currently available to the workbench, including
previews for truncated content. **Search broker** separately scans the selected
finite range within its budgets and reports offset coverage. Neither mode proves
complete historical coverage; see [message investigation](messages.md#search-beyond-the-loaded-sample).

Continuing a finite read replaces its displayed result page. Cumulative progress
counts all confirmed passes but does not retain those earlier records for export.
Export a page before continuing if you need to keep it. A continuation retains
offsets and read settings in host memory, not record bytes or credentials. It
does not expand the original end offsets to include later arrivals. Reconnect,
another read, changed record settings or host restart invalidates it; it is not
a durable job or a saved query.

## Analysis limits

**Analyze range…** shares the finite-read bounds: up to 100,000 matching records,
1,000,000 scanned records, 1 GiB scanned, five minutes and 1,000 passes. Each
read still follows the per-pass bounds above. Its own result bounds also apply:

| Boundary                 | Limit                                  | What happens                                                                                                                         |
| ------------------------ | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Analysis selected fields | 12 / 256 path characters / 16 segments | Scalar key/value paths only; unsupported selectors are rejected                                                                      |
| Analysis groups          | 256 / 1 KiB per group key              | An oversized or new over-limit group stops before that record is counted; no Other bucket                                            |
| Analysis scalar value    | 1 KiB                                  | Oversized preview values are explicitly unavailable; whole-count field totals remain separate and grouping never uses shortened keys |
| Analysis preview         | 200 rows / 256 KiB                     | Preview retention stops; counting continues and reports omitted preview rows                                                         |
| Analysis result          | 384 KiB                                | Combined result bytes are bounded; exhaustion produces a partial count                                                               |
| Analysis evaluation work | 50,000,000 units                       | Total expression work budget across the analysis; exhaustion stops with an explicit partial reason                                   |
| Analysis work per record | 250,000 units                          | Shared across selected fields in one record; exhaustion stops before that record is counted                                          |

Counts, groups and field availability cover the confirmed matching records,
including records omitted from the preview. Unknown filter results make the match
count partial. Known missing fields, null keys, tombstones and JSON nulls remain
distinct from unavailable or masked values. Grouping excludes protected or
unavailable values and reports the exclusion counts separately; complete total
count does not imply complete grouping.

Analysis retains only a bounded protected projection and grouped values in host
memory. These can still contain sensitive message data. Disconnect, browser lock
or host shutdown clears them; there is no durable analysis file, implicit report
export or restart resume. Encoding or protection changes cannot reinterpret an
existing result. [Count and inspect a range](messages.md#count-and-inspect-a-range)
explains the controls and coverage labels.

## Understand an export

### Current-page JSON

The current-page export uses **schema version 3**: UTF-8 JSON containing the filtered records from one topic.
It includes the structured key/value projection, writer-schema identity, decoding
errors, ordered header previews, original-byte availability, partitions, offsets
and timestamps;
the active masking policy applies before export, but the file is not encrypted. Review the destination and contents
before sharing it. Stopping a tail gives a stable view but does not recover evicted
records or dropped delivery.

Inspect these fields before using an export as evidence:

| Field                                          | Meaning                                                                                                                                |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `topic`, `filters`                             | Topic and filters applied to the exported view                                                                                         |
| `retainedMessageCount`, `exportedMessageCount` | Retained and exported counts; neither is the broker's total record count                                                               |
| `stale`                                        | Whether the snapshot was marked stale when exported                                                                                    |
| `partition`, `offset`                          | The broker position of each exported record                                                                                            |
| `truncated`, `originalByteSize`                | Whether the representation is incomplete and the original content size                                                                 |
| `payload`, `preview`                           | Aliases of the interpreted value or its bounded preview; use the structured state to distinguish a tombstone from unavailable decoding |
| `structured.key`, `structured.value`           | Captured encoding, decoded text/JSON, per-record writer schema or explicit error/null/masked state                                     |
| `structured.headers`                           | Ordered text header entries; preserves duplicate names, nulls, empty values and decoding errors                                        |
| `structured.headersState`                      | Distinguishes captured ordered headers from unavailable header evidence; an empty list alone is not proof that no headers existed      |
| `structured.protection`                        | Whether host masking has been applied                                                                                                  |
| `payloadTruncated`                             | Marks a value withheld by retention limits; `structured.value.state` separately identifies decoding errors and tombstones              |
| `original.state`                               | `complete` means the bounded original bytes are present; `unavailable` carries a reason                                                |
| `original.key`, `original.value`               | Canonical Base64 bytes; JSON `null` means Kafka null, while an empty string means zero bytes                                           |
| `original.headers`                             | Ordered Base64 key/value entries preserving repeated names and null header values                                                      |

The **Original** inspector tab can copy one complete original as Base64 JSON.
Its ordered byte headers are authoritative for exact bytes; `structured.headers`
provides an ordered text projection without collapsing duplicate names.
Originals retain at most 128 headers, with 512 bytes per header name and 8 KiB per
header value, within the combined 256 KiB limit. Base64 is encoding, not encryption.
Interpretation follows the saved encoding preferences. Invalid text and decoding
failures are explicit; use the complete original envelope when byte fidelity matters. Incomplete, unavailable or masked
originals must not be reconstructed from previews. Version 1 exports did not retain
original bytes; version 2 retained originals but did not include the shared structured
projection. Consumers must recognize version 3 explicitly rather than treating its
interpreted payload as the original UTF-8 wire value.

Current-page exports describe only the retained grid records, not a complete broker backup.
If you need a complete archive, use an
approved Kafka data-export process with its own offset coverage and retention checks.

### Range JSONL and CSV

**Export → Read range…** reads the captured finite range on the host. It reuses the
same structured projection and protection policy as the grid, including explicit
null/error/masked states, writer schemas, duplicate ordered headers and immutable
original-byte availability. Original bytes remain subject to their capture bound;
a larger export does not make an oversized individual record complete.

JSONL contains one record per line. CSV has a fixed header and JSON-encoded text
and structured cells; decode those cells as JSON to recover their meaning. This
preserves numeric precision, newlines and duplicate headers, and avoids treating
record text as spreadsheet formulas. Neither format is encrypted after download.

Download the separate **receipt** with each file. Its
`streamskope.record-export/v1` schema identifies the exact topic, connection,
filters, range, codec/protection settings, start/end times, cumulative counts,
partition coverage, outcome and stopping reason. `output.sha256` and `output.bytes`
identify the data file. Zero matches produce a valid empty JSONL file or CSV header
and a receipt; no rows does not imply an unbounded search of all broker history.

The host spool is encrypted with runtime keys in a private temporary location
outside the durable profile/vault directory. Exports are not resumable after
host restart. Disconnect, browser lock, host shutdown, expiry, explicit discard or
replacement revokes download authority and removes the owned temporary copy during
normal operation. An abrupt process termination can leave unreadable encrypted
temporary files after its runtime key is lost; expiry limits download authority,
not guaranteed filesystem deletion after a crash. A failed cleanup keeps its
operation visible. Retry cleanup when offered; an unresolved broker close can
require disconnecting or restarting the host.

Desktop Save writes a private plaintext `.partial` file beside your chosen
destination before committing the final file. Normal cancellation or failure
removes that owned partial file; an abrupt process termination can leave it
behind. Review and remove any such destination partial file yourself. Browser
downloads use the browser's normal temporary-file behavior. Neither destination
is covered by the encrypted host spool.

An interrupted download or Save can fail even after transfer started; check the final file against its receipt.

## Other export and developer artifacts

[Environment snapshots](environment-comparison.md) use `streamskope.topic-config/v1` and include cluster/topic identities, observation times and seven validated settings. They omit credentials and unsupported settings, but topic names and configuration remain operational information.

[CLI exports](read-only-cli.md) are NDJSON with `format: "streamskope.cli/v1"`, record lines and a final coverage summary. They are separate from the desktop JSON export above. Their explicit protection configuration controls masking; they do not inherit desktop settings. Output files are private on POSIX, but are not encrypted. A failed command removes its own partial export; already-emitted stdout cannot be recalled.

[Generated clients](schema-clients.md) contain schema-derived validation code and subject/version/ID/hash provenance, without connection credentials. Clipboard copies and saved source files remain under your control. The [consumer sandbox](developer-sandbox.md) stores disposable records inside its owned containers, and private helper files under `.artifacts/sandbox`. Keep its instance marker until cleanup; `down` removes the owned data but does not erase your exported files.

## Share diagnostic evidence

**Raw logs → Export → Support report** in the Kafka workspace creates
`streamskope.support/v1` JSON. It contains the release identity, visible activity
metadata and explicit retention/filter counts, bounded to the latest 100 visible
entries. Free-form detail, object, operation and filter text are excluded;
invalid correlation IDs or timestamps are omitted rather than copied as text.
The report does not include profiles, endpoints, configuration, credentials,
message data or host logs. It is a limited view of retained activity, not a full
audit trail or a root-cause diagnosis. It remains an ordinary downloaded file.

Include the release tag/build, OS/CPU, action, expected and actual result, and a
redacted correlation ID. For EDA include plugin/cluster versions and session phase.
Review raw logs and screenshots even when application secrets are redacted; topic
names, hostnames and message content can still be sensitive. Exclude credentials,
tokens, trust material, private payloads and full app-data directories from public
issues. See [Troubleshooting](troubleshooting.md#share-useful-evidence).

## Mask records before they reach the workbench

Disconnect Kafka, then open **Preferences → Protection**. Finish active plugin
capture and cleanup work before saving. Protection settings apply to every
connection on this host and use the same storage as workbench preferences.

| Control                   | Result                                                                                                                                                           |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Mask record keys          | Replaces non-null keys with `[MASKED]`.                                                                                                                          |
| Header names to mask      | Replaces exact, case-sensitive names, including duplicate occurrences.                                                                                           |
| JSON value paths          | Replaces the selected fields using JSON Pointer, such as `/customer/email` or `/items/0/token`. Escape `/` as `~1` and `~` as `~0`; wildcards are not supported. |
| Mask entire record values | Replaces each non-null value. Tombstones remain null.                                                                                                            |

There can be at most 32 header names and 32 value paths, each at most 512
characters, with at most 16 path segments. Invalid or duplicate rules are rejected.
JSON paths apply to the shared decoded JSON projection, including supported Avro
and Protobuf records. Non-JSON, malformed and incomplete values are fully masked
when selective safety cannot be established; they never fall back to a raw
preview. A path absent from valid decoded JSON does not change that document.

Masking runs in the application host before records reach the table, inspector,
local filters, live rules, comparison, tracing, copy or export. This includes reads through connections
created by EDA and NSP plugins. All original-byte envelopes are withheld while any
masking is active. Structured projections and ordered headers contain only the
protected values. Broker search and tracing evaluate that same protected view. Topic names, offsets, timestamps and record sizes
remain visible. The status bar shows **Masking active**.

Saving protection clears retained records and transform logs. It cannot recall
previous exports, clipboard contents or data already copied into another program.
Masking is a disclosure aid controlled by the local operator, not encryption or a
sandbox for installed plugins. The plugins are trusted application code.
