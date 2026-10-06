# Data, exports and limits

Use this reference when collecting incident evidence, backing up the workbench or
removing local data. Profile secret protection does not encrypt every stored file
or exported message.

## Stored data

Paths below are relative to the [application-data directory](recovery.md#find-your-application-data).

| Data                                        | Location                                         | Protection and lifetime                                                                                                                                                                                                                   |
| ------------------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Connection profiles and EDA resume metadata | `profiles/kafka-profiles.json`                   | Profile metadata is readable JSON; protected values such as trust material and secrets use OS-backed encryption. Persists across restarts                                                                                                 |
| Migration snapshots                         | `profiles/kafka-profiles.json.pre-*`             | Retain the original profile document and its protection. Persist until deliberately archived or removed                                                                                                                                   |
| Retrieval recipes and legacy templates      | `templates/`                                     | Ordinary JSON containing endpoints, commands, parameter definitions and bindings; do not put secrets in recipe defaults                                                                                                                   |
| Rules                                       | `rules/kafka-rules.json`                         | Ordinary JSON; persists across restarts                                                                                                                                                                                                   |
| Saved investigation queries                 | `queries/kafka-queries.json`                     | Ordinary JSON with bounds, filters and optional local profile IDs; no message records. Filter literals can contain sensitive text; persists across restarts                                                                               |
| Topic configuration history                 | `history/kafka-topic-configuration-history.json` | Ordinary JSON with recorded configuration-change evidence; not a broker audit log                                                                                                                                                         |
| Kafka observations                          | `history/kafka-observations.json`                | Unencrypted JSON with cluster/topic/group identities, offset and health samples, optional record-size/key-frequency aggregates and example partition/offset locators; no raw keys, headers or payloads. Private file permissions on POSIX |
| Relationship graphs                         | Workbench page memory                            | Bounded identities and timestamped evidence; no saved graph, raw records, schema definitions or connector credentials                                                                                                                     |
| Operational preferences                     | `workbench/kafka-operational-preferences.json`   | Ordinary JSON; persists across restarts                                                                                                                                                                                                   |
| Installed plugins                           | `plugins/`                                       | Verified code, manifests and selection state; removed through Preferences → Plugins                                                                                                                                                       |
| Cached plugin packages                      | `plugins/.packages/`                             | Verified original archives and provenance; private file permissions, not encrypted. Up to four archives of 48 MiB each; unused copies may be evicted. Removing a plugin retains cached delivery bytes for offline reinstallation          |
| NSP recovery identifiers                    | `plugins/.recovery/streamskope.nsp.json`         | Non-secret API/account and request/execution identifiers; survive restart and plugin version changes until confirmed cleanup clears them                                                                                                  |
| Browser-engine state                        | Other Electron files under application data      | Runtime caches/state; include in a same-machine full backup, but do not treat them as a message archive                                                                                                                                   |
| Read messages and activity history          | Bounded workbench memory                         | No durable message archive; closing/replacing a view or process can discard it                                                                                                                                                            |
| Downloaded JSON and copied text             | User-selected file or OS clipboard               | Plaintext; remains outside the profile store and is not removed by uninstalling a plugin                                                                                                                                                  |

Releases through v0.7.0 use the legacy `preferences/` location. On upgrade, valid legacy
`preferences/` directories migrate into `workbench/` without changing
read-only or masking settings. Original directories are retained under
`workbench/migrations/preferences-*/preferences/`; include these in backups.
Chromium owns the separate `Preferences` file, which StreamSkope does not reset or
replace. Corrupt workbench preferences block connection until restored or deliberately
[reset while disconnected](recovery.md#operational-preference-recovery);
the browser file is never treated as workbench configuration. Operational
preferences remain ordinary JSON, separate from OS-backed credential encryption.

Browser development profiles are session-only. Its installed plugins live in
`.cache/development-plugins`; a development checkout is not a desktop backup.
Keep approved portable files separately from the plugin cache. Cache metadata alone
does not authorize executable code; retained archives are verified again before use.
Browser observation history also lasts only for the host session. Desktop
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

## Message limits

These are workbench bounds, not Kafka retention settings
or a throughput guarantee. Both the record-count and byte limits apply.

| Boundary                         | Limit                                | What happens                                                                                                 |
| -------------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| Retained message count           | 1,000                                | Older records leave the view as the window advances                                                          |
| Retained message bytes           | 64 MiB                               | Retention can reach this limit before the record-count limit                                                 |
| Full record content              | 1 MiB                                | Combined display text, headers, previews and original-byte envelope; over-limit display content is truncated |
| Original record bytes            | 256 KiB                              | Key, value and ordered headers are retained as Base64 within this bound; otherwise explicitly unavailable    |
| Value preview                    | 8 KiB                                | Shows a bounded prefix, with original-size/truncation information                                            |
| Maximum bounded fetch count      | 1,000                                | A bounded read does not imply a complete topic export                                                        |
| Serialized export record content | 8 MiB                                | An oversized export fails; narrow the filters and retry                                                      |
| Complete JSON export document    | 16 MiB                               | Includes formatting and metadata; this is a separate final size check                                        |
| Default recent time window       | 2 minutes                            | Resolved before Load messages; Custom interval accepts explicit start/end with a time zone                   |
| Broker search scan               | 10,000 records / 32 MiB / 30 seconds | The first reached budget stops the scan with partial coverage; limits do not imply complete history          |
| Saved query library              | 100 queries / 1 MiB                  | Unreadable or unsupported files are preserved for recovery                                                   |
| Portable query document          | 32 KiB                               | Versioned settings only; import requires review and explicit opening                                         |

Check [Monitor](operations.md#stream-monitor) for historical display omissions and
current pressure. Ordinary retention eviction as the selected window advances is
separate from overload loss. Stopping can also omit queued records when bounded
terminal publication reaches its budget or the transport is paused.
Filters operate on the records currently available to the workbench, including
previews for truncated content. **Search broker** separately scans the selected
finite range within its budgets and reports offset coverage. Neither mode proves
complete historical coverage; see [message investigation](messages.md#search-beyond-the-loaded-sample).

## Understand an export

The message export uses **schema version 2**: UTF-8 JSON containing the filtered records from one topic.
It includes keys, headers, payloads/previews, partitions, offsets and timestamps;
it is not automatically redacted or encrypted. Review the destination and contents
before sharing it. Stopping a tail gives a stable view but does not recover evicted
records or dropped delivery.

Inspect these fields before using an export as evidence:

| Field                                          | Meaning                                                                                                         |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `topic`, `filters`                             | Topic and filters applied to the exported view                                                                  |
| `retainedMessageCount`, `exportedMessageCount` | Retained and exported counts; neither is the broker's total record count                                        |
| `stale`                                        | Whether the snapshot was marked stale when exported                                                             |
| `partition`, `offset`                          | The broker position of each exported record                                                                     |
| `truncated`, `originalByteSize`                | Whether the representation is incomplete and the original content size                                          |
| `payload`, `preview`                           | Available complete value or bounded preview; a null payload with truncation is not a recovered full record      |
| `payloadTruncated`                             | Distinguishes a withheld display value from a Kafka tombstone, including invalid UTF-8 that expands on decoding |
| `original.state`                               | `complete` means the bounded original bytes are present; `unavailable` carries a reason                         |
| `original.key`, `original.value`               | Canonical Base64 bytes; JSON `null` means Kafka null, while an empty string means zero bytes                    |
| `original.headers`                             | Ordered Base64 key/value entries preserving repeated names and null header values                               |

The **Original** inspector tab can copy one complete original as Base64 JSON.
Its ordered headers are authoritative; the Metadata header dictionary is a text preview.
Originals retain at most 128 headers, with 512 bytes per header name and 8 KiB per
header value, within the combined 256 KiB limit. Base64 is encoding, not encryption.
Display text decodes UTF-8 and can replace invalid sequences; use the complete
original envelope when byte fidelity matters. Incomplete, unavailable or masked
originals must not be reconstructed from previews. Version 1 exports did not retain
original bytes; consumers of the export format must recognize version 2 explicitly.

Exports still describe only the retained records, not a complete broker backup.
If you need a complete archive, use an
approved Kafka data-export process with its own offset coverage and retention checks.

## Other export and developer artifacts

[Environment snapshots](environment-comparison.md) use `streamskope.topic-config/v1` and include cluster/topic identities, observation times and seven validated settings. They omit credentials and unsupported settings, but topic names and configuration remain operational information.

[CLI exports](read-only-cli.md) are NDJSON with `format: "streamskope.cli/v1"`, record lines and a final coverage summary. They are separate from the desktop JSON export above. Their explicit protection configuration controls masking; they do not inherit desktop settings. Output files are private on POSIX, but are not encrypted. A failed command removes its own partial export; already-emitted stdout cannot be recalled.

[Generated clients](schema-clients.md) contain schema-derived validation code and subject/version/ID/hash provenance, without connection credentials. Clipboard copies and saved source files remain under your control. The [consumer sandbox](developer-sandbox.md) stores disposable records inside its owned containers, and private helper files under `.artifacts/sandbox`. Keep its instance marker until cleanup; `down` removes the owned data but does not erase your exported files.

## Share diagnostic evidence

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
When JSON paths are configured, non-JSON and incomplete values are fully masked;
they never fall back to a raw preview. A path that is absent from valid JSON does
not change that document.

Masking runs in the application host before records reach the table, inspector,
local filters, live rules, copy or export. This includes reads through connections
created by EDA and NSP plugins. All original-byte envelopes are withheld while any
masking is active. Broker-side search is unavailable with masking; use bounded
reads and local filters instead. Topic names, offsets, timestamps and record sizes
remain visible. The status bar shows **Masking active**.

Saving protection clears retained records and transform logs. It cannot recall
previous exports, clipboard contents or data already copied into another program.
Masking is a disclosure aid controlled by the local operator, not encryption or a
sandbox for installed plugins. The plugins are trusted application code.
