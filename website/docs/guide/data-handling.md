# Data, exports and limits

Use this reference when collecting incident evidence, backing up the workbench or
removing local data. Profile secret protection does not encrypt every stored file
or exported message.

## Stored data

Paths below are relative to the [application-data directory](recovery.md#find-your-application-data).

| Data                                        | Location                                         | Protection and lifetime                                                                                                                                     |
| ------------------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Connection profiles and EDA resume metadata | `profiles/kafka-profiles.json`                   | Profile metadata is readable JSON; protected values such as trust material and secrets use OS-backed encryption. Persists across restarts                   |
| Migration snapshots                         | `profiles/kafka-profiles.json.pre-*`             | Retain the original profile document and its protection. Persist until deliberately archived or removed                                                     |
| Retrieval recipes and legacy templates      | `templates/`                                     | Ordinary JSON containing endpoints, commands, parameter definitions and bindings; do not put secrets in recipe defaults                                     |
| Rules                                       | `rules/kafka-rules.json`                         | Ordinary JSON; persists across restarts                                                                                                                     |
| Saved investigation queries                 | `queries/kafka-queries.json`                     | Ordinary JSON with bounds, filters and optional local profile IDs; no message records. Filter literals can contain sensitive text; persists across restarts |
| Topic configuration history                 | `history/kafka-topic-configuration-history.json` | Ordinary JSON with recorded configuration-change evidence; not a broker audit log                                                                           |
| Operational preferences                     | `preferences/kafka-operational-preferences.json` | Ordinary JSON; persists across restarts                                                                                                                     |
| Installed plugins                           | `plugins/`                                       | Verified code, manifests and selection state; removed through Preferences → Plugins                                                                         |
| NSP recovery identifiers                    | `plugins/.recovery/streamskope.nsp.json`         | Non-secret API/account and request/execution identifiers; survive restart and plugin version changes until confirmed cleanup clears them                    |
| Browser-engine state                        | Other Electron files under application data      | Runtime caches/state; include in a same-machine full backup, but do not treat them as a message archive                                                     |
| Read messages and activity history          | Bounded workbench memory                         | No durable message archive; closing/replacing a view or process can discard it                                                                              |
| Downloaded JSON and copied text             | User-selected file or OS clipboard               | Plaintext; remains outside the profile store and is not removed by uninstalling a plugin                                                                    |

Browser development profiles are session-only. Its installed plugins live in
`.cache/development-plugins`; a development checkout is not a desktop backup.
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

| Boundary                         | Limit                                | What happens                                                                                        |
| -------------------------------- | ------------------------------------ | --------------------------------------------------------------------------------------------------- |
| Retained message count           | 1,000                                | Older records leave the view as the window advances                                                 |
| Retained message bytes           | 64 MiB                               | Retention can reach this limit before the record-count limit                                        |
| Full record content              | 1 MiB                                | Combined key/value content above this limit is truncated; a preview is not the complete value       |
| Value preview                    | 8 KiB                                | Shows a bounded prefix, with original-size/truncation information                                   |
| Maximum bounded fetch count      | 1,000                                | A bounded read does not imply a complete topic export                                               |
| Serialized export record content | 8 MiB                                | An oversized export fails; narrow the filters and retry                                             |
| Complete JSON export document    | 16 MiB                               | Includes formatting and metadata; this is a separate final size check                               |
| Default recent time window       | 2 minutes                            | Resolved before Load messages; Custom interval accepts explicit start/end with a time zone          |
| Broker search scan               | 10,000 records / 32 MiB / 30 seconds | The first reached budget stops the scan with partial coverage; limits do not imply complete history |
| Saved query library              | 100 queries / 1 MiB                  | Unreadable or unsupported files are preserved for recovery                                          |
| Portable query document          | 32 KiB                               | Versioned settings only; import requires review and explicit opening                                |

Check **Monitor** for overload drops as well as ordinary retention evictions.
Filters operate on the records currently available to the workbench, including
previews for truncated content. **Search broker** separately scans the selected
finite range within its budgets and reports offset coverage. Neither mode proves
complete historical coverage; see [message investigation](messages.md#search-beyond-the-loaded-sample).

## Understand an export

The message export is UTF-8 JSON containing the filtered records from one topic.
It includes keys, headers, payloads/previews, partitions, offsets and timestamps;
it is not automatically redacted or encrypted. Review the destination and contents
before sharing it. Stopping a tail gives a stable view but does not recover evicted
records or dropped delivery.

Inspect these fields before using an export as evidence:

| Field                                          | Meaning                                                                                                    |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `topic`, `filters`                             | Topic and filters applied to the exported view                                                             |
| `retainedMessageCount`, `exportedMessageCount` | Retained and exported counts; neither is the broker's total record count                                   |
| `stale`                                        | Whether the snapshot was marked stale when exported                                                        |
| `partition`, `offset`                          | The broker position of each exported record                                                                |
| `truncated`, `originalByteSize`                | Whether the representation is incomplete and the original content size                                     |
| `payload`, `preview`                           | Available complete value or bounded preview; a null payload with truncation is not a recovered full record |

Exports contain the current representation, not a byte-for-byte broker backup.
Headers and other fields also have bounds. If you need a complete archive, use an
approved Kafka data-export process with its own offset coverage and retention checks.

## Share diagnostic evidence

Include the release tag/build, OS/CPU, action, expected and actual result, and a
redacted correlation ID. For EDA include plugin/cluster versions and session phase.
Review raw logs and screenshots even when application secrets are redacted; topic
names, hostnames and message content can still be sensitive. Exclude credentials,
tokens, trust material, private payloads and full app-data directories from public
issues. See [Troubleshooting](troubleshooting.md#share-useful-evidence).
