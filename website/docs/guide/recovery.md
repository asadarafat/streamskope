# Upgrade, back up and recover

Use this procedure before replacing the desktop app or downgrading it. Record the
release tag as well as the app version: `v0.1.0+build.1` identifies the first
release, while `0.1.0` is the app version. For new releases the app version identifies the desktop release, while plugins
version independently. Record each installed plugin version and its requirements.

The [qualification record](qualification.md) separates recorded results from
platforms and migration scenarios that still need verification.

For a production browser deployment, use [browser host backup and restore](browser-host.md#back-up-and-restore).
Preserve the complete private data directory and a separate vault passphrase;
installer-managed hosts also need their private deployment records.
Desktop profile files cannot be copied into it to recover their OS encryption
keys; re-enter credentials through the browser connection workflow.

## Find your application data

The current packaged app uses Electron's `userData` directory and the lowercase
application name `streamskope`:

| Platform | Default directory                                                     |
| -------- | --------------------------------------------------------------------- |
| macOS    | `~/Library/Application Support/streamskope`                           |
| Windows  | `%APPDATA%\streamskope`                                               |
| Linux    | `$XDG_CONFIG_HOME/streamskope`, or `~/.config/streamskope` when unset |

These follow [Electron's application-data conventions](https://www.electronjs.org/docs/latest/api/app#appgetpathname).
Custom launchers, redirected user folders or older builds can use another location.
Locate the existing `profiles/kafka-profiles.json` before restoring anything;
creating an empty directory does not recover the original store.

On Linux, unlock a supported Secret Service/KWallet credential store in the desktop
session. StreamSkope refuses the `basic_text` and `unknown` protection backends.
If profile protection is unavailable, restore credential-service access and restart
the app before retrying. A copied profile file alone does not recreate OS keys.

## Back up before upgrading

1. Record the installed release tag, OS/CPU, plugin versions and target EDA/NSP versions.
   Retain the matching installer and its verified checksum for recovery.
2. Stop or deliberately keep any EDA capture using the exit prompt. Keeping a
   capture only preserves it until its [lease expires](../plugins/eda.md#stop-update-and-resume).
3. Quit every StreamSkope process using this data directory.
4. Make a dated, access-restricted copy of the **entire** directory outside the
   live app directory. Include `profiles/` and every recovery generation,
   `templates/`, `rules/`, `queries/`, `history/`, `workbench/`, any legacy
   `preferences/`, `plugins/` and Electron's
   supporting files. Compare file sizes or hashes with the originals.
5. Preserve the same OS user and credential-service context. Store exported message
   files separately if needed; they are outside this directory. Review the
   [data inventory](data-handling.md#stored-data) when choosing backup protection.

Automatic migration files are snapshots, not continuous backups. OS account
recreation, credential-store loss or copying files to another machine can make
protected values unreadable. Plan to re-enter credentials if the OS keys cannot be
recovered. Never attach a full backup to an issue report.

## Upgrade and verify

1. Download the intended release and verify its checksum, then replace/install the
   app using the [platform instructions](../start/installation.md). Retain the backup.
2. Start under the same OS account with its credential service unlocked.
3. Check profile names, broker endpoints and TLS/plaintext choices before connecting.
   Confirm that protected values are available; do not overwrite failed profiles
   with empty values to dismiss an error.
4. Open **Preferences → Plugins**. Install the compatible plugin if a saved EDA
   or NSP profile reports it missing. The original release uses API 3 packages;
   desktop 0.2.0 introduced API 4 support and release builds preserve installed API 2/3 packages. Check the
   [migration instructions](../plugins/versioning.md#upgrade-from-the-original-packages),
   desktop interval and target version. A stopped EDA capture needs an explicit resume.
5. Test a reviewed profile, read a known topic and confirm the expected result.
   Retain the backup until the workflows you use have been verified.

[Observed health](observed-health.md) retains bounded local history under `history/`;
it is covered by the full backup above. **Clear all observation history** removes observations
and saved settings for every profile in this app-data directory and cannot be undone without a backup.
It does not erase copies in backups or change Kafka data. Older desktop versions
without Observed health do not display this history; a new history file does not
by itself migrate the profile format. Observation history format2 retains desired
settings and direct summaries. Its first migration preserves exact format1 bytes
in `history/kafka-observations.json.pre-observation-v1`; this copy has separate
retention from active measurements. Older hosts refuse unsupported format2, so
restore a verified whole-data backup before rollback. Restoring saved observation
settings does not connect, unlock or authorize collection; explicitly start again.

### Operational-preference recovery

Workbench settings use `workbench/kafka-operational-preferences.json`,
separate from Chromium's `Preferences` file. Valid older `preferences/` directories
are migrated with every protection choice preserved; the original directory is
archived under `workbench/migrations/preferences-*/preferences/`. Malformed or
unreadable workbench data stays blocked instead of silently loading permissive
defaults. Do not remove or replace Chromium's file to recover workbench settings.
For a downgrade, restore the complete pre-upgrade backup; older apps do not read
the new location, and later changes are not copied back into archived settings.
Releases through v0.7.0 use the legacy location. See the
[source-specific qualification](qualification.md#current-source-qualification).

If operational preferences cannot be read, preserve a complete backup and restore
known-good settings before reconnecting. If you deliberately choose to reset them:

1. Disconnect Kafka, finish or cancel active requests, and complete pending plugin
   capture or cleanup work.
2. Open **Preferences → Workbench → Reset workbench preferences** and confirm.
3. Open **Preferences → Protection** before reconnecting. A reset preserves
   readable protection settings. Recovery from unreadable storage enables
   read-only mode and masks keys and values; review header masking rules and your
   intended protection choices explicitly.

This reset does not remove profiles, rules, templates, topic history or Kafka data.
Do not delete Chromium's `Preferences` file to perform it. If storage remains
unavailable, restore filesystem access or a complete known-good backup rather than
repeatedly resetting it.

### Saved-view library recovery

The view library keeps its existing path, `queries/kafka-queries.json`. Reading
legacy version-1 queries, version-2 views or version-3 bookmarked views supplies missing default positions and an empty topic catalog
without rewriting the file. The first save or delete that changes the library
writes version 4 and preserves the exact original first:

| Original library           | Preserved predecessor               |
| -------------------------- | ----------------------------------- |
| Version 1 queries          | `kafka-queries.json.pre-views-v1`   |
| Version 2 views            | `kafka-queries.json.pre-records-v2` |
| Version 3 bookmarked views | `kafka-queries.json.pre-catalog-v3` |

An existing different valid predecessor uses an unused numbered generation;
later saves do not replace it or silently downgrade. These JSON files contain
settings, local topic notes and record locators, including potentially sensitive filter literals,
resource names and offsets. They contain no saved record bodies or original bytes.

Back up the whole data directory before upgrading. Hosts supporting only version
1, 2 or 3 cannot read the version-4 library. Browser maintenance refuses normal
rollback when the current
library or its retained sidecars are incompatible with the older image. Normal
rollback changes the image; it does **not** restore historical data. To recover an
older host, first stop the host and separately restore a verified complete
pre-upgrade backup using the [browser backup/restore procedure](browser-deployment.md#back-up-and-restore), preserving the original
lease and data-directory ownership. Verify hashes and permissions before running
normal rollback; follow the [in-place downgrade precautions](browser-deployment.md#recover-data-before-an-incompatible-downgrade). Do not remove view files, backups or lease metadata just to make
compatibility checks pass. Desktop recovery likewise needs the complete compatible
backup; changes made after that backup will be absent.

## Choose the correct rollback snapshot

| Situation                                                                             | Snapshot to preserve and use                                                                                                                                             |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Downgrade from API 4 plugins                                                          | Complete pre-upgrade backup containing the older desktop-compatible plugin packages                                                                                      |
| Downgrade after the saved-view library becomes version 4                              | Complete pre-upgrade data backup containing the target's supported query/view format and compatible settings; retain all predecessor families with that backup           |
| Routine downgrade                                                                     | Your complete backup made with the target release before upgrading                                                                                                       |
| Downgrade across explicit TLS/plaintext transport migration                           | The exact profile filename recorded by `rollbackGeneration` in the version-3 profile document                                                                            |
| Downgrade after saving password SASL, client identity or independent service security | The exact `.pre-security-v3` generation recorded by `rollbackGeneration` in the version-4 profile document; restore a complete compatible data backup for a browser host |
| Earlier protected-schema migration                                                    | `kafka-profiles.json.pre-upgrade.bak`, only when it matches that migration and target build                                                                              |
| Downgrade across plugin-owned EDA profile metadata                                    | The pre-upgrade backup from before those profiles were saved by the new host                                                                                             |

For transport migration, the app preserves the original as
`kafka-profiles.json.pre-transport-v2`, or an unused numbered generation through
`.pre-transport-v2.99`. It records the chosen basename in `rollbackGeneration` and
keeps that reference on later writes. Read that field without editing the document;
do not choose a generation by filename order or modification time.
An existing `.pre-upgrade.bak` is not a substitute for the indicated generation.

Saving expanded security settings adopts profile envelope version 4 and protected
content version 6. Before converting existing data, the host preserves the exact
previous file as `kafka-profiles.json.pre-security-v3` (or an unused numbered
generation through `.99`) and records it in `rollbackGeneration`. Existing
OAuth/plugin-only profiles keep the older envelope until expanded security is
saved; a file already using version 4 never silently downgrades when credentials
are later removed.

An older app refuses the new format. Browser maintenance also blocks rollback to
the reviewed older image when current profiles or retained profile backups use
version 4. Keep the current host or restore a complete backup from before the
migration using the owned recovery procedure. Do not delete security settings,
backup files or ownership metadata merely to make a compatibility check pass.

Plugin metadata migration is a separate compatibility boundary. Saving a migrated
EDA profile writes a representation older hosts cannot read, even if both builds
say `0.1.0`. A transport snapshot does not necessarily predate that change.

## Restore safely

1. Quit the app. Preserve a separate copy of the current data and all recovery files,
   even if loading them fails.
2. Select the snapshot for the exact target build using the table above. If none
   exists, keep the newer app or plan a fresh setup with reviewed connection values.
3. For a complete rollback, restore the matching full backup into a clean replacement
   directory while preserving file permissions. For a profile-only transport rollback, copy the exact selected
   generation to `profiles/kafka-profiles.json`; preserve the original backup file.
   Other stores and plugins still need to be compatible with the target build.
4. Install the matching app and start it under the same user/credential context.
   Inspect profiles before connecting, then test a known connection.
5. Confirm that the expected saved settings and plugin capability are present.
   Changes after the snapshot will be absent. If restoration fails, stop and retain
   both copies for diagnosis; do not let an older app rewrite newer data.

Never change store schema numbers, reinterpret plaintext profiles as TLS, or decrypt
stores to bypass protection. File migration tests cover generation selection;
recovery on a different OS account or machine is not a verified portability feature.
