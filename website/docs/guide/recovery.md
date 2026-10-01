# Upgrade, back up and recover

Use this procedure before replacing the desktop app or downgrading it. Record the
release tag as well as the app version: `v0.1.0+build.1` identifies the first
release, while `0.1.0` is the app version. The shared app version does not establish identical plugin
behavior or profile compatibility between builds.

The [qualification record](qualification.md) separates recorded results from
platforms and migration scenarios that still need verification.

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

1. Record the installed release tag, OS/CPU, plugin version and target EDA version.
   Retain the matching installer and its verified checksum for recovery.
2. Stop or deliberately keep any EDA capture using the exit prompt. Keeping a
   capture only preserves it until its [lease expires](../plugins/eda.md#stop-update-and-resume).
3. Quit every StreamSkope process using this data directory.
4. Make a dated, access-restricted copy of the **entire** directory outside the
   live app directory. Include `profiles/` and every recovery generation,
   `templates/`, `rules/`, `history/`, `preferences/`, `plugins/` and Electron's
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
   or NSP profile reports it missing. This release uses plugin API 3 packages;
   check the minimum desktop and target version. A stopped EDA capture needs an
   explicit resume.
5. Test a reviewed profile, read a known topic and confirm the expected result.
   Retain the backup until the workflows you use have been verified.

## Choose the correct rollback snapshot

| Situation                                                   | Snapshot to preserve and use                                                                  |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Routine downgrade                                           | Your complete backup made with the target release before upgrading                            |
| Downgrade across explicit TLS/plaintext transport migration | The exact profile filename recorded by `rollbackGeneration` in the version-3 profile document |
| Earlier protected-schema migration                          | `kafka-profiles.json.pre-upgrade.bak`, only when it matches that migration and target build   |
| Downgrade across plugin-owned EDA profile metadata          | The pre-upgrade backup from before those profiles were saved by the new host                  |

For transport migration, the app preserves the original as
`kafka-profiles.json.pre-transport-v2`, or an unused numbered generation through
`.pre-transport-v2.99`. It records the chosen basename in `rollbackGeneration` and
keeps that reference on later writes. Read that field without editing the document;
do not choose a generation by filename order or modification time.
An existing `.pre-upgrade.bak` is not a substitute for the indicated generation.

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
