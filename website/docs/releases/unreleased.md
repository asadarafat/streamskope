---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

These changes follow [v0.7.0](v0.7.0.md). A maintainer assigns the next version when starting release CI.

## Desktop

- Fix profile reconnection after restart on case-insensitive filesystems. App
  settings now live under `workbench/`, separately from Chromium's `Preferences`
  file. Existing valid operational settings, including read-only and masking
  choices, are migrated; original bytes are retained in a migration archive.
  Keep a complete [pre-upgrade backup](../guide/recovery.md) for rollback.
- Allow a protected OS credential service up to ten seconds to initialize at
  startup. The previous one-second limit could report an unlocked Linux keyring
  as unavailable. Initialization still completes immediately when ready and
  refuses unprotected storage backends.

The manual native recovery workflow can build and qualify unreleased installers
without assigning a release version. Candidate results identify their source
revision and artifact hashes; they do not change the qualification of previously
published installers.

See the [current-source qualification record](../guide/qualification.md#current-source-qualification)
for installed EDA/NSP lifecycle results and passing Linux, macOS and Windows
installer recovery. These results do not imply publication
of a new desktop or plugin package.
