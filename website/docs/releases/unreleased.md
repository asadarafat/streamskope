---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

This patch fixes profile recovery and protected credential startup after [v0.7.0](v0.7.0.md).

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

## Qualification and compatibility

See the [current-source qualification record](../guide/qualification.md#current-source-qualification)
for installed EDA/NSP lifecycle results and passing Linux, macOS and Windows
installer recovery. Those rehearsals used published 0.6.0 as the baseline and a
source candidate with the same application code as this patch; they verified
candidate restart and full backup restoration under the same OS account.
They do not claim the old baseline's restart succeeded or qualify cross-account
credential transfer. Linux FUSE/launcher integration remains outside that test.

The host protocol and plugin API are unchanged. Desktop publication does not
republish EDA/NSP plugins or change the compatibility manifests of existing packages.
