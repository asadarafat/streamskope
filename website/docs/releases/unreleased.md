---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

## Fixes

The offline plugin guide now distinguishes installable catalog packages from
publisher-signed portable files. It lists exact downloads only after verifying
their published assets, signature, compatibility and package contents; otherwise
it records that the selected compatible catalog package has no portable download.
An illustrative filename no longer sends users searching desktop installer
releases for a nonexistent plugin asset.

EDA Capture and NSP Capture publish independently from the desktop. Their source
API 4 requirements now cover stable 0.9.x hosts, with exact EDA 26.8.2 and NSP
26.4.0 target requirements. Compatible signed portable files must be published
through each plugin's release workflow before this desktop's Pages snapshot can
list them. Existing immutable plugin packages keep their original requirements.

## Upgrade and qualification

Transactional plugin browser checks use the supported reduced-motion interface,
and disposable Kafka replication brokers bound their log-cleaner buffers within
the fixture heap. These qualification changes preserve the exercised workflows.

Host protocol 52 and plugin API 4 remain unchanged. Installer upgrades and
rollback are not qualified by documentation checks. The qualification report
records actual package, native file-import and live target evidence separately.

Desktop versions are assigned during release CI. Pending plugin changes remain
in each plugin's release commentary and are released independently.
