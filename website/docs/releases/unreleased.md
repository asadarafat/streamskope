---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

The source adds a production browser host with an explicitly unlocked encrypted
credential vault, durable Kafka and NATS profiles, and signed portable plugin
file selection. Locking disconnects providers before releasing the vault. This
is a single-owner Linux host; desktop operating-system credential protection
keeps its existing behavior.

Containerlab image and launch packaging are being qualified separately. No
published release currently supplies this browser host or topology. Browser
plugin downloads currently use direct networking; desktop custom proxy support
is unchanged, and signed portable files remain the offline path.

Desktop versions are assigned during release CI. Pending plugin changes remain
in each plugin's release commentary and are released independently.
