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

Containerlab source deployment includes a pinned non-root Linux image, private
persistent data and a browser-unlocked vault. Offline plugin download policy
survives locks and restarts; corrupt policy blocks remote acquisition. Browser
plugin downloads use direct networking; signed portable files and verified cache
remain available without GitHub.

Core release CI prepares native AMD64/ARM64 Docker-save archives, matching
topology, image identity and checksums. Existing published releases are not
retroactively repackaged. Use [Run with Containerlab](../start/containerlab.md)
for source instructions and release-derived availability. Desktop custom proxy
support keeps its existing behavior.

Desktop versions are assigned during release CI. Pending plugin changes remain
in each plugin's release commentary and are released independently.
