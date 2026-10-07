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

Core release CI prepares a public AMD64/ARM64 image in
`ghcr.io/asadarafat/streamskope` and an online Containerlab topology pinned to its
version and digest. Optional offline delivery provides two gzip-compressed Docker
save archives, a separate local-image topology and a combined registry/image
manifest, with shared release checksums. The root source topology remains offline
and uses the development image. Existing published releases are not
retroactively repackaged. Use [Run with Containerlab](https://asadarafat.github.io/streamskope/start/containerlab/)
for source instructions and release-derived availability. Desktop custom proxy
support keeps its existing behavior.

EDA and NSP Connector 0.1.2 extend the supported core interval to
`>=0.9.0, <0.11.0`, retaining plugin API 4 and the exact
EDA 26.8.2 and NSP 26.4.0 targets. Update compatible plugins before upgrading the
desktop to 0.10.0; offline users should obtain the signed portable files first.
These declarations do not imply completed live-target qualification.

Desktop versions are assigned during release CI. Plugins keep their own release
commentary and are released independently.
