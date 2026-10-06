---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

Core NATS live subscriptions, offline plugin management and actionable Kafka
health investigations extend the desktop workbench. Reliability fixes cover
production startup, stream cleanup and cancellation.

## Highlights

- Kafka and Core NATS share one application frame, with provider-specific workspaces.
- Signed portable plugin files and complete cached packages can be reviewed and
  installed without reaching GitHub; compatible plugin publication remains separate.
- Observed health prioritizes selected-topic findings, measurement coverage and
  investigation actions.

## Features

### Core NATS live subscriptions

Select **Core NATS** to create a profile with token authentication and verified TLS,
subscribe to wildcard subjects, and inspect original payloads, headers and reply
subjects. Native profiles use protected credential storage when available;
development profiles are session-only. Viewer eviction, application omissions
and transport omissions are reported separately. Stop and disconnect require
confirmed cleanup before switching providers.

Core NATS provides live receipt only: no retained message history, Kafka offsets,
subject inventory or historical replay. EDA and NSP remain Kafka connection
plugins. See [Core NATS subscriptions](https://asadarafat.github.io/streamskope/guide/core-nats/).

### Offline plugin installation and network controls

**Install from file** reviews a publisher-signed portable package without GitHub
access. The host authenticates the complete payload against shipped Ed25519
public keys before parsing or execution. Complete cached packages have a separate
exact-version installation path; cached catalog metadata alone cannot install a
new package. Review receipts expire and bind the candidate to the current active
work. File, cache and catalog installation share cleanup, activation, recovery and
idempotency.

Installed-state management remains available when catalog discovery fails.
Removal and local activation retry do not require a successful catalog refresh.
A cached catalog shows its observation date rather than claiming current
availability. Failed activation retains recoverable state, and retries verify
the retained package and its compatibility again.

Plugin downloads use the system proxy or a configured HTTP/HTTPS proxy. Proxy
credentials are scoped to that proxy and OS-protected when available; otherwise
they remain session-only. Download progress and cancellation are separate from
installed-plugin lifecycle actions. Explicit offline mode blocks **remote** plugin
acquisition while file/cache installation and local provider connections remain
available. See [offline plugin installation](https://asadarafat.github.io/streamskope/plugins/offline/).

This desktop adds signed portable delivery support; compatible plugin packages
must be published separately. Older downloads are not retroactively signed, and
desktop publication does not change their API or desktop-version bounds.

### Reusable development fixtures

The source launcher prepares a private, digest-pinned `aio-nats` lab alongside
`aio-kafka`, with verified TLS, token authentication and a session-only profile.
Start a subscription before publishing bounded generated samples. Persistent and
disposable NATS tests share one server definition while keeping separate ownership
and cleanup. An on-demand real-server 60-second soak supplements the existing
pipeline soak. See [Source workbench](https://asadarafat.github.io/streamskope/start/development/).

## Improvements

### Kafka observed health

Connected-profile resource selection, visible collection progress and cooldown,
partition filtering and sorting, and group/topic/exact-record drilldowns reduce
manual investigation. Host loss stops collection and marks previous measurements
as retained evidence; recovery requires a new capture before resource links become
actionable.

Collection errors retain specific safe recovery reasons. Selected-topic lag is
independent of unrelated group-member or assignment omissions. Record sampling
uses bounded adaptive windows; incomplete coverage does not qualify key or size
inference. Existing schema-1 history remains readable. See
[Observed health](https://asadarafat.github.io/streamskope/guide/observed-health/).

### Provider and host ownership

The shared shell owns navigation presentation and layout. Registered provider
routes, events and shutdown ownership remain isolated. Provider switching waits
for confirmed stop and disconnect, retains the current workspace after cleanup
failure, and blocks commands from retired views while preserving already-admitted
write receipts and cleanup events.

Desktop event failures mark the affected stream unavailable. Replacement
subscriptions cannot acknowledge events from an earlier subscription. Missing
native NATS support is explicitly unavailable.

## Fixes

- Production renderer builds preserve React and Material UI initialization order
  across lazy workspaces. Partial forced vendor chunks could leave the desktop
  window blank before the application loaded. Required CI now exercises the
  minified production renderer as well as development workflows.
- Shutdown rejects new requests immediately and waits for every owned cleanup,
  including environment comparisons that use a separate saved destination. One
  failed cleanup cannot make shutdown finish while another is still running.
- Cancellation preserves acknowledged or uncertain write outcomes and does not
  automatically retry a promotion. Cancelled Kafka reads and latency probes keep
  started driver promises observed through settlement, avoiding unhandled rejections.
- When the final development-browser client leaves, its host stops the reader and
  confirms cleanup before accepting further commands. Desktop event failure uses
  the same stop rule. The broker connection remains available; failed cleanup
  requires restarting the host. Transport queues account for records held by an
  outstanding write, and HTTP omission counts stay with their read generation.

## Upgrade and compatibility

The desktop host protocol advances from **48 to 52**. Update renderer and host
together; the desktop installer includes both. Plugin API **4** is unchanged.
This release does not publish or upgrade EDA/NSP plugin packages.

Existing published API 3 packages cannot become API 4 packages through a desktop
upgrade. Source API 4 manifests also retain their declared desktop bounds; those
bounds currently exclude desktop 0.9.0. These API 4 source packages require a
separate plugin release whose manifest supports 0.9.0. Existing compatible
packages retain their own requirements and supported API generation. Check requirements in
**Preferences → Plugins** rather than inferring compatibility from this desktop
version. Ordinary Kafka and Core NATS connections do not require capture plugins.

Back up the full application-data directory before an installed upgrade. Follow
[Upgrade, back up and recover](https://asadarafat.github.io/streamskope/guide/recovery/)
for protection, recovery and rollback limits.

## Qualification and limits

The [source-bound qualification record](https://asadarafat.github.io/streamskope/guide/qualification/#current-source-qualification)
retains exact revisions, executed checks and earlier failed attempts. Local
qualification after v0.8.0 passed shared static/types and unit/integration checks,
real Kafka/NATS cases, browser workflows, configured live EDA/NSP checks, six native
Linux ARM64 scenarios, and both 60-second soaks. The real NATS soak delivered all
60,000 records without duplicates, invalid records or omissions; the pipeline soak
passed its unchanged budgets.

Native local evidence uses Linux ARM64 source bundles with genuine GNOME credential
storage. It does not qualify new macOS/Windows installers, installed upgrade/rollback
or OS process sandbox enforcement. Native installer checks are recorded by the
release build separately; publication does not establish unexecuted tests.

EDA used the existing capture application. Native plugin catalogs were isolated
local fixtures; update packages used the same code with newer manifests. NSP API
certificate verification was disabled in the lab; Kafka verification was enabled.
Neither soak measures renderer interaction or native IPC latency, and the NATS
soak does not measure server resource use or internal queue high-water marks.

Release CI assigns the desktop version in its build checkout. Plugin releases remain
independent; their pending notes live in each plugin's release commentary.
