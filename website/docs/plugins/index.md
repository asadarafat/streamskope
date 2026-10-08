---
plugin_scope: all
---

# Plugins

Plugins add connection setup and management for a particular platform to
StreamSkope. Install one when you want the app to discover that platform's Kafka
endpoints, obtain connection material, or manage a temporary capture for you.
Ordinary Kafka connections work without a plugin.

Read the **Plugin availability** notice above before following a setup guide.
It separates the source behavior described here from packages available for the
documented desktop. Desktop and plugin publication are independent; installing a
new desktop does not make an unreleased plugin update available.

The plugin adds its own action under **Add connection**, a setup dialog, and
actions for its saved profiles. Once connected, you use the same **Topics**,
message reader, consumer lag and other workbench views as any Kafka connection.

A compatible [Containerlab browser host](../start/containerlab.md) exposes the
same plugin connection actions and hot lifecycle. Its API and broker traffic
originate in the container; plugins do not run platform requests in the browser.
A signed file can be transferred through its authenticated browser picker.
Development identity and published package compatibility still apply.

## Choose a plugin

| Plugin                  | What you supply                                                 | What it creates                                                                                                       | Where Kafka traffic goes                                                                              |
| ----------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| [EDA Connector](eda.md) | EDA API URL and credentials, then a Producer or ClusterProducer | A temporary capture profile backed by a separate broker and exporter, or a normal profile for an existing destination | Temporary capture: through an authenticated EDA tunnel; existing destination: directly to its brokers |
| [NSP Connector](nsp.md) | NSP API URL and credentials                                     | A reusable profile with the retrieved Kafka CA truststore and, when required, NSP OAuth settings                      | Directly to the existing NSP Kafka brokers                                                            |

The individual guides include prerequisites, call flows, installation steps and
cleanup/recovery behavior. Both connectors set up Kafka access through a platform
API. EDA additionally offers **Temporary capture**, which creates a separate
destination; NSP connects to its existing Kafka broker. Neither connector creates
source events for you.

Connector names describe the setup experience. Temporary capture, credential
refresh and resource removal retain their specific labels because they have
different effects. Existing installations and older releases may still show
**EDA Capture**, **NSP Capture**, **Capture from EDA** or **Connect to NSP**.
Display-name changes keep the same plugin identities, saved profiles and recovery
records; they require a compatible plugin update rather than reinstalling under
a new identity.

## How a plugin extends a connection profile

```text
User: Add connection
  |
  v
Plugin setup dialog
  | URL + API credentials
  v
Application plugin/host
  | Platform API calls
  | Discover or prepare Kafka
  v
Core: test + save profile
  |
User: Connect
  |
  v
Core Kafka client -> Broker
  |
  v
Topics, messages, consumer lag
```

The application host performs the API requests, holds secret connection material and
uses StreamSkope's profile validation and protected storage. Truststores and
retrieved passwords are not returned to the plugin dialog. The saved profile
records its plugin owner and platform metadata so that refresh, resume and
cleanup actions remain attached to the correct connection.

Plugin installation grants no platform permissions by itself. Your platform
account must be authorized for the operations described in that plugin's guide;
Kafka access and broker network reachability still have to work independently.

## Install and manage plugins

1. Open **Preferences → Plugins** and select **Check for updates** if needed.
2. Select **Install** for a compatible plugin. Its connection action becomes
   available immediately; a desktop restart is not required.
3. Follow the plugin's guide to create a profile, then connect and inspect a topic.

The desktop uses the official StreamSkope release catalog on GitHub. Catalog downloads need access to GitHub. You can also use **Install from file**
with a publisher-signed portable package, or select a complete verified package
already in the local cache. Every new installation has a review that pins its
exact version and bytes before applying the same hot lifecycle.
Read [Install without GitHub](offline.md) for transfer, trust and target prerequisites.
An installed plugin can load without internet access.
Installed state appears independently of catalog discovery, so a stalled catalog
does not block **Remove** or **Retry activation**. Retry verifies and reloads the
retained installed package locally; it does not download a replacement.

After a successful catalog check, the host retains a bounded local catalog.
If GitHub cannot be reached, **Available** shows that cached catalog and the last
successful check time. Cached entries describe the previous check, not current
availability or an assurance that the installed version is up to date. A cached
catalog is metadata; it does not make an undownloaded package available offline.
Local file imports require a trusted publisher signature; cached archives are checked
again. Packages are checked for compatibility and integrity before activation. This is
an official plugin catalog, not a general marketplace or a sandbox for arbitrary
third-party code. Plugin backend code runs in the trusted application host.

| Action                        | Expected behavior                                                                                                     |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Update                        | Activates without restart; asks to stop affected work and complete required cleanup first                             |
| Failed download or activation | Keeps or restores the previous working installation when available; stopped platform work may need an explicit resume |
| Remove                        | Stops affected work before changing the installation, then closes the old plugin instance                             |
| Cleanup fails                 | Reports the failure and retains recovery records; the installed version may already have changed or been removed      |
| Reinstall                     | Restores the plugin UI for retained profiles                                                                          |
| Retry activation              | Verifies retained package bytes and compatibility, then reloads locally without a GitHub request                      |

### When a change takes longer

An installation, update or removal shows its current stage in **Preferences →
Plugins**, including time spent in that stage and outstanding requests or
connections. A queued change is waiting for another plugin operation. A waiting
stage means the host is still waiting for the actual operation; it does not mean
the operation was cancelled or that cleanup finished.

You can close and reopen Preferences to inspect the same change. Its status does
not depend on GitHub being reachable. Controls for a conflicting change to that
plugin remain unavailable until the operation settles. Download cancellation
applies to acquiring package bytes, not to an installation that has already begun
changing the running plugin.

Installation state and cleanup state are distinct. Until storage completion is
confirmed, files may already have changed. Once the storage change is saved, the
previous instance can still be closing.
Removing code does not prove that every remote resource was removed. Inspect the
plugin's error and its guide before retrying or changing the application host.
Keep saved profiles and recovery records until the platform cleanup is confirmed.

If you try to exit while another plugin change is pending, finish that change
before reviewing the exit action. This keeps the exit decision tied to the
plugins that will actually close.

The status warning never forces a replacement, skips cleanup or automatically
retries a platform operation. A plugin that blocks the host process itself can
also prevent status updates. During application startup, the desktop or browser
host still waits for backend activation before opening the workbench; these
status views do not bypass that startup requirement.

Removing a plugin retains its saved profiles. Profiles owned by that plugin need
a compatible installation before reconnecting; **Open plugins** leads back to
the catalog. A normal profile created from EDA's existing Kafka destination has
no managed capture lifecycle. Removing the desktop plugin also does not uninstall
EDA's cluster app or delete NSP's shared helper workflow.

## Versioning and compatibility

The API 4 format gives each plugin an independent Semantic Version.
Its manifest separately declares an inclusive minimum/exclusive maximum desktop
interval, plugin API, target system and inclusive target versions. The
[manifest-derived source declarations](versioning.md#declared-packages) show these
requirements. API 4 support in the desktop does not mean an API 4 plugin package
has been published. The original published release retains API 3 packages; see the
[migration steps](versioning.md#upgrade-from-the-original-packages).

Read [Versioning and compatibility](versioning.md) to interpret requirements,
plan a target upgrade and distinguish component versions. For desktop and
plugin requirements, use the [compatibility matrix](../start/compatibility.md).
