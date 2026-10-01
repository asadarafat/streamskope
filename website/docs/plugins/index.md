# Plugins

Plugins add connection setup and management for a particular platform to
StreamSkope. Install one when you want the app to discover that platform's Kafka
endpoints, obtain connection material, or manage a temporary capture for you.
Ordinary Kafka connections work without a plugin.

The plugin adds its own action under **Add connection**, a setup dialog, and
actions for its saved profiles. Once connected, you use the same **Topics**,
message reader, consumer lag and other workbench views as any Kafka connection.

## Choose a plugin

| Plugin                | What you supply                                                 | What it creates                                                                                                       | Where Kafka traffic goes                                                                              |
| --------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| [EDA Capture](eda.md) | EDA API URL and credentials, then a Producer or ClusterProducer | A temporary capture profile backed by a separate broker and exporter, or a normal profile for an existing destination | Temporary capture: through an authenticated EDA tunnel; existing destination: directly to its brokers |
| [NSP Capture](nsp.md) | NSP API URL and credentials                                     | A reusable profile with the retrieved Kafka CA truststore and, when required, NSP OAuth settings                      | Directly to the existing NSP Kafka brokers                                                            |

The individual guides include prerequisites, call flows, installation steps and
cleanup/recovery behavior. **Capture** does not mean that both plugins create a
broker: EDA can create a temporary destination, while NSP connects to an existing
one. Neither plugin creates source events for you.

## How a plugin extends a connection profile

```text
User: Add connection
  |
  v
Plugin setup dialog
  | URL + API credentials
  v
Desktop plugin/host
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

The desktop host performs the API requests, holds secret connection material and
uses StreamSkope's profile validation and protected storage. Truststores and
retrieved passwords are not returned to the plugin dialog. The saved profile
records its plugin owner and platform metadata so that refresh, resume and
cleanup actions remain attached to the correct connection.

Plugin installation grants no platform permissions by itself. Your platform
account must be authorized for the operations described in that plugin's guide;
Kafka access and broker network reachability still have to work independently.

## Install and manage plugins

1. Open **Preferences → Plugins** and refresh the catalog if needed.
2. Select **Install** for a compatible plugin. Its connection action becomes
   available immediately; a desktop restart is not required.
3. Follow the plugin's guide to create a profile, then connect and inspect a topic.

The desktop uses the official StreamSkope release catalog on GitHub. Installation
and updates need access to GitHub; an installed plugin can load without it.
Packages are checked for compatibility and integrity before activation. This is
an official plugin catalog, not a general marketplace or a sandbox for arbitrary
third-party code. Plugin backend code runs in the trusted application host.

| Action                        | Expected behavior                                                                                                     |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Update                        | Activates without restart; asks to stop affected work and complete required cleanup first                             |
| Failed download or activation | Keeps or restores the previous working installation when available; stopped platform work may need an explicit resume |
| Remove                        | Requires the plugin's active work to stop and cleanup to succeed before removing its code                             |
| Cleanup fails                 | Keeps the plugin available for recovery; inspect its error and retry using the relevant guide                         |
| Reinstall                     | Restores the plugin UI for retained profiles                                                                          |

Removing a plugin retains its saved profiles. Profiles owned by that plugin need
a compatible installation before reconnecting; **Open plugins** leads back to
the catalog. A normal profile created from EDA's existing Kafka destination has
no managed capture lifecycle. Removing the desktop plugin also does not uninstall
EDA's cluster app or delete NSP's shared helper workflow.

## Versioning and compatibility

The API 3 package format uses one convention for all connection plugins:
minimum desktop release, target system, inclusive target version bounds and
package revision. The [manifest-derived source declarations](versioning.md#declared-packages)
show the current identities without maintaining another copy here.

Read [Versioning and compatibility](versioning.md) to interpret an identity,
plan a target upgrade and distinguish component versions. For desktop and
plugin requirements, use the [compatibility matrix](../start/compatibility.md).
