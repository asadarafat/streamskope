---
title: NATS live subscriptions
---

# Inspect a NATS live subscription

Use the documentation for your installed desktop release. The site notice
identifies the source revision and whether this is development or published
documentation. Development instructions may describe features absent from an
older installer; merging source does not update its release documentation.

**NATS** is a built-in sibling of **Kafka**. Select it with **Messaging
provider** in the application header. EDA Connector and NSP Connector are optional
Kafka connection plugins; installing either does not add or configure NATS.

StreamSkope connects to an external NATS server using the endpoints and
authentication you supply. The desktop does not start or embed a NATS server,
and this view does not inspect an internal StreamSkope message bus. A remote
server can be on your network or reachable through your site's authorized route.

You need a reachable server, a known subject or wildcard, permission to subscribe,
and an independent producer of new events. The workspace reads live NATS
subscriptions. It can connect to a server with JetStream enabled, but it does not
manage JetStream or read stored stream history. Subject inventory, publishing,
replay and Kafka-style partitions/offsets are outside this workspace's scope.

## Save and connect a profile

1. Open **Connection Profiles**, then **Add NATS profile**.
2. Enter **Profile name** and **NATS servers** using the endpoints supplied by
   your server operator. Credentials belong in the authentication fields, never
   in a server URL.
3. Choose no authentication or **Token**. Supply a token authorized for the
   subjects you intend to read. Username/password, NKeys, JWT credentials and
   mutual-TLS client certificates are outside this workspace's scope.
4. Choose plaintext only when your server operator explicitly supplies it, or
   **Verified TLS**. TLS always verifies the server certificate and hostname.
   Enter **CA certificate PEM** for private trust, or use system trust when the
   server certificate is accepted by the host's trust store. TLS failure does
   not fall back to plaintext.
5. Select **Save profile**, then the profile's **Connect** action. Wait for
   **Connection status** to show connected before starting a subscription.

The storage capability displayed with the profiles is authoritative. Native
profiles use durable operating-system-protected storage when available.
Development-browser profiles are **session/memory** and are lost when the
development host restarts. An unavailable protected store requires recovery;
the app does not silently write unprotected durable credentials.

Profile summaries contain secret-presence flags, never a hydrated token or CA.
When editing an existing token/CA, a blank replacement field retains its saved
value. Deliberately replacing a value updates it; selecting no authentication
clears the token, and selecting system trust clears a saved private CA. Dismissing
or successfully saving the editor clears its secret buffers. Profiles in use
cannot be changed or deleted. Revision conflicts require refreshing the profiles
and reviewing the current entry before retrying.

## Start, inspect and stop

1. Open **Live Subscription**.
2. Enter **Subject filter**, such as `orders.created`, `orders.*` for one token,
   or `orders.>` for one or more remaining tokens. Use a subject allowed by the
   server account.
3. Select **Start subscription**. Loading becomes streaming only after the host
   confirms subscription interest with the server. Publish new traffic from
   your existing producer; an empty grid does not prove no traffic exists.
4. Select a row in **NATS records** to open **Record inspector**. Arrow keys navigate
   cells; press Enter to inspect the focused record. The inspector shows the original UTF-8 or canonical base64,
   payload byte count, subject, optional reply and ordered case-sensitive
   multi-value headers. A zero-byte payload is present and empty. Any pretty
   JSON view is prepared on demand, with at most 32 container levels and 1 MiB
   of formatted output; the original remains the evidence when formatting is
   unavailable.
5. Select **Stop subscription** and wait for stopped status. The host confirms
   subscription cleanup before reporting success. Stopped rows remain labelled
   as retained evidence. Already-emitted batches may arrive for that stopped
   generation; they do not reopen the subscription. A new subscription starts
   a fresh generation/window.
6. Select **Disconnect** when finished, and wait for disconnected status.

The displayed received time is UTC from the StreamSkope host receiving the
record. It is neither a producer timestamp nor proof of broker-wide ordering.
Header truncation is explicit. Payloads and headers have host limits, and the
browser retains at most 1,000 records and 8 MiB of serialized UTF-8 evidence.
The earlier limit evicts the oldest records.
Records from an earlier subscription cannot enter a new window. If viewer
eviction removes a selected row, the selection and inspector clear with an
explicit notice to select a retained record.

## Interpret missing records and recover

Omission counters describe separate stages:

| Counter               | Meaning                                                            |
| --------------------- | ------------------------------------------------------------------ |
| Application omissions | Records the host could not retain in its bounded application queue |
| Transport omissions   | Records omitted from that client's bounded delivery path           |
| Viewer omissions      | Records evicted from the browser's live window                     |

Published records means records emitted by the host's delivery stage; it does
not prove every client processed them. These counters do not measure all broker
traffic or establish zero loss. This live subscription provides no replay for missed records.
Restarting a subscription receives future traffic only.

An authentication error needs an accepted token; a TLS error needs the correct
hostname and trust; a permission error needs subscription rights for the subject.
Do not disable verification or widen server permissions merely to clear an
error. Reconnection is explicit. When the backend becomes unavailable, new
operations stop and old rows remain evidence; follow its recovery message and
connect/start a new subscription after recovery.

Changing **Messaging provider** first stops the current stream and disconnects.
If either cleanup fails, StreamSkope keeps the original workspace selected and
offers retry. Successful switching retires its old callbacks while preserving
commands already admitted and their actual receipts. Switching does not delete
profiles or remote capture resources. A native host that lacks the NATS port
shows an unavailable workspace; it never redirects NATS to Kafka or HTTP.

## Try a separate local development server

For a source checkout, the [local development walkthrough](../start/development.md#try-local-nats)
provides a separately owned token/TLS server and **Local AIO NATS** profile.
That fixture is a test server run by development tooling, not a desktop dependency
or an internal StreamSkope transport. Subscribe to `streamskope.fixture.>` before
using the walkthrough's publisher command. Each invocation sends a finite
sample batch; it does not seed retained history. The desktop workspace remains
read-only.

## Qualification boundary

The repository's real-server scenarios use a pinned NATS 2.15.0 token/verified-TLS
fixture and the actual product renderer/provider registry. Controlled tests
exercise admission, stale responses and bounded viewer behavior. Record executed
results for the exact source revision; this guide does not claim a published
release, support for every managed NATS service, or native credential-storage
qualification from a development-browser run. See
[Qualification evidence](qualification.md) for release-specific evidence.
