---
plugin_scope: eda
---

# EDA Connector

<span id="capture-from-eda"></span>

Use EDA Connector to inspect an existing exporter destination or create a temporary
destination without changing the original exporter. Start with a running desktop
app and review [Security and permissions](../guide/security.md#eda-authorization) with your
EDA administrator.

EDA Connector is an optional [connection plugin](index.md). It adds **Connect via
EDA** to **Add connection** and resume/status/cleanup actions to managed capture
profiles. The EDA cluster app supplies the temporary broker and tunnel endpoint;
the desktop plugin supplies the setup experience and local tunnel.

## Before you start

- Open a supported [desktop and plugin release](../start/compatibility.md).
- Have an EDA HTTPS origin, API credentials and an exporter that emits the topics
  you need. Check [API certificate trust](../guide/tls-trust.md#eda-and-nsp-api-certificates);
  the dialog cannot upload a private CA.
- For temporary capture, ask the administrator to confirm the
  [cluster app, permissions, image access and capacity](#administrator-prerequisites).
  Existing Kafka access instead needs its broker network route and Kafka credentials.

## Install the desktop plugin and discover sources

1. Open **Preferences → Plugins**, then **Install** on **EDA Connector**. Activation
   is immediate. Catalog refresh and download need GitHub access; an installed
   plugin can load without GitHub access.
2. Open **Add connection → Connect via EDA**. Enter the EDA HTTPS origin and
   credentials, keep TLS verification enabled, then choose **Discover sources**.
3. Select a source and review its Kafka destination. Discovery reads EDA and does
   not deploy a capture or require local Kubernetes credentials.

If no compatible plugin appears, distinguish an empty catalog from a failed
refresh in the displayed error. Check GitHub access, publication and plugin API
compatibility before retrying.

### Choose the right source

| Source kind     | Meaning                      | What to check                                                       |
| --------------- | ---------------------------- | ------------------------------------------------------------------- |
| Producer        | Exporter in an EDA namespace | Match the namespace, resource name and listed export topics         |
| ClusterProducer | Cluster-scoped exporter      | Match the resource name and export topics; it is not a Kafka broker |

Select the exporter for the data you need, not the kind that sounds more powerful.
For example, a namespaced Producer `eda/kafka-export` may export `eda-nodes` and
`eda-current-alarms`; choose it only if those are the intended streams. Names and
topic lists depend on your EDA configuration. The two kinds are exporter resources,
not interactive Kafka message-production tools.

Discovery lists named exports from `spec.exports[].topic`. It excludes resources
without named exported topics and StreamSkope-owned capture copies. If discovery
is empty, have the administrator verify API read access, exporter kind/scope and
its export definitions. A successful API sign-in alone does not establish those.

### Use existing Kafka

Choose **Connect to existing Kafka** to open a normal profile with the discovered
broker addresses. Supply Kafka credentials and trust, test, save and connect.
EDA sign-in does not grant Kafka access. Internal broker addresses need an
authorized network route from the desktop, such as the site's VPN.

### Start temporary capture

1. Choose **Set up temporary capture**. If the cluster app is missing, complete
   **Install and continue** with an authorized administrator.
2. Review the source and local port (default `19092`), then choose **Start capture**.
3. Wait for the temporary broker and authenticated tunnel. StreamSkope tests the
   endpoint and creates the capture connection profile.
4. Connect the saved capture profile, open an exported topic and inspect a record. If it is empty, check source
   activity and your filters before assuming deployment failed.

The cluster app creates a broker, Service and copied exporter. The original
exporter and its destination remain unchanged. The local loopback endpoint belongs
to the desktop tunnel; it is not a route other machines can use directly.

**Only one unexpired capture session is accepted across the cluster.** Coordinate
with other operators. The eight-connection tunnel limit is not permission for eight
capture sessions. A session conflict requires identifying the current session and
its owner; repeated start attempts do not free it.

**Expected result:** the profile connects, the selected export topics become
available, and a source emission supplies a record. Capture does not generate
fixture data. If Ready is empty, clear filters and use a suitable read mode; then
check the original source's export health and emission schedule. If no topics
appear, check the copied exporter's status and broker endpoint with the EDA
administrator. Changing a local port cannot make a quiet source emit data.

## Stop, update and resume

| Action                                   | Result                                                                                                      |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Close a topic                            | Capture continues                                                                                           |
| Disconnect Kafka                         | Client disconnects; capture remains                                                                         |
| Stop and remove capture                  | Removes owned resources and discards temporary messages; disconnect Kafka first                             |
| Update/remove the plugin during EDA work | Confirmation stops work and requests cleanup; cancelling keeps the current plugin and work                  |
| Remove desktop plugin                    | Removes installation files after cleanup; retains saved profiles and the cluster app                        |
| Keep capture and exit                    | Stops desktop renewal; temporary resources remain only until the current lease expires and cleanup succeeds |
| Resume a saved profile                   | Requests EDA credentials again; resumes a valid session or starts a new one after expiry                    |

The desktop requests a **900-second (15-minute) lease** and renews it every
**300 seconds (5 minutes)** while active. After exit or a lost connection, expiry
is measured from the last successful renewal, not from the next app launch.
Keeping a capture does not preserve messages overnight. A new session cannot
restore the old broker's temporary data.

Plugin packages use independent Semantic Versions, so a newer compatible version appears
as an update in **Preferences → Plugins**. Confirm cleanup of active captures
first. Check both desktop bounds before upgrading: a newer host can also fall
outside a plugin's supported interval.

The cluster app treats removal of a confirmed absent session as success, so an
expired or previously removed capture can be cleared locally. Authentication
failures, an unavailable API or failed resource deletion still report errors. Updating the desktop/plugin alone
does not replace the running cluster agent.

Installation, updates, removal and reinstall work without restarting the desktop.
Failed downloads leave the installed plugin intact. Failed activation restores the
previous working version when available, but a stopped capture needs an explicit
resume. Cleanup failure keeps the plugin available for retry. Unrelated Kafka
connections remain available. Missing-plugin profiles offer **Open plugins**.

Lease expiry makes resources eligible for cleanup; the running agent still has to
reconcile and delete them. An unreachable API, stopped agent or denied deletion
can delay cleanup. Do not treat elapsed time or a saved profile as proof of removal.

## Administrator diagnosis and cleanup verification

Ordinary capture uses the EDA API. The following **read-only Kubernetes checks**
are for an administrator with a separately authorized cluster context. Confirm
the context before running them; they do not install or delete anything.

```sh
kubectl config current-context
kubectl -n eda-system get deployment streamskope-capture-agent
kubectl -n eda-system logs deployment/streamskope-capture-agent --tail=100
kubectl get capturesessions.capture.streamskope.io -A \
  -o custom-columns='NAMESPACE:.metadata.namespace,ID:.metadata.name,PHASE:.status.phase,EXPIRES:.spec.leaseExpiresAt,SOURCE:.spec.source.name'
```

Expect one available agent replica. For a current capture, inspect its exact
namespace, UUID, phase and expiry. `Ready` confirms the agent's readiness checks;
receipt of an expected Kafka record confirms the data path. If another user's
unexpired session exists, coordinate its stop rather than deleting it.

For a selected session, replace both example values below from that inventory:

```sh
capture_namespace='REPLACE_WITH_SESSION_NAMESPACE'
capture_id='REPLACE_WITH_SESSION_UUID'
capture_selector="app.kubernetes.io/managed-by=streamskope-capture-agent,capture.streamskope.io/session=$capture_id"
kubectl -n "$capture_namespace" get capturesessions.capture.streamskope.io "$capture_id" -o yaml
kubectl -n "$capture_namespace" get services,statefulsets -l "$capture_selector"
kubectl -n "$capture_namespace" get pods -l "capture.streamskope.io/workload=streamskope-capture-${capture_id%%-*}"
kubectl get producers.kafka.eda.nokia.com,clusterproducers.kafka.eda.nokia.com -A -l "$capture_selector"
```

After **Stop and remove capture**, the selected session should be absent, as should
its labelled Service, StatefulSet, exporter copy and broker pod once termination
completes. The original exporter must still exist. The broker uses `emptyDir`, so
its temporary data is discarded with its pod; there is no capture PVC to retain.
If a resource remains, inspect agent logs, workload events and EDA installation
health. Repair the underlying access/readiness issue and retry normal cleanup.
Do not strip finalizers or delete resources by a broad name prefix; ownership
conflicts require administrator investigation.

| Symptom                              | Next check                                                                                     |
| ------------------------------------ | ---------------------------------------------------------------------------------------------- |
| EDA version mismatch                 | Compare the API product version with the exact plugin target; install matching components      |
| Package requires a newer desktop     | Check its supported StreamSkope interval and plugin API before updating                        |
| Catalog/key conflict or app absent   | Inspect the existing catalog, trust key and signed publication before authorizing installation |
| Broker pending or image pull failure | Inspect pod events, node capacity and registry access                                          |
| Another capture is active            | Identify the unexpired session and coordinate with its operator                                |
| Tunnel failed/local port occupied    | Check EDA WebSocket routing, authentication, TLS trust and local port availability             |
| Ready but no records                 | Check that the source is producing and that filters/read mode match the expected data          |
| Expired session/resources remain     | Confirm agent health, reconciliation logs and deletion permissions; verify removal after retry |

Share the release/plugin/EDA versions, failing stage and redacted session/correlation
IDs. Keep credentials, payloads and full resource dumps out of public reports.

## Components and compatibility

The desktop plugin adds discovery, setup and a local tunnel. The separately
installed **StreamSkope Capture** cluster app (`capture.streamskope.io`) supplies
the temporary broker and its tunnel endpoint. Review the released component
versions in the [compatibility matrix](../start/compatibility.md).

The plugin reads `GET /core/about/version` before discovery, app installation and
capture. A timestamp/Git build suffix is separate from the product release.
Installing the desktop plugin does not establish that the matching signed cluster
app is published or installed. An unsigned OCI artifact from release CI is a
development artifact, separate from the signed App Store publication.

### Compatibility package

The source API 4 package targets **EDA 26.8.2**. Its version is assigned when
the plugin release workflow starts. See
the [manifest-derived declarations](versioning.md#declared-packages) for its
development identity, desktop bounds and inclusive target range. The official
release catalog establishes which packages are available to install.

A desktop plugin version does not rename the running EDA product or cluster
app. A compatible desktop plugin still requires the matching signed cluster app
and its readiness check.

The [source-bound rehearsal record](../guide/qualification.md#current-source-qualification)
describes the installed API 4 lifecycle exercised on Linux ARM64 against an
already installed EDA 26.8.2 cluster app. That result does not qualify a fresh
cluster-app installation or a published plugin upgrade.

## Administrator prerequisites

Before approving cluster installation, check:

- The desktop can reach EDA's HTTPS origin and verify its certificate using
  [the supported API trust](../guide/tls-trust.md#eda-and-nsp-api-certificates). Authentication, the
  API proxy and WebSocket upgrades must work through any intervening gateway.
- The account can read EDA's version and Kafka source inventories. Capture users
  need the proxy operations listed in the [permission table](../guide/security.md#eda-authorization).
- The signed cluster application is published in the `streamskope` catalog at
  `https://github.com/asadarafat/streamskope.git`. EDA can fetch that catalog and
  its application image from `ghcr.io/asadarafat/streamskope-eda-app`.
- Cluster nodes can pull the pinned Redpanda `v24.3.5` broker image from
  `docker.redpanda.com/redpandadata/redpanda`. A local desktop plugin download does
  not supply these cluster images. For a restricted network, the administrator
  must arrange an approved registry/network delivery path; the desktop UI does
  not import a local OCI file into the cluster.
- The selected Producer/ClusterProducer exists and exports the data you need.
  A ready capture can remain empty until that source emits matching data.
- The cluster has capacity for the agent and one temporary broker. Current agent
  requests are 25m CPU/24 MiB memory, with limits 250m/96 MiB. Each broker requests
  100m/384 MiB, with limits 500m/768 MiB and ephemeral `emptyDir` storage. These are
  scheduling settings, not a production capacity or throughput guarantee.

**Install and continue** can create a Catalog named `streamskope` with the canonical
Git URL and TLS verification enabled, create a SigningKey named
`streamskope-capture` with the bundled public key, and submit an `AppInstaller` for
the exact app version. An administrator can approve this in the dialog. Review
the [public signing key](https://github.com/asadarafat/streamskope/blob/v0.1.0%2Bbuild.1/vendors/streamskope/apps/capture/signing/streamskope-eda.pub)
and the [agent's actual privileges](../guide/security.md#eda-authorization) first.

Existing conflicting catalog/key objects are rejected rather than overwritten.
Inspect and reconcile them through the site's EDA administration process; do not
disable signature or TLS verification to make installation succeed. Installation
is ready only when the app API is present and the proxied agent health endpoint
reports `ready` with the exact app version. A submitted installer workflow alone
is not proof of readiness.

## Call flow

Discovery is read-only. A temporary capture adds resources only after you choose
**Start capture**; if needed, **Install and continue** first installs the signed
cluster app using an authorized account.

```text
User: URL + credentials
  |
  v
Desktop plugin/host
  | Authenticate; read version
  v
EDA API: list exporters
  |
  v
User: source + temporary capture
  |
  v
Desktop: check/install cluster app
  | Recheck version
  | Reserve local port
  | Create session + lease
  v
Capture app
  | Create broker
  | Copy exporter
  | Preserve original exporter
  v
Desktop: open EDA WebSocket tunnel
  | Verify Kafka; save profile
  v
User: connect -> topic -> read
  |
Desktop renews lease while active
```

The local port is reserved before deployment. If it is occupied, choose a free
port; the plugin does not replace another process's listener. Kafka traffic uses
an authenticated WebSocket tunnel through EDA's API proxy. The local endpoint is
loopback-only, while the temporary exporter sends matching source data to the
cluster broker.

**Connect to existing Kafka** takes a shorter path: discovery opens a normal
profile using the source's existing broker addresses; you provide its Kafka trust
and credentials, then test and save. The Kafka client connects directly to those
brokers, without deploying a capture session or using the temporary tunnel.

```text
Stop/remove capture
  -> delete exact owned session
  -> delete copy, Service, broker
  -> close tunnel
  -> remove capture profile
  -> discard temporary messages

Update/remove plugin
  -> confirm stop
  -> remote cleanup
  -> retain profile
  -> update/remove plugin code

Exit / lost renewal
  -> lease expires
  -> agent cleans up
  -> resume authenticates again
     valid -> reopen tunnel
     absent session -> new capture

Cleanup not confirmed
  -> retain recovery information
  -> restore access
  -> retry cleanup
```

Repeated removal of a confirmed absent session succeeds. Cleanup never removes
the original Producer/ClusterProducer. See the [lifecycle table](#stop-update-and-resume)
for the distinction between disconnecting Kafka, stopping capture and uninstalling
the plugin.
