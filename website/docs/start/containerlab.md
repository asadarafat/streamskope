---
container_downloads: true
---

# Run StreamSkope with Containerlab

Run the workbench in a browser, with its Kafka and NATS application host in one
Linux container. Connect to your own brokers; this topology does not start a
Kafka or NATS server. The browser edition avoids installing an unsigned desktop
app or making a macOS security exception.

The availability below follows the documented release. A source checkout may
contain browser-host changes that have not been published.

<!-- container-downloads -->

The EDA application image is a separate component and cannot run this workbench.

## Use a released image

When the table lists browser assets, download the image archive for your Linux
Docker host's architecture, the matching `.clab.yml` topology and `SHA256SUMS`
from that exact release. Verify the archive and topology before use. Release
archives use Docker's `save` format compressed with gzip. On Linux, after placing
the chosen archive and topology beside the downloaded checksum file, run
`sha256sum --ignore-missing --check SHA256SUMS` and require an `OK` result for
each chosen file. Load the selected
archive with `docker load --input <image-archive.tar.gz>`.

Create a private `streamskope-data` directory beside the downloaded topology,
export your numeric user and group as shown below, then run
`clab deploy -t <downloaded-topology.clab.yml>`. The topology selects the exact
loaded image version and never silently pulls a different one. This path does
not need Node, npm, a source build or a registry account.

If browser assets are unavailable for the documented release, use the following
source procedure. Publication of a desktop installer does not imply a browser
image is available.

## Before you start

Use a Linux Docker host with Containerlab, a source checkout, **Node 24.21+ in the 24.x line**, and
npm for source builds. Released image archives need only the Linux Docker host
and Containerlab. The image's vault uses Linux advisory file locking. Choose the image for
the Docker host's architecture; a local build qualifies only that architecture.
Building needs access to the base image and package registry, or their approved
local mirrors and caches.

For macOS, run Docker and Containerlab inside your Linux VM, such as OrbStack.
The default address is the Linux host's loopback interface. It is usable from
your Mac only when the VM forwards that address and port. Linux browser tests do
not establish that Safari or another browser on your Mac can reach it.

This is a single-owner host. It does not provide separate user accounts, SSO,
tenant isolation, or concurrent independent workbenches. Same-session tabs share
connections and settings.

## 1. Build source and deploy

Run these commands in the repository root. The two npm commands use the existing
project scripts; Containerlab manages the running instance.

```sh
ELECTRON_SKIP_BINARY_DOWNLOAD=1 npm ci --ignore-scripts
npm run package -- container
mkdir -m 700 streamskope-data
export STREAMSKOPE_UID="$(id -u)"
export STREAMSKOPE_GID="$(id -g)"
clab deploy -t streamskope.clab.yml
```

Run deployment as an operator permitted to use Containerlab. If your host
requires an approved sudo invocation, preserve the topology's configuration
variables rather than switching to a different numeric owner. Do not configure
the application container to run as root.

The source image is `streamskope:0.0.0-dev`. The lab is `streamskope`, its node is
`app`, and its Docker container is `clab-streamskope-app`. The topology mounts
`./streamskope-data` at `/data`. This directory is outside Containerlab's
generated `clab-streamskope` directory so redeployment does not discard it.

Open **http://127.0.0.1:8080** from a browser that can reach the Linux host's
loopback port. On a first deployment, you should see **Create vault**.

### A different browser address

The gateway accepts one exact configured browser origin, including its scheme,
hostname and port. An alternate hostname is not an alias for the default origin.

If Mac loopback forwarding is available, use the default URL. If you deliberately
publish the service on a trusted lab interface for access through an OrbStack VM
hostname, configure both the host bind address and `STREAMSKOPE_PUBLIC_ORIGIN` to
match the address you actually open. An all-interface bind exposes the listening
port beyond Linux loopback; apply the host's normal network access controls.

For example, the explicit origin is `http://clab.orb.local:8080` for a VM whose
actual reachable hostname is `clab.orb.local`. Do not substitute `0.0.0.0` as the
browser origin. Recreate the node after changing the topology environment.
An origin mismatch is rejected even if the TCP connection succeeds. For that
explicit VM address, before deployment:

```sh
export STREAMSKOPE_HOST_BIND=0.0.0.0
export STREAMSKOPE_PUBLIC_ORIGIN=http://clab.orb.local:8080
```

The management-network setting avoids broad forwarding rules; it does not disable
outbound connections or override the host firewall. Restrict access to any
explicitly published lab interface.

For access outside a trusted local host or lab, use an authenticated deployment
behind an approved HTTPS reverse proxy and configure the exact external HTTPS
origin. The application does not provide a TLS listener itself. This source
delivery does not claim a qualified public-facing or multiuser deployment.

## 2. Create the vault

Retrieve the first-time setup code from the instance's private file:

```sh
docker exec clab-streamskope-app cat /data/setup-code
```

The file is private to the application user, with mode `0600`. The application
prints its location, not its value. Use the code in the browser's **Setup code**
field, enter and confirm a **Vault passphrase**, and select **Create vault**.
The code is removed after successful creation.

Use a unique passphrase between 12 and 1024 UTF-8 bytes and preserve it in your
approved password manager. The passphrase is not saved in the image, topology,
data directory, or browser storage. There is no password reset or recovery key:
losing it makes the stored credentials unrecoverable.

The vault protects stored Kafka and NATS credentials and protected trust material
using passphrase-derived authenticated encryption. **It does not encrypt the
entire data directory.** Profile names, endpoints, configuration, queries,
operational preferences, observation history, and plugin metadata remain
sensitive filesystem data. Restrict and protect the directory and its backups.
See [Data, exports and limits](../guide/data-handling.md) for the data inventory.

## 3. Connect a broker

Open **Connection Profiles → Add connection**, choose **Kafka broker** or
**NATS server**, and supply the reachable server and authentication parameters.
Test and save the connection before connecting.

Network connections originate in the **application container**, not the browser.
In a profile, `localhost` and `127.0.0.1` refer to that container. A broker on
your Linux host or another machine needs an address reachable from the container.
Kafka's advertised broker addresses must also resolve and be reachable there;
a reachable bootstrap address alone is insufficient.

Use [Connect your Kafka](../guide/connections.md) or
[NATS live subscriptions](../guide/core-nats.md) for the provider's workflow.
NATS subscriptions inspect your supplied remote server and receive future
traffic; the workbench does not embed a NATS server or inspect its own internals.

## 4. Lock and reopen

Select **Lock vault and disconnect** in the workbench header when finished. Locking closes the
host's active provider work and invalidates its browser session before releasing
the unlocked key. Unlock again with the same passphrase to use saved profiles.

The session expires two hours after unlocking; activity does not extend it.
Closing the browser tab is not an explicit vault lock. Restarting or redeploying
the container starts with the vault locked and never supplies its passphrase
automatically. A wrong passphrase preserves the existing data.

Only one host may open the same data directory. A second instance fails to unlock
while the first holds its lease. The kernel releases that lease if the process
crashes. Do not remove a lock file, replace metadata, or use a fresh empty store
to dismiss an error on an existing instance.

If provider or plugin cleanup cannot be confirmed, preserve the data and inspect
the target resources before restarting. An unlock or health endpoint alone does
not establish that remote capture resources were removed.

## Plugins and offline use

Use **Preferences → Plugins → Install from file** to transfer an approved signed
portable `.skope-plugin` file from your browser computer to this host. The host
verifies the publisher signature, exact package bytes and host compatibility.
Target compatibility declarations are reviewed before installation; the running
target version is checked through its API when connecting or discovering. Uploads are bounded to 48 MiB;
an unused selection expires after 60 seconds. Cancelling an upload cannot install
its late result. Local file installation does not contact GitHub.

The source host identifies itself as `0.0.0-dev`. It intentionally rejects
published packages whose declared host interval excludes that development
identity. A released desktop/plugin package cannot be made compatible by
renaming its file. Use an actually compatible released host, or the project's
separate source plugin development flow. Qualification builds use a disposable
version-stamped checkout; they do not claim that development source is a released
host.

Browser-host plugin downloads currently use direct networking. The desktop's
custom proxy transport is not provided by this host. For a restricted network,
transfer a complete signed portable package and enable **Offline plugin downloads**.
The production browser host preserves that policy across vault locks and restarts.
Unreadable saved policy blocks remote downloads until explicitly repaired.
See [Install without GitHub](../plugins/offline.md) for publisher verification,
cache behavior, and target-side prerequisites.

The [EDA Connector](../plugins/eda.md) requires the approved capture application
and broker images in its reachable EDA cluster. Its local Kafka tunnel and the
StreamSkope Kafka client run in the same container: the topology does not publish
the capture's Kafka port. The [NSP Connector](../plugins/nsp.md) needs both the NSP
API and its discovered Kafka endpoints reachable from the container. Installing
a portable plugin does not package either target system or make it reachable.

## Stop and redeploy

Finish active work and explicitly stop owned captures or pending plugin cleanup
before stopping the lab:

```sh
clab destroy --graceful --timeout 2m -t streamskope.clab.yml
clab deploy -t streamskope.clab.yml
```

Containerlab normally force-removes containers; the explicit graceful option lets
the application disconnect providers and release its vault key before exiting.
For a downloaded release topology, use its filename in both commands.

The second deployment reuses `streamskope-data`. You should see **Unlock**, then
your saved profiles and installed plugins after entering the passphrase. It does
not automatically reconnect to brokers or resume an EDA capture.

## Back up and restore

1. Record the source revision or published image digest, host version, plugin
   versions, and target compatibility. Keep the matching topology and image.
2. Finish target-side capture and cleanup work, lock the vault, and destroy the lab with `--graceful --timeout 2m`.
3. Copy the entire `streamskope-data` directory into an access-restricted backup.
   Preserve file permissions and numeric ownership. Include `vault.json`, both
   profile stores, plugins and their recovery records, settings, and history.
4. Preserve the vault passphrase separately. The directory alone cannot recover it.
5. To restore, stop the host and keep a separate copy of its current directory.
   Restore the complete chosen backup into a new private directory at the topology's
   data path, preserve its owner, and deploy the matching compatible image.
6. Unlock with the original passphrase. Review saved endpoints and protection
   settings, then test a known connection before resuming normal work.

Copying Electron desktop profile files into this directory does not migrate their
operating-system encryption keys. Desktop-to-container credential migration is
not implemented; re-enter the credentials in reviewed container profiles.
Exports, clipboard copies, and target-side messages remain separate from this
backup. Never attach this directory or its backup to a public issue.

## If it does not open

| Symptom                                  | Next action                                                                                                          |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| No container or listener                 | Check Containerlab deployment and `docker logs clab-streamskope-app`; correct image availability and data ownership. |
| Linux opens it; Mac cannot               | Verify the VM's loopback forwarding or explicit lab-interface publication and the Mac-reachable hostname.            |
| HTTP 403 at a reachable address          | Use the exact configured public origin; scheme, hostname and port must match.                                        |
| Vault is in use                          | Stop the other host using that data directory; do not remove its lease file.                                         |
| Vault cannot be unlocked                 | Check the passphrase and preserve the store; restore a complete known-good backup if it is corrupt.                  |
| Kafka connects but topic operations fail | Verify every advertised broker address from the application container.                                               |
| Plugin file is rejected                  | Check its signature and host/target interval; `0.0.0-dev` is not a published release.                                |

For connection failures, open **Raw logs** and follow
[Troubleshoot a problem](../guide/troubleshooting.md). Share only redacted errors,
the image/source identity, and the action that failed.
