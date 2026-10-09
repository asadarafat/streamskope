---
container_downloads: true
---

# Deploy the browser workbench manually

Use this reference for offline installation, custom networking and Linux host
maintenance. For the normal online path, use
[Install the browser workbench](../start/containerlab.md). For day-to-day use,
follow [Use the browser workbench](browser-host.md).

## Released files

Availability follows the exact documented release. A development preview does
not establish that its browser changes have been published.

<!-- container-downloads -->

The EDA application image is a separate component and cannot run this workbench.

## Manual deployment

Use an already prepared Linux AMD64 or ARM64 Docker host with Containerlab.
Manual deployment does not require Node, npm or a source build. Keep the release
files together in a private directory owned by your non-root Linux account.

Keep manual installations separate from `/var/lib/streamskope/browser`, which is
reserved for installer-managed state. The installer does not adopt an existing
manual `clab-streamskope-app` container or its data.

### Online image

When the availability table lists registry delivery, download
`streamskope-VERSION.clab.yml`, `streamskope-VERSION-container.json` and
`SHA256SUMS` from that exact release. Replace `VERSION` below with its version.

The online topology pulls the public image from
`ghcr.io/asadarafat/streamskope`. Its reference includes the release version and
immutable index digest, `:VERSION@sha256:DIGEST`; Docker selects Linux AMD64 or
ARM64 for the host. No registry login is required. Use the matching topology
instead of a floating tag.

### Offline or restricted networks

On a machine with access, download the Docker save archive for the Linux host's
architecture, `streamskope-VERSION-offline.clab.yml`, the image manifest and
`SHA256SUMS`. Transfer the complete chosen set to the host.

| Linux host CPU | Archive architecture |
| -------------- | -------------------- |
| x64            | `amd64`              |
| ARM64          | `arm64`              |

The archives are gzip-compressed Docker `save` files. After verification below,
load the matching archive. For AMD64, use:

```sh
docker load --input StreamSkope-VERSION-container-linux-amd64.tar.gz
```

For ARM64, use `StreamSkope-VERSION-container-linux-arm64.tar.gz`.

Use the **offline** topology: its local `streamskope:VERSION` reference and pull
policy `Never` avoid GHCR access. The online topology and archive identify the
same qualified native image. Historical archive-only releases use their supplied
local-image topology; the availability table identifies the files actually published.

Prepare Docker, Containerlab and their dependencies separately on an offline
host. The online installer cannot provision missing packages without their
repositories. For a proxy-controlled host, have its administrator configure
Docker's image-pull transport and package/download access, or use offline files.
[Plugin downloads](#plugins-and-target-access) have separate limitations.

### Verify and start

Place the chosen files beside `SHA256SUMS` and verify them on Linux:

```sh
sha256sum --ignore-missing --check SHA256SUMS
```

Require an `OK` result for every chosen topology, image manifest and archive.
Stop on a mismatch and obtain a fresh copy from the trusted release. Checksums
verify the downloaded bytes; they are not a publisher signature.

Create the private data directory and select your non-root numeric owner:

```sh
mkdir -m 700 streamskope-data
export STREAMSKOPE_UID="$(id -u)"
export STREAMSKOPE_GID="$(id -g)"
```

Run deployment from this directory as an operator allowed to use Containerlab.
For online delivery:

```sh
clab deploy -t streamskope-VERSION.clab.yml
```

For offline delivery, substitute `streamskope-VERSION-offline.clab.yml`.
If Containerlab requires sudo, retain the chosen application owner:

```sh
sudo env "STREAMSKOPE_UID=$STREAMSKOPE_UID" "STREAMSKOPE_GID=$STREAMSKOPE_GID" \
  clab deploy -t streamskope-VERSION.clab.yml
```

Keep any deliberately configured bind, port and public-origin variables in that
approved sudo invocation too. Do not run the application container as root.
The topology mounts `./streamskope-data` at `/data`, outside Containerlab's
`clab-streamskope` output directory so redeployment preserves it.

With the defaults, open **http://127.0.0.1:8080**. Retrieve the private first-time
code from your trusted host terminal:

```sh
docker exec clab-streamskope-app cat /data/setup-code
```

Then follow [Create the vault](../start/containerlab.md#2-create-the-vault).
The application prints the code's file location rather than the code itself.
Do not put the code or vault passphrase in a URL, topology, shell command or log.

## VM and remote-host access

The default URL addresses Linux loopback. Your browser computer can run macOS,
Windows or Linux, but a VM must forward localhost ports for that URL to open
there. OrbStack and WSL normally provide this forwarding; check their
configuration if the printed URL does not open. Linux container checks alone
do not prove that a particular VM or remote-access path works.

An SSH tunnel retains the exact loopback origin without exposing the workbench
on the host's other interfaces. You need an SSH client on your browser computer,
a reachable SSH server on Linux and an account allowed to connect. The installer
does not provision SSH.

Run this on your browser computer if the printed URL is
`http://127.0.0.1:8081`:

```sh
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:8081:127.0.0.1:8081 user@linux-host
```

Replace `user@linux-host` with your SSH account and host. Replace **both** port
numbers with the printed or configured port if it differs. Keep the tunnel
running and open the exact URL printed by the installer.

Use the same port on both ends: the gateway checks the exact scheme, hostname
and port. If that port is already occupied on your browser computer, resolve
the conflict or prepare a manual deployment with a deliberate matching origin.
A different VM hostname is not automatically a valid browser origin.

### Custom browser address

For manual deployment through an explicit VM address, configure the bind address
and exact browser origin together before deployment. For a VM whose actual
reachable hostname is `clab.orb.local`, for example:

```sh
export STREAMSKOPE_HOST_BIND=0.0.0.0
export STREAMSKOPE_HOST_PORT=8080
export STREAMSKOPE_PUBLIC_ORIGIN=http://clab.orb.local:8080
```

| Variable                    | Meaning                                                                                               |
| --------------------------- | ----------------------------------------------------------------------------------------------------- |
| `STREAMSKOPE_HOST_BIND`     | Host interface that accepts connections. `0.0.0.0` binds all interfaces; it is not a browser address. |
| `STREAMSKOPE_HOST_PORT`     | Published port on the Linux host.                                                                     |
| `STREAMSKOPE_PUBLIC_ORIGIN` | Exact browser scheme, hostname and port authorized by the gateway.                                    |

An all-interface bind exposes the listener beyond Linux loopback; restrict access
through the host's normal network controls. Preserve these variables if using
sudo and recreate the node after changing them. A reachable TCP port with a
mismatched origin is rejected.

The installer retains its loopback origin and saved port; it does not accept
custom networking options. Linux runtime qualification does not qualify every
VM forwarding configuration or browser-computer network path.

For access outside a trusted host or lab, use an approved authenticated HTTPS
reverse proxy and configure the exact external HTTPS origin in a manual
deployment. The application has no TLS listener and this delivery does not claim
a qualified public-facing or multiuser service.

## Protect the vault and data

The browser host serves one owner's Kafka and NATS connections. It does not
provide separate users, SSO, tenant isolation or independent concurrent
workbenches. Same-session tabs share connections and settings. Your brokers and
optional target systems remain separate services.

A wrong passphrase preserves the data. Restarting the container starts with the
vault locked and never supplies its passphrase automatically.

Choose a unique vault passphrase with at least 12 characters and at most 1024
UTF-8 bytes. Keep it separately in a password manager. There is no passphrase
reset or recovery key; a directory backup cannot recover it.

Only one host may open a data directory. Another instance cannot unlock while
the first holds its lease; the kernel releases the lease after a process crash.
Do not remove lock files, replace metadata or create a fresh store to dismiss an
existing-data error.

If provider or plugin cleanup is unconfirmed, preserve its recovery records and
inspect the target resources before restarting. Health or unlock success does
not prove that remote resources were removed.

## Plugins and target access

Use [plugin versioning and compatibility](../plugins/versioning.md) to select a
package for the actual workbench and target versions. Publication availability
and live qualification are separate facts; follow each plugin's guide.

For a restricted network, transfer an approved signed `.skope-plugin` file from
your browser computer through **Preferences → Plugins → Install from file**.
The host verifies its publisher, exact bytes and compatibility. Enable
**Offline plugin downloads** to avoid remote catalog access. The production
browser host preserves that policy across locks and restarts; unreadable policy
blocks remote downloads. Its remote downloads use direct networking and do not
provide the desktop's custom proxy transport. Follow
[Install without GitHub](../plugins/offline.md) for cache behavior, upload limits
and target-side prerequisites.

The [EDA Connector](../plugins/eda.md) needs its approved capture application and
broker images available in the reachable EDA cluster. Its local tunnel and Kafka
client run inside the same application container; the topology does not publish
the capture's Kafka port. The [NSP Connector](../plugins/nsp.md) needs the NSP API
and discovered Kafka endpoints reachable from the container. Installing a
portable plugin does not package either target or make it reachable.

## Stop and resume

Finish active work, stop owned captures and complete pending plugin cleanup.
Then lock the vault before stopping the host. Containerlab's graceful option lets
the application disconnect providers and release its key before exit.

### Installer-managed host

Keep the saved release topology and deployment records under
`/var/lib/streamskope/browser`. Replace `VERSION` with its installed version:

```sh
sudo clab destroy --graceful --timeout 2m \
  -t /var/lib/streamskope/browser/streamskope-VERSION.clab.yml
```

Run the trusted published installer again from the **original Linux account** to
resume. It restores the saved UID, GID, image, port and origin, reuses the existing
vault, and prints its URL. You should see **Unlock**, followed by your saved
profiles and plugins after entering the original passphrase.

Do not run a plain `clab deploy` against an installer-managed topology: its
saved owner and port are supplied by the installer, rather than the topology's
defaults. Keep `installation.json` and the private release files. Removing these
records does not migrate the data into a new installation.

Rerunning even a newer installer retains the pinned release. To change it, use
the explicit [check and upgrade procedure](browser-host.md#upgrade-an-installer-managed-host).
The installer does not adopt a manual instance.

### Manually managed host

From the original release-file directory, retain the same UID/GID and any bind,
port and origin variables, then run:

```sh
clab destroy --graceful --timeout 2m -t streamskope-VERSION.clab.yml
clab deploy -t streamskope-VERSION.clab.yml
```

Use the offline filename for offline delivery and preserve the same variables in
sudo invocations. The second deployment reuses `streamskope-data`. It does not
automatically reconnect brokers or resume an EDA capture.

## Maintenance limits and recovery

Installer-managed maintenance retains the saved owner, account home, port,
loopback origin and data location. It requires the local Docker daemon, existing
Containerlab and the original Linux account. It does not install prerequisites,
change networking or migrate a manual deployment. Metadata and image downloads
may need GitHub/GHCR access; cached bytes must still match the exact release.

The reviewed legacy starting point is **0.10.3**. Newer targets must declare the
supported browser data contract. A similar version number is not evidence of
compatible data, and an arbitrary earlier image is not a rollback target. Only
the previous release recorded by a completed transition is selected by `rollback`.

Before the first upgrade, create and unlock the vault at least once, then lock
it. Maintenance requires its original lease file; it does not fabricate or
replace that file. Complete plugin-owned target work through the application.
The initial automated maintenance scope refuses saved plugin-managed profile
sources, including retained profile backups, and unresolved plugin recovery.
Retain those records and obtain a reviewed migration if the check still refuses;
deleting them is not proof of remote cleanup.

The preflight inspects storage envelopes and installed package integrity without
decrypting profiles or executing plugins. It cannot prove protected-content
authenticity or remote-resource cleanup. After a confirmed graceful stop, the
transaction holds the existing vault lease while checking consistent data,
backing it up and committing the replacement. A forced stop, nonzero exit,
competing data owner or substituted lease blocks progress.

Complete backups and their integrity records remain under
`/var/lib/streamskope/browser/backups/`. They preserve the data's contents,
permissions and numeric ownership. Retain them until the replacement has been
unlocked and verified and your normal backup policy permits retirement. They
consume local disk space and remain sensitive; do not upload them to an issue.

If maintenance is interrupted, keep `installation.json`, `maintenance.json`,
the backup generations and all application data. Run the trusted maintenance
installer again with `recover` from the original account. Ordinary no-argument
resume refuses an active transaction, so it cannot accidentally start the old
release against changed data.

Recovery advances only when the journal, actual container and installation
record agree. If the new record was already committed, it finishes bookkeeping.
If a candidate was unlocked or its data changed after an interruption, or its
identity was never durably recorded, recovery can require operator review. It
does not guess ownership, remove another container or restore a stale snapshot.
Preserve the failure's reason and transaction identity for a maintainer.

## Back up and restore

1. Record the installed release, source revision or image digest, plugin versions
   and target compatibility. Keep the matching release files and installer.
2. Complete target-side capture/cleanup work, lock the vault and stop the host
   gracefully as described above.
3. For an **installer-managed host**, make an access-restricted backup of the
   **complete `/var/lib/streamskope/browser` directory**, including
   `installation.json`, its topology, image manifest, checksums and
   `streamskope-data`. Preserve root ownership and private permissions on the
   deployment records, and the data directory's original numeric UID/GID.
   Record the original account's UID, GID, home and selected port.
4. For a **manual host**, back up the complete `streamskope-data` directory and
   matching release files. Preserve its numeric owner, permissions and configured
   bind, port and public origin. Include `vault.json`, both profile stores,
   settings, history, plugins, package cache and recovery identifiers.
5. Preserve the vault passphrase separately. A directory backup cannot recover it.
6. Before restoring, stop the host and retain a separate copy of its current
   records and data. Restore the complete chosen backup with its original
   ownership and permissions. Installer-managed recovery requires the same
   Linux account identity and home at the saved path; rerun the installer from
   that account. Manual recovery uses the matching compatible image and topology.
7. Unlock with the original passphrase, review saved endpoints and protection
   settings, and test a known connection before resuming work.

Moving installer state to another account, changing stored identities or
upgrading by replacing the pinned image is not a supported migration. Preserve
the backup and prepare a separately reviewed migration instead of editing the
metadata to bypass validation.

The vault encrypts credentials and protected trust material, **not the entire
data directory**. Endpoint names, preferences, saved views, observations, installed
plugin metadata and recovery identifiers remain sensitive filesystem data.
Protect all backups. Desktop profile files do not carry their OS encryption keys
into this host; automatic desktop-to-browser credential migration is not
implemented. Exports and target-side messages remain separate from the backup.
Never attach the data directory or its backup to a public issue.

### Recover data before an incompatible downgrade

Normal **rollback** switches to the recorded older image only if the current
complete data is compatible. It does not restore a historical backup. Newer
saved-view libraries or profile formats can therefore correctly block rollback.
Use a reviewed manual recovery with the host stopped if you need to restore older
data; there is no installer command that performs this restoration for you.

For an in-place downgrade, retain the current deployment records and installation
identity for the normal rollback transaction. Preserve a separate complete copy
of the changed data. Verify the chosen pre-upgrade backup's complete inventory,
hashes, permissions and numeric ownership before restoring it. Keep the existing
`streamskope-data` directory and its original `vault.lock` inode, holding that
file's exclusive advisory lock and the existing `installer.lock` throughout the
recovery. Do not unlink those lock files or replace them with backup copies.
Restore all other data entries from the complete verified backup, then use normal
rollback once compatibility checks pass. This controlled data restoration differs
from the full deployment disaster recovery above. Keep the current host if you
cannot establish backup integrity and exclusive ownership; do not erase recovery
files to bypass a refusal.

## Troubleshooting

| Symptom                                                      | Next action                                                                                                 |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| Installer reports an existing runtime or repository conflict | Preserve the existing workloads; prepare Docker manually without replacing them, or use another Linux host. |
| Installer cannot download files or pull the image            | Check HTTPS/proxy access to GitHub and GHCR, or use a separately prepared offline manual deployment.        |
| Installer reports another account or unrelated container     | Use the original account and deployment procedure; do not remove records or adopt another host's data.      |
| No container or listener                                     | Check deployment and `docker logs clab-streamskope-app`; review image availability and ownership.           |
| Linux opens the workbench; your browser computer cannot      | Check VM forwarding or use the [matching-port SSH tunnel](browser-host.md#vm-and-remote-host-access).       |
| HTTP 403 at a reachable address                              | Match the configured scheme, hostname and port; changing the browser URL alone does not change the origin.  |
| Vault is in use                                              | Stop the other host using the data; keep its lease file.                                                    |
| Vault cannot unlock                                          | Check the passphrase, preserve the store and restore a complete known-good backup if needed.                |
| Kafka bootstrap works but topic operations fail              | Check every advertised broker address from the application container.                                       |
| Plugin file is rejected                                      | Review publisher verification and the host/target intervals in the plugin guide.                            |

Open **Raw logs** for connection failures and follow
[Troubleshoot a problem](troubleshooting.md). Share only redacted errors, the
release/source identity and the action that failed.

Operational startup and vault failures include a reason code, the failed stage and a
correlation ID with a recovery action. Keep that ID when contacting a maintainer;
the matching host diagnostic uses the same ID. Share the diagnostic record,
not an unreviewed container-log dump. A cleanup diagnostic means cleanup is
unconfirmed: preserve recovery records and check owned target resources before
restarting. A reported failure does not release ownership or mark cleanup as done.
