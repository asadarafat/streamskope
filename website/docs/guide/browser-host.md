# Use the browser workbench

Install → Open → Create vault → Connect → Inspect → Lock → Reopen

Use this guide after [installing the browser workbench](../start/containerlab.md).
One vault owner uses the workbench; tabs in the same session share connections
and settings.

## Open and unlock

Open the exact URL printed by the installer. If you no longer have it, rerun the
trusted published installer from the original Linux account to print it again.
Enter the original **Vault passphrase** and select **Unlock**. On first use,
follow [Create the vault](../start/containerlab.md#2-create-the-vault) instead.

## Connect and inspect

Open **Connection Profiles**, select a saved profile's **Connect** action and
wait for connected status. To add one, follow [Connect a broker](../start/containerlab.md#3-connect-a-broker).

| Task                                   | Guide                                                                 |
| -------------------------------------- | --------------------------------------------------------------------- |
| Configure and test Kafka access        | [Connect your Kafka](connections.md)                                  |
| Find, filter and inspect Kafka records | [Find and inspect a message](messages.md)                             |
| Subscribe to new NATS traffic          | [NATS live subscriptions](core-nats.md)                               |
| Diagnose a failed connection or read   | Open **Raw logs**, then [Troubleshoot a problem](troubleshooting.md). |

The container makes the connection. `localhost` and `127.0.0.1` in a profile
refer to the container; every Kafka advertised broker address must be reachable
from it. Connecting another profile stops the current stream and disconnects
its host first. If cleanup fails, follow the original workspace's recovery message.

## Lock and reopen

Finish active work and any owned capture cleanup, then select
**Lock vault and disconnect**. Locking closes provider work, ends the session and
releases the unlocked key. Closing a browser tab does not lock the vault.

Return to the same URL and unlock with the original passphrase. Saved profiles
remain available; select **Connect** to resume work. Reopening does not
automatically reconnect brokers or resume a capture.

The session expires two hours after unlocking; activity does not extend it.
Restarting the container starts with the vault locked. If cleanup is unconfirmed,
preserve recovery records and [inspect target resources before restarting](browser-deployment.md#protect-the-vault-and-data).

## Plugins and target access

Manage optional plugins through **Preferences → Plugins**. Check
[plugin compatibility](../plugins/versioning.md) and follow the
[EDA](../plugins/eda.md) or [NSP](../plugins/nsp.md) guide for target prerequisites.
For signed file installation on a restricted network, use
[offline plugins and target access](browser-deployment.md#plugins-and-target-access).

## VM and remote-host access

Open the exact printed URL first. [OrbStack](https://docs.orbstack.dev/machines/ssh#port-forwarding)
and [WSL](https://learn.microsoft.com/en-us/windows/wsl/networking#accessing-linux-networking-apps-from-windows-localhost)
normally forward localhost ports to your macOS or Windows browser, depending on
their configuration. For a remote host or a VM without forwarding, follow
[the matching-port SSH tunnel procedure](browser-deployment.md#vm-and-remote-host-access).

## Stop and resume

Finish cleanup and lock the vault, then follow
[the graceful stop and resume procedure](browser-deployment.md#stop-and-resume).
Resume an installer-managed host by rerunning the trusted installer from its
original Linux account. It preserves the owner, port, vault and pinned release.
To change the release, use the explicit [upgrade procedure](#upgrade-an-installer-managed-host).

With Docker running, the container's `unless-stopped` policy restarts it after a
Docker restart unless it was explicitly stopped. The vault starts locked;
unlock and connect explicitly to resume work.

## Upgrade an installer-managed host

Download `install-browser-workbench.sh` from the release you intend to install
and save it on the Linux host. Use the original Linux account. A normal rerun
still resumes the saved release; it never silently upgrades the workbench.

Finish active work and owned plugin cleanup, then check the target release:

```sh
sudo bash install-browser-workbench.sh check
```

The check leaves the running workbench in place. It verifies the release, local
deployment ownership, supported data formats and backup space. If it refuses,
follow the reported action and keep the existing data and deployment records.

When the check succeeds, run:

```sh
sudo bash install-browser-workbench.sh upgrade
```

The installer gracefully stops the owned host, makes a private backup, checks
the stopped data, and starts the target release with the same owner, port and
browser URL. It records the new version only after the host starts locked and
passes readiness checks. Open the URL, unlock with your original passphrase,
and test a saved connection before resuming work. Readiness alone does not
verify your passphrase or broker access.

| Need                              | Command                                           | Result                                                                    |
| --------------------------------- | ------------------------------------------------- | ------------------------------------------------------------------------- |
| Review an upgrade                 | `sudo bash install-browser-workbench.sh check`    | Checks the target without stopping the host.                              |
| Install that target               | `sudo bash install-browser-workbench.sh upgrade`  | Backs up current data and replaces the verified owned host.               |
| Return to the previous release    | `sudo bash install-browser-workbench.sh rollback` | Uses the previous recorded image with current compatible data.            |
| Continue an interrupted operation | `sudo bash install-browser-workbench.sh recover`  | Reconciles the recorded transaction or reports why recovery needs review. |

Rollback does not restore old messages, rewind target systems or replace current
data with an older backup. If current data is incompatible, it stops instead.
Backups remain private on the Linux host; they are not an off-host backup.

This procedure requires an initialized vault and an installer-managed deployment.
Saved plugin-managed profiles, unresolved recovery or unsafe/unknown data formats
can block maintenance. Do not delete ownership metadata or lock files to bypass
a refusal. Follow [maintenance limits and recovery](browser-deployment.md#maintenance-limits-and-recovery)
for the supported starting release and retained evidence.

## Back up and restore

Follow [the complete backup and restore procedure](browser-deployment.md#back-up-and-restore).
For an installer-managed host, preserve all of `/var/lib/streamskope/browser`,
including deployment records and `streamskope-data`, with its ownership and
private permissions.

The vault encrypts credentials and protected trust material, **not the entire
data directory**. Saved metadata and backups remain sensitive. Preserve the
passphrase separately; there is no reset or recovery key.

## Troubleshooting

Use [Troubleshoot a problem](troubleshooting.md) for connection failures and
[deployment troubleshooting](browser-deployment.md#troubleshooting) for installer,
host or vault errors. Preserve existing data and recovery records.

<span id="manual-deployment"></span>
<span id="released-files"></span>

For release downloads, offline installation or custom networking, use
[Deploy the browser workbench manually](browser-deployment.md).
