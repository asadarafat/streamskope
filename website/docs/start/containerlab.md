---
browser_installer: true
---

# Install the browser workbench

Run StreamSkope on a Linux host and use it from your browser. It connects to your
own Kafka and NATS servers; it does not start a broker. Saved credentials use an
encrypted vault that you unlock in the browser.

Use a Linux AMD64 or ARM64 host, or a Linux VM on your Mac, with `curl` available
and an account that can run `sudo`. Run the installer from that account. It reuses existing Docker
and Containerlab and can install missing prerequisites on Ubuntu 22.04/24.04 and
Debian 12/13. An existing conflicting runtime is preserved for you to review.

For a restricted network, another distribution, or custom browser address, use
[manual deployment and host maintenance](../guide/browser-host.md).

## 1. Install

The published release determines the installer command below. Run it in a Linux
terminal, then keep that terminal open for the first-time setup code.

<!-- browser-installer -->

**You should see:** one browser URL and the next vault action. Open the exact
printed URL. It uses Linux loopback and normally port 8080, choosing the next free
port when necessary. Use the printed port.

If you use a Linux VM, the URL needs loopback forwarding to your Mac. If it does
not open, follow [VM and remote-host access](../guide/browser-host.md#vm-and-remote-host-access)
instead of changing the URL alone.

## 2. Create the vault

On the first installation, enter the **Setup code** shown in your terminal,
choose and confirm a **Vault passphrase**, then select **Create vault**. The
installer reveals the code only in an owner terminal, so it is absent from
captured output and the browser URL. If it was not shown, rerun the same installer
interactively from the same Linux account.

Choose a unique passphrase with at least 12 characters and at most 1024 UTF-8 bytes,
and keep it in your password manager. There is no password reset or recovery key. Losing the passphrase makes
saved credentials unrecoverable. The setup code is removed after vault creation.

An existing installation shows **Unlock**. Enter its original passphrase; rerunning
the installer preserves its saved profiles, vault and pinned version. A newer
installer does not upgrade that installation automatically.

**You should see:** Connection Profiles after the vault opens.

## 3. Connect a broker

Open **Connection Profiles → Add connection**, choose **Kafka broker** or
**NATS server**, and enter the reachable server and authentication parameters.
Test and save the profile, then connect.

Connections originate in the application container. In a profile, `localhost`
and `127.0.0.1` refer to that container. Kafka's advertised broker addresses must
also be reachable from it. Use [Connect your Kafka](../guide/connections.md) or
[NATS live subscriptions](../guide/core-nats.md) to inspect your first record.

**You should see:** Kafka topics or the NATS Live Subscription view. NATS receives
future traffic from the server you supplied.

When finished, select **Lock vault and disconnect**. Closing a browser tab does
not lock the vault. See [locking and reopening](../guide/browser-host.md#lock-and-reopen)
for session expiry and restart behavior.

## Keep your installation

The installer keeps private deployment records under
`/var/lib/streamskope/browser` and application data in its `streamskope-data`
child. Preserve both the records and data when backing up. Credentials and trust
material are encrypted; other saved metadata is sensitive filesystem data.

<span id="back-up-and-restore"></span>

Use [browser host backup and restore](../guide/browser-host.md#back-up-and-restore)
for the complete procedure, or [host maintenance](../guide/browser-host.md#stop-and-resume)
to stop and resume safely.
