---
browser_installer: true
---

# Install the browser workbench

Run StreamSkope on a Linux AMD64 or ARM64 host and open it in a browser on macOS,
Windows or Linux. It connects to your own Kafka and NATS servers; it does not
start a broker. Saved credentials use an encrypted vault.

Run the installer **inside Linux**, using an account with `sudo` and `curl`.
The Linux host can be your computer or a VM. The installer reuses existing Docker
and Containerlab and can install missing prerequisites on Ubuntu 22.04/24.04 and
Debian 12/13. It preserves conflicting runtimes for you to review.
For offline installation, another distribution or custom networking, use
[manual deployment](../guide/browser-deployment.md).

## 1. Install

Run this command in the Linux terminal to install the latest release.
Keep the terminal open for the first-time setup code.

<!-- browser-installer -->

Open the **exact browser URL printed by the installer**, including its port.
It normally uses port 8080 and chooses the next free port when necessary.
You should see **Create vault** or **Unlock**.

Can't open the URL from your browser computer?
Follow [VM and remote-host access](../guide/browser-host.md#vm-and-remote-host-access).

## 2. Create the vault

On the first installation, enter the **Setup code** shown in your Linux terminal,
choose and confirm a **Vault passphrase**, then select **Create vault**.
The installer shows the code only in your terminal, not captured output or the
URL. If it was not shown, rerun interactively from the same Linux account.
The code is removed after vault creation.

Use a unique passphrase of at least 12 characters and at most 1024 UTF-8 bytes.
Keep it in your password manager. There is no password reset or recovery key;
losing the passphrase makes saved credentials unrecoverable.

An existing vault shows **Unlock**. Enter its original passphrase.
Rerunning the installer preserves the vault, profiles and installed version;
a newer installer does not upgrade an existing installation automatically.
Use the separate [upgrade procedure](../guide/browser-host.md#upgrade-an-installer-managed-host)
when you want to change releases.

**You should see:** **Connection Profiles** after the vault opens.

## 3. Connect a broker

Have the server addresses, authentication settings and any TLS trust material
ready. Open **Connection Profiles → Add connection**.

| System | Save and connect                                                                                                           |
| ------ | -------------------------------------------------------------------------------------------------------------------------- |
| Kafka  | Choose **Kafka broker**, enter the settings, then **Test connection → Save profile**. Select **Connect** on the saved row. |
| NATS   | Choose **NATS server**, enter the settings, then **Save profile**. Select **Connect** on the saved row.                    |

Wait for connected status. Use [Connect your Kafka](../guide/connections.md)
or [NATS live subscriptions](../guide/core-nats.md) for field details.
Connections originate in the container: `localhost` and `127.0.0.1` in a profile
refer to that container. Kafka's advertised brokers must also be reachable from it.

For **Kafka**, open **Topics**, select a known topic, choose **Newest N** with a
small limit such as `10`, then **Load messages**. Select a row and open
**Message details → Value**. Follow [Find and inspect a message](../guide/messages.md)
for filters and metadata.

For **NATS**, open **Live Subscription**, enter a permitted **Subject filter**,
then **Start subscription**. Have your producer send new traffic and select a
row in **NATS records** to open **Record inspector**. This view receives future
traffic, not stored history.

**You should see:** a record you can inspect. If a connection or read fails,
open **Raw logs** and follow [Troubleshoot a problem](../guide/troubleshooting.md).

## Finish and return

When finished, complete any owned capture cleanup, then select
**Lock vault and disconnect**. Closing a browser tab does not lock the vault.

Return to the same URL and unlock with the original passphrase, then connect a
saved profile. Use [the browser workbench guide](../guide/browser-host.md)
for everyday operations, session expiry and host maintenance.

<span id="back-up-and-restore"></span>

Keep a [complete backup](../guide/browser-deployment.md#back-up-and-restore)
of the deployment records and data under `/var/lib/streamskope/browser`.
