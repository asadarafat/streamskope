---
plugin_scope: all
plugin_portable_downloads: true
---

# Install a plugin without GitHub access

Use the documentation for your installed desktop release. Development instructions
describe source behavior; they do not add file installation to an older installer.
Check the site and plugin availability notices before following this procedure.

You can install or update an official connection plugin from a publisher-signed
`.skope-plugin` file. The app verifies the publisher using public keys shipped with
the desktop, then checks the plugin's desktop requirements. Installing a plugin
does not require internet access when its complete package is available locally.
Connecting afterward still requires access to your Kafka brokers and the relevant
EDA or NSP API.

## Configure plugin downloads

Open **Preferences → Plugins → Plugin download settings**. The desktop uses the
operating system's proxy configuration by default. If your network requires an
explicit proxy, select the custom proxy option and supply an HTTP or HTTPS proxy
address. Enter proxy credentials in the separate fields; do not put credentials
in the address. Save settings, then test the applied connection.

Proxy settings apply only to plugin catalog and package downloads. They do not
change Kafka, Core NATS, EDA or NSP connections. An authenticated HTTP CONNECT
proxy is supported; SOCKS, NTLM and every organization's automatic proxy policy
are not individually qualified. Corporate certificate validation remains enabled.
Install the organization's approved CA in the operating system when required;
there is no plugin-download switch to bypass TLS verification.

The connection test checks the official catalog and, when available, a release
asset route. A catalog-only result describes its remaining limitation; it does
not establish that a complete plugin package can be downloaded. Successful tests
apply to the saved settings revision, not an unsaved form or a later proxy change.

**Offline plugin downloads** blocks catalog refresh, remote package download and
connection tests before they access the network. You can still install signed
files, use verified cached packages, retry or remove installed plugins, and
connect to locally reachable systems. Turn it off when you intend to download
again. This option does not make the whole workbench offline.

Proxy credentials never return to the renderer. When OS-backed protection is
available, the host saves them encrypted. Otherwise they remain in memory for
the current session and must be entered again after restart. Unreadable saved
settings block remote acquisition until explicitly reset; they do not block
local plugin management. Browser development hosts do not provide native proxy
configuration.

## Cancel a stalled download

Catalog refresh, package download and connection tests show their current phase.
Downloads show bytes received and, when supplied by the server, total size. Use
the cancellation action while an operation is running. Cancelling acquisition
cannot install its late result. Changing saved network settings also cancels
remote operations admitted under the previous settings.

Installed-plugin management and file/cache installation remain usable during a
remote download. A completed package review pins verified local bytes; changing
the network afterward does not replace or invalidate that selected package.
Installation and confirmed owned-work cleanup use their existing lifecycle rules.

| Download failure                    | Next action                                                                                           |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Proxy authentication                | Check the separately supplied proxy credentials and supported authentication method.                  |
| Proxy connection or tunnel          | Check proxy host, port and permission to reach GitHub release assets.                                 |
| Certificate validation              | Install the organization's trusted CA through its OS administration process.                          |
| GitHub rate limit or timeout        | Retry later or transfer an approved signed portable package.                                          |
| Offline mode or unreadable settings | Disable offline mode or explicitly reset the download settings; local installation remains available. |

## Prepare the file on a connected computer

The table below identifies signed portable files verified for the desktop release
shown in this site's version notice. Desktop installer releases and connection
plugin releases are separate. Open the linked **plugin release**, rather than
looking for plugin files among the desktop installers.

<!-- portable-plugin-downloads -->

If a row has no portable download, the catalog-selected compatible package has
no verified portable file in this snapshot. Use a complete previously downloaded package
from the app's cache, restore catalog access, or wait for a compatible plugin
release. Renaming an older catalog package does not make it a signed portable file.
Publication or network verification failures are not treated as proof of absence.

1. Open the verified plugin release linked in the table and compare its desktop
   and target requirements with your environment.
2. Download that row's **signed portable file**. The portable and primary catalog
   assets contain the same plugin code, resources and version. Choose the portable
   asset for **Install from file**.
3. Transfer that exact file to the desktop computer using your organization's
   approved method. Keep the release identity with it for troubleshooting.

The primary `streamskope-nsp-vVERSION.skope-plugin` catalog asset uses the older
delivery format and cannot be imported as a portable file. Older releases are not
retroactively signed. A checksum file supplied alongside an arbitrary package
cannot authorize its publisher. Never disable signature checks to work around an
unknown publisher or an altered file.

## Review and install

1. Open **Preferences → Plugins → Install from file** in the desktop app.
2. Select the portable file. The host reads a bounded local copy and verifies it;
   it does not send your file or local path to GitHub.
3. Review its plugin name, version, publisher, source, digest, and desktop/target
   requirements. An incompatible package is blocked. An identical healthy
   installation is shown as already installed and keeps active work running.
4. Install or update the reviewed package. If the plugin owns active work, read
   and confirm the cleanup prompt. A failed cleanup keeps the working plugin
   available for recovery. Successful activation takes effect without restarting.

The review pins the exact verified package. It does not reread your original file
or silently select a different catalog version when you confirm. Cancelling the
review releases that candidate; an expired review must be opened again.
File selection is a native desktop capability. The browser development host can
exercise packaged plugins but does not provide the OS file-selection dialog.

```text
Connected computer             Desktop with no GitHub access
  |                              |
  | download signed portable     | Install from file
  +----- approved transfer ----->| select local file
                                 | verify publisher + package
                                 | review exact version + requirements
                                 | confirm owned-work cleanup if needed
                                 | activate reviewed bytes
                                 v
                          Add connection -> local EDA / NSP API
                                 |
                                 v
                          Test + save Kafka profile -> Connect
```

## Use an already downloaded package

The Plugins page lists complete verified packages retained in its bounded local
cache separately from the last-known catalog. Select the exact cached version
you intend to use and review it before installation. Its cache date and trust
source describe the retained copy, not the latest release available online.

A cached catalog is only metadata. Seeing a version there does not mean its bytes
have been downloaded. If the selected package is unavailable, use a portable file
or restore network access; the host never silently substitutes an older cached
version for a failed new download. Cached archives are checked again before use.
The cache retains a small number of packages and can evict unused copies. Keep a
separate copy of approved portable files when you need reliable future access.

Remove and **Retry activation** continue to use local installed state. Retry
verifies retained bytes and reloads an inactive or failed installation; it does
not look up a replacement on GitHub. The shared
[lifecycle rules](index.md#install-and-manage-plugins) still apply to every source.

## Target-side prerequisites remain separate

NSP's helper workflow is already bundled in its plugin and verified with the
package. The plugin uses the locally reachable NSP API to ensure and execute that
helper; no Kubernetes credentials or GitHub access are needed for that step.

EDA also needs its cluster application and broker images available to the cluster.
A desktop portable plugin contains neither the EDA application OCI image nor its
container dependencies. In an air-gapped cluster, have the administrator provision
the approved application and images through the cluster's internal delivery
process before starting capture. Follow the
[EDA administrator prerequisites](eda.md#administrator-prerequisites).

For trust and cleanup failures, use [security and permissions](../guide/security.md),
the individual [EDA](eda.md) / [NSP](nsp.md) guides, and
[plugin versioning](versioning.md). Installing the desktop plugin does not widen
its qualified target interval.
