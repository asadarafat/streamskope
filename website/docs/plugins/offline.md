---
plugin_scope: all
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

## Prepare the file on a connected computer

1. Open the chosen plugin release in the
   [official GitHub releases](https://github.com/asadarafat/streamskope/releases).
2. Download its **portable** asset, such as
   `streamskope-nsp-portable-v0.1.0.skope-plugin`. This filename is an example,
   not a claim that this version is published. Check the release's desktop and
   target requirements.
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
you intend to use and review it before installation. Its download date and trust
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
