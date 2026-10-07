---
desktop_downloads: true
---

# Install StreamSkope

Choose a desktop installer below, or [use the development sandbox](development.md).

Before downloading, check the [supported authentication and data formats](compatibility.md)
and [desktop prerequisites](#desktop-prerequisites).

GitHub Releases contains desktop installers. The separately versioned EDA Connector
catalog and container image are not desktop releases; their tags do not imply a
new desktop download is available.

## Download

Choose the file for your operating system and CPU:

<!-- desktop-downloads -->

These releases are unsigned: macOS builds have no Developer ID signature or
notarization, Windows installers have no Authenticode signature, and Linux
downloads have no publisher signature. OS security warnings are expected.

Compare your file's digest to its entry in `SHA256SUMS` before installing:

```sh
# macOS
shasum -a 256 "StreamSkope-<version>-darwin-arm64.dmg"
# Linux
sha256sum "StreamSkope-<version>-linux-x64.AppImage"
```

```powershell
# Windows PowerShell
Get-FileHash ".\StreamSkope-<version>-win32-x64-Setup.exe" -Algorithm SHA256
```

Replace `<version>` with the downloaded version. If you downloaded all three
installers, `shasum -a 256 -c SHA256SUMS` (macOS) or
`sha256sum -c SHA256SUMS` (Linux) checks the complete set.
On a mismatch, stop and obtain a fresh copy from the trusted release.

## Browser alternative

Use [Install the browser workbench](containerlab.md) for a Linux-hosted workbench
with an encrypted connection vault. Its installer command appears only when the
documented release contains the verified installer. For earlier releases or
restricted networks, use [manual deployment](../guide/browser-host.md#manual-deployment).
It connects to your own Kafka or NATS servers and avoids desktop OS signing warnings.

## Desktop prerequisites

The packaged app needs no Node, Docker, Containerlab or Java installation. Kafka
is a separate reachable service. Choose the released CPU architecture; macOS Intel,
Windows ARM64 and Linux ARM64 installers are not currently published.

| Package     | Native release-check environment | Required on the user's machine                                                  |
| ----------- | -------------------------------- | ------------------------------------------------------------------------------- |
| macOS ARM64 | macOS 15                         | Apple Silicon desktop session; working OS credential service                    |
| Windows x64 | Windows Server 2025 runner       | x64 Windows desktop session; working user credential protection                 |
| Linux x64   | Ubuntu 24.04                     | Graphical desktop, AppImage runtime support, unlocked Secret Service or KWallet |

The middle column records where CI builds and launches the app, not a claim that
all other OS versions are supported. Minimum OS versions and additional Linux
distributions have not been qualified by this release pipeline.

On Linux, profile storage is unavailable when Electron selects `basic_text` or
`unknown` secret protection. Configure/unlock the desktop's credential service,
then restart StreamSkope. Do not work around this by forcing plaintext password
storage. If the AppImage cannot mount or launch, have the workstation administrator
check AppImage runtime/FUSE support and desktop dependencies before troubleshooting
Kafka. Use [recovery guidance](../guide/recovery.md) if saved profiles fail to load.

## macOS

The unsigned release and locally built app are not Developer ID signed or notarized. Verify their source before
granting a macOS security exception.

1. Obtain the DMG and expected checksum from a trusted source.
2. Verify the release checksum as described above. For a locally built DMG with
   its individual checksum file, use:

    ```sh
    shasum -a 256 -c <filename>.dmg.sha256
    ```

3. Open the DMG and drag **StreamSkope.app** to **Applications**.
4. Open StreamSkope. If blocked, use **System Settings → Privacy & Security → Open Anyway**
   for this app.

On a checksum mismatch, stop and obtain a verified replacement. A checksum
confirms file integrity, not publisher identity or malware safety.
Only if the per-app opening flow is unavailable and you have verified
the trusted copy, use:

```sh
xattr -dr com.apple.quarantine "/Applications/StreamSkope.app"
open "/Applications/StreamSkope.app"
```

Do not add `sudo`, use wildcards or broader paths, or disable Gatekeeper or
System Integrity Protection. To remove this exception, quit and delete this app
bundle; obtain a fresh download to restore download checks.

## Windows and Linux

1. Verify the package's origin and checksum before opening it.
2. On Windows, run the `Setup.exe` installer. Unsigned builds may trigger
   SmartScreen; confirm its origin before proceeding.
3. On Linux, give the AppImage executable permission, then open it in a compatible
   desktop environment.
4. Launch StreamSkope and open **Connection Profiles**.

**You should see:** the workbench ready for a connection. Kafka is a separate
service; the desktop package does not include a broker.

## Next: read your first message

[Continue the desktop quickstart →](quickstart.md#2-connect-your-kafka)

For an upgrade, preserve your settings using [Backup and recovery](../guide/recovery.md).
Source builds are covered in [CONTRIBUTING.md](https://github.com/asadarafat/streamskope/blob/main/CONTRIBUTING.md#build-a-desktop-package).
