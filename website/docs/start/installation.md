# Install StreamSkope

Download StreamSkope from [GitHub Releases](https://github.com/asadarafat/streamskope/releases/latest),
or [try the browser workbench with local Kafka](quickstart.md).

## Download

Choose the file for your operating system and CPU:

| Platform             | Download                                    |
| -------------------- | ------------------------------------------- |
| macOS, Apple Silicon | `StreamSkope-<version>-darwin-arm64.dmg`    |
| Windows x64          | `StreamSkope-<version>-win32-x64-Setup.exe` |
| Linux x64            | `StreamSkope-<version>-linux-x64.AppImage`  |

These releases are unsigned: macOS builds have no Developer ID signature or
notarization, Windows installers have no Authenticode signature, and Linux
downloads have no publisher signature. OS security warnings are expected.

Download `SHA256SUMS` from the same release. Compare your file's digest to its
entry in that file before installing:

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

## Build a package

1. Open a local checkout of StreamSkope on your target platform.
2. Use **Node 24.x** and install dependencies for that operating system and CPU:

    ```sh
    npm ci
    ```

3. Run the command for your platform:

    | Platform    | Command                                                                            | Output                                  |
    | ----------- | ---------------------------------------------------------------------------------- | --------------------------------------- |
    | macOS ARM64 | `npm run package:dmg:macos`                                                        | DMG in `dist/release/unsigned-macos-*/` |
    | Windows x64 | `npm run package:installer:windows`                                                | NSIS `Setup.exe` in `dist/release/`     |
    | Linux x64   | `npm run package:verify:linux`, then `npm run package:appimage:linux:from-package` | AppImage in `dist/release/`             |

4. Wait for package verification and the packaged launch test to finish.

**You should have:** the package at the printed output path. A successful build
checks the package; it does not establish signing, notarization or production support.

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

## Next: connect your cluster

[Add a Kafka connection profile →](../guide/connections.md)
