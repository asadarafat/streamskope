# Install StreamSkope

You can build a desktop package
on a matching native platform, or [try the browser workbench with local Kafka](quickstart.md).

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

The locally built app is not signed or notarized. Verify its source before
granting a macOS security exception.

1. Obtain the DMG and expected checksum from a trusted source.
2. In the directory containing both files, verify the checksum:

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
