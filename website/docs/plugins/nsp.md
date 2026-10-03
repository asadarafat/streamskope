---
unreleased: true
---

# NSP Capture

<span id="connect-to-nsp-kafka"></span>

NSP Capture creates a Kafka connection profile using your NSP API URL, username
and password. It retrieves the broker's CA truststore and matching password through
an NSP workflow, tests the connection, then saves the profile.

It is an optional [connection plugin](index.md), adding **Connect to NSP** under
**Add connection** and **Refresh NSP credentials** to its saved profiles. The
workflow is a reusable helper inside NSP; the plugin does not deploy a broker or
keep a tunnel running after setup.

## Before you start

- Use a [supported desktop and plugin](../start/compatibility.md).
- The StreamSkope host must reach the NSP HTTPS API and the Kafka broker's
  advertised addresses. The plugin creates no broker, port forward or tunnel.
- Your NSP account needs permission to read, create and publish the helper
  workflow, execute it, inspect its output, and cancel/delete its own executions.
  It also needs permission to read the product version.
  Ask your NSP administrator to map these operations to your site's permissions.
- NSP's workflow service must support `nsp.python`, provide `keytool`, and mount
  the Kafka CA JKS and its password under `/opt/nsp/os/ssl/certsmanager`. The
  helper rejects stores containing private-key entries. This mounted layout is
  required in addition to the declared target-version range.
- Review [NSP API certificate trust](../guide/tls-trust.md#eda-and-nsp-api-certificates).
  The dialog cannot upload a custom CA; the API certificate must be trusted by
  the desktop host. Kafka separately validates the broker
  certificate against the retrieved CA truststore, including its hostname.

You do not supply Kubernetes credentials or arrange SSH access. The helper runs
inside NSP using the workflow service's existing access to the mounted trust files.

## Create the connection profile

1. Open **Preferences → Plugins** and install **NSP Capture**. Activation is
   immediate. The catalog must contain a published compatible package.
2. Open **Add connection → Connect to NSP**.
3. Enter the **NSP API URL** as an HTTPS origin, such as
   `https://nsp.example.com`, plus **NSP username** and **NSP password**.
4. Keep **Verify NSP API certificate** enabled. Only for a trusted development
   lab, you may explicitly disable verification for this API retrieval. That
   choice does not disable Kafka broker or saved OAuth endpoint verification.
5. Leave **Kafka authentication** at **Detect automatically**, unless your
   administrator specifies **TLS only** or **TLS and NSP OAuth**. Automatic mode
   tests TLS first and tries OAuth only when Kafka reports an authentication failure.
6. Click **Create connection profile**. Wait for retrieval, execution cleanup,
   Kafka connection testing and profile saving to complete.
7. Select the saved profile's connect action, then open **Topics**.

**You should see:** the topics the broker makes available to this connection.
The plugin does not create topics or produce fixture messages. An empty topic or
message list can still require broker permissions, an active producer, or a
different [message read mode](../guide/messages.md#choose-a-read-mode).

The default bootstrap address is the API hostname on port **9192**. If your Kafka
listener differs, enable **Override default Kafka brokers** and enter reachable
`host:port` addresses. Kafka metadata may return additional broker
addresses that also need to be reachable and match their certificates.

## Refresh, cancel and recover

| Action                                      | Result                                                                                                                                 |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Repeat setup for the same API               | Reuses the existing NSP profile instead of creating a duplicate; multiple matching profiles require selecting one explicitly           |
| Refresh after certificate/password rotation | Disconnect the profile, open **Refresh NSP credentials**, enter API credentials again, then choose **Refresh credentials**             |
| Cancel an operation                         | Cancels owned work and waits for execution cleanup; an already committed profile save remains successful                               |
| Cleanup cannot be confirmed                 | Retains non-secret API/account and request/execution identifiers across restart; blocks new work until cleanup succeeds                |
| Recover pending cleanup                     | Reopen NSP capture, use the same API URL and account, then choose **Retry cleanup**; use **Refresh status** to check the current state |
| NSP upgraded beyond the declared target     | New retrieval stops; pending cleanup is still permitted without a version check                                                        |
| Update or remove the plugin                 | Cancels active work when confirmed and requires cleanup to succeed; failure leaves the plugin available for retry                      |
| Successful removal                          | Disconnects its active connection and removes plugin code; saved profiles and the shared helper workflow remain                        |
| Reinstall                                   | Restores the NSP UI for retained profiles; it does not create another helper with the same definition                                  |

Saved profiles use the core [credential protection](../guide/data-handling.md#stored-data).
API credentials used only for retrieval are not stored in the recovery journal.
If Kafka requires NSP OAuth, the profile retains the supplied credentials as
protected OAuth settings so later connections can obtain tokens. Retrieved
truststore material and passwords are handled by the host and are not returned
to the plugin UI.

Removing the plugin does not make retained NSP profiles ordinary profiles:
reinstall a compatible plugin before reconnecting them. The helper stays available
for other StreamSkope users; an NSP administrator may retire it separately after
confirming no clients depend on it.

## Diagnose a failure

| Failure                                | Next check                                                                                                  |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| NSP Capture absent from the catalog    | Check the supported desktop interval, then refresh the official catalog                                     |
| API sign-in or certificate failure     | Verify the HTTPS origin, account permissions and API CA trust                                               |
| Target version unknown or unsupported  | Check `/sdn/api/v4/system/version` and the manifest's inclusive target range; cleanup is still available    |
| Helper name conflict                   | Have the administrator inspect the existing definition; do not overwrite it to bypass the ownership check   |
| Workflow export rejected               | Verify the mounted JKS/password layout, `keytool` availability and certificate-only store contents          |
| Kafka certificate/name failure         | Use the correct advertised broker hostname and CA; the API lab override does not weaken broker verification |
| Kafka unavailable or no visible topics | Check listener/network access, authentication and topic permissions separately from successful API login    |
| Cleanup required                       | Restore API access and retry cleanup using the same account; do not discard the recovery state              |

Share the desktop/plugin versions, failing step and redacted correlation ID from
**Raw logs**. Exclude passwords, tokens, truststore bytes and workflow output.

[Open a topic and inspect messages →](../guide/messages.md)

## Components and compatibility

The plugin runs in the desktop host; **`streamskopeNspCaptureV1`** runs inside
NSP's workflow service. See the [compatibility matrix](../start/compatibility.md)
for desktop and plugin requirements. The
[qualification record](../guide/qualification.md)
states which environment and operations were actually exercised.

### Compatibility package

The development API 4 package targets **NSP 26.4.0** and declares desktop
**>=0.4.0, <0.6.0**. These are compatibility bounds, not a promised release
number; the plugin version is assigned when its release workflow starts. See
the [manifest-derived declarations](versioning.md#declared-packages) for its
development identity, desktop bounds and inclusive target range. The official
release catalog establishes which packages are available to install.

The plugin reads `GET /sdn/api/v4/system/version` before creating,
adopting or executing its helper for setup or credential refresh. For example,
`NSP-CN-26.4.0-rel.200` means product **26.4.0**, build **200**; `v4` is the API
version. An unknown format, unreadable version or product outside the declared
range prevents new retrieval and profile changes. Pending cleanup remains available.
Workflow permissions and mounted-file prerequisites still apply.

## What the workflow changes

The plugin creates or reuses **`streamskopeNspCaptureV1`**, an immutable shared
helper. Before reuse it checks the definition against the bundled content. A
different definition under that name is a conflict; it is left unchanged for an
administrator to inspect. The original export workflows and Kafka configuration
are not modified.

### Review the packaged workflow

The API 4 package includes **`nsp-capture.workflow.yaml`** as a declared
resource inside its downloadable `.skope-plugin`. Packaging also exports the
same YAML beside the package and manifest for review:

| File                                                          | Purpose                                                  |
| ------------------------------------------------------------- | -------------------------------------------------------- |
| `streamskope-nsp-v<plugin-version>.skope-plugin`              | Installable plugin, including the workflow resource      |
| `streamskope-nsp-v<plugin-version>-plugin.json`               | Compatibility declarations and workflow resource SHA-256 |
| `streamskope-nsp-v<plugin-version>-nsp-capture.workflow.yaml` | Readable copy of the same workflow                       |

For source builds, `npm run package -- plugin nsp` writes these files to
`dist/plugin-package/`. Download the YAML and manifest from the same official
release as the plugin to inspect what will run. Compute the YAML's SHA-256 with
`sha256sum`, macOS `shasum -a 256`, or PowerShell `Get-FileHash -Algorithm SHA256`
and compare it with the manifest resource entry whose path is
`nsp-capture.workflow.yaml`. The desktop verifies the downloaded package digest
and declared resource hash before activation; a digest establishes integrity,
not a separate publisher signature.

The downloadable workflow has the same bytes and fingerprint as the installed
resource. An already-created matching `streamskopeNspCaptureV1` is reused. **No manual workflow upload is required:** install the
desktop plugin, then use **Connect to NSP**; the plugin creates, verifies and
publishes the helper through the API as needed. Editing the review copy does not
customize the installed plugin. A different remote definition under the owned
name remains a conflict.

### Retrieval and output handling

Each retrieval has its own execution identity. The helper retrieves the mounted
truststore and password in one task, validates that the store contains certificate entries only, and returns
the JKS and its matching password together. The host verifies the returned size
and digest, removes the owned execution, and confirms it is absent before saving
the Kafka profile. Removal of an already absent owned execution succeeds.

Workflow output temporarily contains credential material and is visible in NSP to
principals permitted to inspect that execution. Verified execution deletion does
not establish erasure from NSP audit logs, backups or administrator exports.
Apply your site's workflow access and retention controls; do not attach raw
execution output to issues.

## Call flow

The plugin performs retrieval and cleanup in the desktop host. The helper combines
truststore and password retrieval in one NSP task; the browser-facing dialog
receives progress and a saved profile identity, never the returned secret values.

```text
User: NSP URL + API credentials
  |
  v
Desktop plugin/host
  | Authenticate
  | Reconcile cleanup
  | Read/check NSP product version
  v
NSP API: get/create helper
  | Verify exact definition
  | Publish matching draft
  v
Desktop: persist request identity
  | Execute helper
  v
NSP workflow
  | Read CA JKS + password
  | Reject private-key entries
  | Return material + digest
  v
Desktop: validate size + digest
  | Delete owned execution in NSP
  | Confirm output absent
  v
NSP Kafka: TLS connection test
  | Auth failure? Try NSP OAuth
  v
Desktop: save/refresh profile
  |
  v
User: connect directly -> topics
```

The helper is created once and reused only if its definition matches the bundled
version. A published matching helper needs no change. NSP API sign-in retrieves
connection material; it is separate from the Kafka connection test. In automatic
mode, an authentication failure on the TLS test triggers a retry with NSP OAuth.
Other failures do not silently change the authentication mode.

```text
Repeat setup
  -> reconcile interrupted work
  -> check version
  -> reuse matching helper
  -> retrieve; clean; test
  -> refresh profile

Cancel / update / remove
  -> cancel owned work
  -> delete exact owned execution
     absent -> continue
     retain profile
     failed -> retain journal
     allow retry

Restart with pending cleanup
  -> same API/account signs in
  -> Retry cleanup
  -> verify absence
  -> clear journal
  -> allow new retrieval

NSP upgrade / version unavailable
  -> block new setup or refresh
  -> keep owned cleanup available
```

Cleanup includes the execution's task/action output. Already absent owned work is
a successful cleanup result; unrelated executions are left alone. A lost response
is reconciled using the persisted request identity, avoiding a blind second
execution. The plugin saves no new connection profile until cleanup and the Kafka
test have succeeded. Cancellation after a completed profile save does not undo
that save.
