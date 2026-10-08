# Fix a connection or reading problem

Start with the failed action and its log entry, then check the matching cause.

## Find the failed operation

1. Expand **Raw logs** at the bottom of StreamSkope.
2. Find the entry for the failed action. Filter by text or severity if needed.
3. Note the stage, category, affected object and correlation ID.
4. Confirm that the expected connection profile is active.
5. Use the checks below, then retry the action after correcting its cause.

| What you see                      | What to do next                                                                                                |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| OAuth endpoint unreachable        | Check its hostname and port from the application host, then service readiness and TLS trust.                   |
| Certificate hostname mismatch     | Use a broker address covered by the certificate. Changing the browser URL does not change Kafka's TLS name.    |
| Topics unavailable                | Check the connection status and the principal's permission to describe the topics.                             |
| Kafka works but Registry does not | Check the Registry URL, its service status and its own authentication/permissions.                             |
| Secret retrieval incomplete       | Check the retrieval result, command output/extraction, file permissions and chosen trust format.               |
| SSH identity changed              | Verify the server fingerprint independently before accepting it.                                               |
| Profile cannot be saved           | Check the OS credential service and access to the protected store and backup.                                  |
| No messages shown                 | Clear filters, confirm the topic and read mode, then check whether the request is waiting, finished or failed. |
| Counts stop updating              | Open **Monitor** and check freshness, stream state, queues and display-drop counters.                          |

**You should see:** a successful retry or a more specific failing stage to
investigate. Keep the correlation ID when escalating the problem.

## Broker connection times out after bootstrap succeeds

Kafka returns advertised broker addresses after contacting a bootstrap server.
Every returned address used by the client must be reachable from the desktop host;
a reachable bootstrap address alone is insufficient.

1. In **Raw logs**, identify whether the failed stage is connection, metadata or
   fetch, and record the target hostname/port if present. Ask the administrator for
   the broker's advertised listener addresses when the error omits them.
2. Check each returned hostname from the machine running StreamSkope, not just from
   a Kubernetes pod or browser. For example, replace `broker.example.net` and `9093`:

    ```sh
    # Linux/macOS, where nc is installed
    nslookup broker.example.net
    nc -vz -w 5 broker.example.net 9093
    ```

    ```powershell
    Resolve-DnsName broker.example.net
    Test-NetConnection broker.example.net -Port 9093
    ```

3. DNS should resolve to an intended reachable address and TCP should connect. A
   timeout points to routing/firewall/listener access; a refused connection points
   to the destination port/listener. Neither result establishes TLS or Kafka access.
4. Have the administrator provide a reachable advertised listener or approved
   network route. Retry **Test connection**, connect the saved profile and read a
   known topic. Replacing only the bootstrap address cannot fix inaccessible
   addresses returned in metadata.

## TLS trust or hostname failure

First identify the failing endpoint. The [trust matrix](tls-trust.md) distinguishes
broker/profile HTTPS trust from EDA/NSP API trust. A plugin API checkbox does not
change Kafka or saved OAuth verification.

1. Confirm that the broker name is covered by the certificate and that the profile
   contains its issuing CA chain. JKS/PKCS12 passwords unlock trust material;
   they are not Kafka credentials.
2. If OpenSSL is available and you have the administrator-provided PEM CA file,
   inspect the same endpoint without sending Kafka credentials:

    ```sh
    openssl s_client -connect broker.example.net:9093 \
      -servername broker.example.net -verify_hostname broker.example.net \
      -CAfile /path/to/broker-ca.pem -verify_return_error </dev/null
    ```

3. Expect a successful certificate verification. An unknown issuer requires the
   correct CA/intermediate chain; expiry requires certificate renewal; a hostname
   mismatch requires an appropriate listener name/certificate. Check the desktop
   clock if a certificate appears not yet valid.
4. Update the profile's trust material, test, save and reconnect. Do not disable
   certificate validation or switch to plaintext to clear a TLS failure.

This example accepts PEM, not a JKS/PKCS12 binary file. For the OAuth endpoint,
check its own HTTPS hostname and CA as well; broker TLS success does not validate
that separate endpoint.

## OAuth failure

| Observation                                        | Check                                                                                          | Expected result / next action                                                                |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Token endpoint DNS, timeout or TLS error           | Apply the host/network and TLS checks above to the token URL                                   | A reachable trusted endpoint; fix this before changing credentials                           |
| Token request rejected                             | Administrator checks endpoint path, client ID, secret validity and allowed scope/grant         | A permitted client-credentials grant; re-enter a rotated secret and retest                   |
| Token succeeds, broker rejects authentication      | Confirm OAUTHBEARER is enabled; administrator checks issuer, audience, expiry and broker clock | Broker accepts that token; this is distinct from topic authorization                         |
| Authentication succeeds, topic access denied       | Review the principal's topic ACLs                                                              | Minimum required Describe/Read permissions; do not broaden to administrator merely to browse |
| Kafka succeeds, Registry/Admin API returns 401/403 | Confirm that service accepts the same token, audience and roles                                | Configure an accepted service auth mode or leave the optional service disabled               |

Use the redacted failure stage and correlation ID to find the corresponding server
log. Do not paste access tokens into online JWT tools or attach token responses to
an issue. Compare the supported authentication modes in [Compatibility](../start/compatibility.md).

## Linux profiles cannot be saved or decrypted

1. Confirm a graphical user session and unlock the session's credential service.
   With a Secret Service desktop, `busctl --user status org.freedesktop.secrets`
   checks whether its D-Bus service is present; an active service does not prove the
   keyring is unlocked. KWallet desktops use their wallet service instead.
2. Unlock the existing keyring/wallet using the desktop's password manager. Restart
   StreamSkope under the same user/session and check **Profile storage status**.
3. Expect **OS-protected profiles**. A `basic_text` or `unknown` backend is refused.
   Do not force plaintext storage or delete the keyring. Missing user D-Bus/session
   integration needs workstation administrator attention.
4. If storage becomes available but existing profiles cannot decrypt, preserve the
   app data and follow [recovery](recovery.md). Reinstalling the application does
   not recreate a lost OS credential key.

## Recover a saved profile store

Follow [Upgrade, back up and recover](recovery.md) for the OS-specific data
location, backup inventory and exact migration generation to restore. Preserve
all recovery files before changing the store. A generic `.pre-upgrade.bak` can be
older than the snapshot needed for your downgrade.

For plugin or cluster failures, use [EDA diagnosis and cleanup verification](../plugins/eda.md#administrator-diagnosis-and-cleanup-verification).
For a save failure on Linux, check the unlocked credential service described in
[installation prerequisites](../start/installation.md#desktop-prerequisites).

## Share useful evidence

In the Kafka workspace, expand **Raw logs**, apply the relevant filters, then
select **Export → Support report**. This JSON report identifies the application
release and contains only timestamps, severity, outcome and valid correlation
IDs from the visible retained activity. It includes retained, visible and exported
counts, so a filtered view cannot be mistaken for a complete history. It does not
contain free-form log text or determine the cause of a failure.

**Export → Visible raw logs** remains available when you need the original
activity text. Review it before sharing: operational identifiers can remain in
raw logs even when credentials are redacted. Neither export retrieves older
activity, startup logs or records from the broker.

Report defects at [GitHub Issues](https://github.com/asadarafat/streamskope/issues).
Include the release tag/build as well as app version, OS/architecture, action, expected result, actual result
and redacted correlation ID. Exclude passwords, tokens, trust material, private
payloads and full application-data directories. Review the [data and export inventory](data-handling.md)
before attaching evidence.

## Browser development does not start

1. Check `node --version`; use Node 24.x.
2. Confirm Docker is running and Containerlab and Java `keytool` are available.
3. Use dependencies installed for this operating system and CPU. The development
   bootstrap can isolate an incompatible shared installation under `.cache/`.
4. Run `npm run dev` and use the address it prints.
5. WebDev normally derives the current OrbStack VM hostname and falls back to
   loopback. To force loopback, launch with:

    ```sh
    STREAMSKOPE_DEV_PUBLIC_HOST=127.0.0.1 npm run dev
    ```

The launcher can reuse its owned server. If another process occupies a requested
port, resolve that conflict and retry; the launcher leaves unrelated processes running.

## Local profile settings

When manually connecting to the default local AIO fixture, use the values printed
at startup. Its default endpoints are:

| Setting           | Default                                                      |
| ----------------- | ------------------------------------------------------------ |
| Bootstrap brokers | `127.0.0.1:19093`                                            |
| Trust material    | PEM CA path printed by fixture startup                       |
| OAuth endpoint    | `http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token` |
| Schema Registry   | `http://127.0.0.1:18081`                                     |

The public development OAuth values are in `aio-kafka/fixture.config.json`.
Use them only for the local fixture. A desktop app running on another machine
needs reachable endpoints and a broker certificate covering that hostname.

## Try again

[Return to the desktop quickstart →](../start/quickstart.md)
