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

## Browser development does not start

1. Check `node --version`; use Node 24.x.
2. Confirm Docker is running and Containerlab and Java `keytool` are available.
3. Use dependencies installed for this operating system and CPU. The development
   bootstrap can isolate an incompatible shared installation under `.cache/`.
4. Run `npm run dev:web` and use the address it prints.
5. If `clab.orb.local` does not resolve, launch with:

    ```sh
    STREAMSKOPE_DEV_PUBLIC_HOST=127.0.0.1 npm run dev:web
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

## Recover a saved profile store

Desktop profiles are stored in `profiles/kafka-profiles.json` under the host's
application-data directory. A protected `kafka-profiles.json.pre-upgrade.bak`
beside it preserves the pre-upgrade store; it is not a continuous backup.
Browser session profiles are not durable backups.

If storage fails, preserve both files and check OS credential-service access.
Do not delete the backup merely to unblock saves. To roll back:

1. Quit every StreamSkope process using the store.
2. Make separate copies of the current store and recovery file.
3. Restore a known-good pre-upgrade copy as `kafka-profiles.json`, preserving
   restricted permissions and the same OS user and credential context.
4. Start the matching earlier application and inspect profiles before connecting.
   Changes made after the backup will not be present.

Do not decrypt stores, edit schema versions or transfer them to another account
to bypass protection. Older builds must not edit newer payloads. Native
credential-service recovery still requires platform verification.

## Share useful evidence

Include the app revision, OS/architecture, action, expected result, actual result
and redacted correlation ID. Exclude passwords, tokens, trust material, private
payloads and full application-data directories.

## Try again

[Return to the local quickstart →](../start/quickstart.md)
