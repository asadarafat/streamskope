---
title: Inspect and export with the CLI
description: Use bounded inspect, query and export commands with shared Kafka read and masking behavior.
---

# Inspect and export with the CLI

The source distribution includes a Node 24 command-line entry for **inspect**, **query** and **export**. Use the source archive or checkout matching your desktop release, then run `npm ci`. There is no separate CLI executable bundled into the desktop installer. Mutation commands, desktop profile-store access, plugin activation and continuous tailing are unsupported.

The CLI uses the desktop Kafka engine, finite-query parser and record-protection policy. It reads explicit host configuration; it does not inherit a desktop session's profiles or masking preferences.

## Private connection configuration

Create `private.json` outside your Git repository:

```json
{
    "connection": {
        "name": "Local sandbox",
        "brokers": ["127.0.0.1:19096"],
        "tls": { "enabled": false }
    },
    "protection": {
        "readOnly": true,
        "maskKey": false,
        "maskHeaders": ["authorization"],
        "valuePaths": ["/customer/email"]
    }
}
```

On Linux/macOS, restrict the file with `chmod 600 private.json`. The command rejects symlinks, non-regular files, files larger than 1 MiB and group/world-readable configuration. On Windows, manage the file's access-control permissions yourself. Keep credentials in this protected file, never command arguments. Configuration errors and raw broker diagnostics are withheld from machine output.

For TLS, use `tls.enabled: true` and `tls.caPem` containing PEM trust text. OAuth uses the same `oauth` object as a desktop connection input: `clientId`, `clientSecret`, `scope`, `tokenEndpoint`. Desktop trust-acquisition handles cannot be used. Read-only behavior is always enabled even if the input says otherwise.

## Commands and output

```sh
npm run dev -- cli inspect --config private.json
npm run dev -- cli query --config private.json --query query.json
npm run dev -- cli export --config private.json --query query.json --output new-records.ndjson
```

`inspect` emits cluster identity, broker count and topic names. `query` writes records and a final coverage summary as newline-delimited JSON. `export` writes the same stream to a **new** file with mode `0600` on POSIX; an existing destination is never overwritten, and a failed export removes only the partial file it created. Use `node --import tsx tools/cli.ts ...` when piping stdout into another program; npm itself may print its command banner.

Every result has `format: "streamskope.cli/v1"` and a `kind` of `inspection`, `record`, `summary` or `error`. Record output follows the shared `KafkaMessage` contract, including retained original bytes when masking is disabled. Enabling any record masking removes the original-byte side channel. Like the desktop, the CLI rejects broker-side search whenever any record masking is enabled; this prevents matches from revealing hidden values. Read a bounded range and inspect its protected output instead. Treat unmasked exports as sensitive data.

Example `query.json`:

```json
{
    "topic": "sandbox.events",
    "mode": "earliest",
    "maxMessages": 10,
    "search": {
        "key": "event-1",
        "value": "",
        "offset": "",
        "timestamp": "",
        "partition": null
    }
}
```

Finite `earliest`, `newest` and `time-window` reads share the app's query semantics. Time windows additionally require `startTimeMs` and `endTimeMs`. When masking is disabled, an optional `search` object supports `key`, `value`, `offset`, `timestamp` string filters, a nullable `partition` and the app's bounded `expression` language. The CLI reads at most 1,000 results and emits at most 8 MiB of record output, with a 60-second command deadline and the engine's lower scan/byte/time bounds. It does not perform interactive Registry decoding; structured binary records follow raw-read masking behavior.

Inspect the final `coverage` and `stopReason`. A successful process may have a bounded partial result: `complete` is true only when the requested retained range was fully scanned. It does not promise that historical data still exists or that an entire topic was exported. Interrupting a query can leave partial stdout; an absent summary means the stream did not finish normally.

| Exit  | Meaning                                                               |
| ----- | --------------------------------------------------------------------- |
| `0`   | Read completed; inspect coverage for truncation or limits             |
| `1`   | Connection, read or output operation failed                           |
| `2`   | Invalid arguments, private configuration, query or output destination |
| `130` | SIGINT/SIGTERM cancellation; active clients close                     |

There are no CLI produce, offset-reset, connector-mutation or promotion commands. Use the desktop's reviewed actions for those operations.
