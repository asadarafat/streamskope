# StreamSkope secure NATS fixture

This persistent development lab is a sibling of `aio-kafka`. It runs one
digest-pinned Core NATS server with verified TLS and a generated token. The
same server definition powers disposable real-provider and browser tests;
those tests create separate containers and never remove this lab.

This fixture is development infrastructure. It is excluded from desktop
packages. Keep it on loopback and do not reuse its credentials for production.

## Prerequisites

- Linux arm64 or amd64, including an OrbStack guest
- Node.js 24 and `npm ci`
- Docker and OpenSSL

Run commands from the repository root. No Containerlab or JetStream is needed.

## Start and connect

```bash
npm run dev
```

The web-development launcher ensures both AIO labs are healthy and seeds an
empty, host-owned NATS profile store with a session-only **Local AIO NATS**
profile. Select the **NATS** provider, connect that profile, and subscribe
to `streamskope.fixture.>`.

The NATS lab can also run without the workbench:

```bash
npm run dev -- nats start
npm run dev -- nats status
```

The default server is `nats://127.0.0.1:14222` with token authentication and TLS
certificate verification. Commands print the server, subject and private CA/token
file paths; they never print token values or signing keys. For an installed
desktop app, create a NATS profile using that endpoint, token and CA certificate
on the same Linux host. An app on another host needs an explicitly configured
secure tunnel: the fixture is intentionally not exposed beyond loopback.

Repeat `start` to reuse the verified owned server and certificates. It resumes
stopped containers without deleting data or rotating credentials. Generated
certificates last one year. Change the port in `fixture.config.json` only after
stopping the owned fixture; an occupied port or foreign container is never
automatically removed.

## Publish live samples

Start your subscription first, then run in another terminal:

```bash
npm run dev -- nats publish
```

This publishes two messages per second for 60 seconds to
`streamskope.fixture.events`. Each message carries a JSON payload, sequence,
timestamp, content type, fixture identifier and correlation ID. The canonical
subject and payload live in `fixture.config.json`.

For a longer or faster bounded sample:

```bash
npm run dev -- nats publish --seconds 120 --rate 10
```

The maximum is 600 seconds at 100 messages per second. Publication stops after
the requested count; the lab has no background producer. Core NATS does not
retain these messages. A subscription started after publication sees only
subsequent messages; there is no topic inventory, retained seed or history.

## Resource and readiness behavior

The server is capped at 128 MiB, half a CPU and 64 processes. Its root filesystem
is read-only, privileges are bounded and debug/trace logging is disabled. TLS
keys and configuration enter only its owned private volume; the CA signing
key stays outside the container. Secrets are generated inside an ignored private
directory, with directory mode `0700` and file mode `0600`.

Startup verifies TLS and token authentication, then independently subscribes,
publishes and reads back an exact readiness value before saving ownership
evidence. Readiness uses a unique subject and opens no lasting subscription.
Each SDK PONG/message wait has a deadline and closes its owned client on failure;
a stalled server cannot keep a lifecycle command waiting indefinitely.
`status` repeats that round trip; it does not rotate credentials or change the
sample configuration.

## Stop and recover

```bash
npm run dev -- nats stop
```

Stop requires the stored container ID, name, pinned image and UUID labels to
match the daemon. It removes only that server, its anonymous private volume,
generated certificate directory and ownership record. Repeat stop is harmless.

If startup fails, check Docker/OpenSSL, the configured port and the private
ownership directory. Do not delete a conflicting container that belongs to
another project. Interrupted startup has a private UUID intent written before
container creation; retry compensates only a matching owned container. If stop
already removed the container, retry finishes private file cleanup after successful
daemon queries confirm that its full ID and name are absent. A failed daemon
query or foreign name is never treated as successful cleanup. Changed configuration
requires explicit stop; the launcher refuses to claim an unverified resource.
Expired certificates can be renewed by stopping a verified
owned fixture and starting it again.

If a Docker create call fails or times out without returning a container ID,
its daemon-side completion may still be pending. The private intent and files
remain available; a single empty container listing cannot establish cleanup.
Retry `stop` once the matching UUID container appears. If it never appears,
establish that the original daemon request has quiesced before manually clearing
that intent and its preparation directory. Preserve the evidence when that
cannot be confirmed, and account for other labs before performing Docker recovery.
The launcher never starts a second competing creation request in this state.

Normal cancellation waits for bounded operations and owned cleanup before exiting.
Lifecycle commands serialize through `ownership/.operation.lock`. If a process
was forcibly terminated and the lock remains, verify that no fixture command
is still running before removing that lock directory and retrying. Do not
remove the ownership record of a running fixture: it is required to prove
safe cleanup.

## Qualification

```bash
npx --no-install vitest run --config config/vitest.config.ts \
  test/nats/provider-real.test.ts
```

The four real tests use isolated instances of this shared pinned definition.
They independently verify token/TLS message fidelity, wrong credentials and
CA rejection, IP/DNS certificate matching, permission denial and confirmed
stream stop/reconnect. They preserve the persistent AIO lab and its credentials.

`npm run check` adds the repository's 60-second mixed-message pipeline soak and
configured live EDA/NSP checks. That pipeline soak is distinct from a real NATS
server round trip; record the actual real-provider evidence separately when
qualifying a release.

For an explicit real-server performance qualification, start this lab first and
run:

```bash
npm run dev -- nats start
node --import tsx test/performance/nats-live-soak.ts
```

This standalone soak verifies the existing ownership/readiness, then offers
60,000 records at 1,000 per second over 60 seconds through an independent SDK
publisher and the production provider endpoint. It checks JSON/headers, exact
record accounting, omissions, credential isolation, confirmed repeat stop,
post-stop silence, repeat disconnect and owned client cleanup. It never creates,
stops or removes the persistent server.

The structured result is `dist/performance/nats-live-soak.json`, using the
repository's existing CPU, sampled RSS, event-loop and minimum throughput
budgets. A 65-second load deadline and overall cancellation guard prevent a
stalled run from qualifying an arbitrarily slow workload. The offered-rate ratio
uses the actual measured load duration, so a late completion cannot hide a
throughput shortfall. Queue maxima describe publicly observed counters;
internal queue high-water marks remain unmeasured.

CPU, event-loop and RSS measurements cover this Node process during the load.
Server resources, renderer paint, native IPC and UI interaction latency are
not measured. This real NATS soak and the Kafka pipeline/clone replay are
separate measurements. The command is on demand; `npm run check` retains one
automatic 60-second pipeline soak.

## Generated files

All content under `ownership/` is private and Git-ignored. Do not commit token
files, server configuration, certificates, signing keys, ownership records or
Docker runtime output. Published product documentation belongs in `website/docs`.
