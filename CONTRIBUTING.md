# Development and releases

Use Node 24, `npm ci`, Go 1.24 for the EDA agent, and Python 3.11+ for docs.
Install docs browsers once with `npx playwright install chromium firefox`.

## Commands

Only five commands are exposed in `package.json`. Each calls one matching entry
in `tools/`; helpers live under `tools/dev/`, `tools/build/`, `tools/check/`, `tools/package/`
and `tools/docs/`. The `dev` entry also dispatches the source CLI (`tools/cli.ts`) and owned consumer sandbox (`tools/sandbox.ts`) without adding npm commands. Manual source exports and icon regeneration live in
`tools/maintenance/`; evidence helpers live in `test/support/`.

| Command                 | Purpose                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------ |
| `npm run dev`           | Start the browser workbench and local Kafka development fixture                                  |
| `npm run build`         | Check TypeScript and build the renderer and Electron host                                        |
| `npm run check`         | Run all local CI qualification                                                                   |
| `npm run package`       | Desktop packaging; `-- plugin [eda\|nsp]` builds optional plugins, `-- eda` builds local EDA OCI |
| `npm run docs -- serve` | Serve docs locally; use `build` to build them                                                    |

The source distribution also supports `npm run dev -- cli --help` for bounded inspect/query/export and `npm run dev -- sandbox up|status|consume|transform|down` for an owned local consumer environment. See [CLI behavior and protection](website/docs/guide/read-only-cli.md) and [sandbox lifecycle](website/docs/guide/developer-sandbox.md). Real-system developer qualification lives in `test/kafka/developer-real.test.ts` and `test/kafka/sandbox-real.test.ts`; it is run explicitly, not inferred from shared CI.

Observation and relationship regressions use `test/kafka/observations-real.test.ts`
and `test/kafka/relationships-real.test.ts`; the latter also needs the configured
AIO Registry fixture. `node tools/package/e2e.mjs web test/e2e/web-observations.spec.ts`
creates disposable Kafka/Connect containers and exercises actual UI sampling,
forecasting, stopped collection and graph interactions, with accessibility checks.
These real-system scenarios run explicitly outside shared CI. Configured local
EDA/NSP qualification additionally checks read-only health/relationship metadata
against each platform's broker; that does not qualify every diagnosis or schema
mapping on those targets.

Local CI runs workflow validation, formatting, lint, TypeScript, architecture,
unit/integration tests, EDA source and agent checks, dependency checks, a
**60-second performance soak**, docs qualification and configured live EDA/NSP tests.
The stages are listed directly in `tools/check.sh`; it stops at the first failure.
`npm run docs -- qualify` checks npm command examples in tracked Markdown and the
retrieval guide's required actions and UI labels. It owns documentation tests,
the strict build and Chromium checks. Firefox intro playback checks run when
media, presentation or its toolchain changes, when CI cannot
establish a comparison base, or with `npm run docs -- qualify --media`. Local runs
cache a successful media fingerprint; unchanged prose still gets all routine checks.
CI installs Firefox only when playback is required. Browser checks are separated
into navigation/accessibility, the static server and real media playback helpers;
there are still only five npm commands.
The pinned Zensical search widget has a small compatibility adapter in
`website/docs/assets/search-accessibility.js`. It supplies accessible control names,
combobox selection state, contrast and keyboard focus handling. Recheck the adapter
when upgrading Zensical. Browser qualification requires zero accessibility findings,
including the search dialog, and exercises keyboard selection, filters, closing
and reopening in both themes on desktop and mobile. Its report is retained in
`.artifacts/website/search-accessibility.json`; there are no widget or rule exceptions.
Automated checks alone do not establish full WCAG conformance.
GitHub calls `npm run check -- --ci` to run the same checks without the local
soak or live EDA/NSP stages. It still qualifies documentation in a browser.
The soak exercises the application pipeline at 1,000 records/s with mixed payloads
and clone round trips. Results are in `dist/performance/qualification-soak.json`;
docs results are in `.artifacts/website/`.
Public docs lead with the installed desktop workflow; the optional source checkout
walkthrough lives in `website/docs/start/development.md`. Keep operator facts in
the security, recovery, EDA, NSP and data-handling references, and link to them from
feature guides instead of copying procedures. The existing docs qualification
checks published message limits against the runtime contracts. The shared guide
notice distinguishes development docs from the immutable published-release docs and identifies
the desktop build plus plugin/EDA compatibility. Record operator rehearsals and
their limits in `website/docs/guide/qualification.md`; site browser checks alone
do not validate backup/restore or broker permissions.

For a focused test, run `npx vitest run --config config/vitest.config.ts PATH`.
Vitest uses available CPU capacity, up to four isolated workers, and saves test
results and timings in `.artifacts/ci/vitest.json`. Add `--maxWorkers=1` when
debugging a test. Unit tests should import the module under test directly; use
`test/support/paste-text.ts` for prerequisite text when keystrokes are not the behavior
being tested.
Keep edge cases at their owning layer and exercise shared shell navigation once.
Theme tests check contrast and contract mapping rather than copying design constants.
`tools/check/eda-source.mjs` owns EDA catalog, version and deployment policy checks;
the architecture suite separately checks the plugin's bundled EDA public key.
The source ownership check follows imports from the actual desktop, renderer,
worker and browser development entrypoints. Test-only imports and disconnected
import cycles do not establish runtime ownership. The separately built plugin
backend and renderer are additional production entrypoints. Core source imports
cannot reach plugin implementations. Both hosts load installed plugins optionally;
the EDA plugin uses the application adapter and does not deploy resources through
Kubernetes directly. Generated EDA catalog projections belong on publication
branches, outside the development source tree.
For Electron/web system tests, use `node tools/package/e2e.mjs electron`
(or `web`) with their runtime prerequisites available. Benchmarks and fixture
utilities can be invoked directly through `node --import tsx PATH`.

The manual **Native recovery** workflow takes a published baseline and either a
published target or the current source, and rehearses installer replacement on
Linux x64, Windows x64 and macOS ARM64. It verifies published installers against
`SHA256SUMS`; source targets are rebuilt from the clean checkout, retain
`0.0.0-dev`, and record the exact source revision and installer hash. It creates
an isolated application-data directory, saves a protected profile and query in
the old release, replaces the app, and restores the complete old backup in the
new release. Reconnection and a filtered known-record export must pass without
re-entering credentials. Only the sanitized report is uploaded; app data,
credentials, browser traces and broker logs are removed.

For an unreleased fix, run the candidate rehearsal from a clean native checkout:

```sh
node --import tsx tools/check/native-recovery.ts --from 0.6.0 --candidate dist/installers/StreamSkope-0.0.0-dev-darwin-arm64.dmg --candidate-version 0.0.0-dev
```

Substitute the native installer filename on Linux or Windows. The command builds
the candidate before testing it and does not publish or assign a release.
Candidate mode explicitly omits baseline restart: it backs up the old release
after quitting it, then requires the candidate to reconnect after replacement,
a further restart, and restoration of that backup. A known old-release restart
defect remains separate evidence; candidate success does not qualify the old build.

Published-to-published mode is available with `--from VERSION --to VERSION` and
additionally requires the baseline to reconnect after restart. The recorded macOS
`--from 0.6.0 --to 0.7.0` run failed at the 0.6.0 baseline restart, before installing
0.7.0, because the old preference path collided with Chromium's file. Keep that
case as a known-failing regression, not a successful published-upgrade example.

It needs Java 17+ and an unlocked credential service. Without explicit fixture
connection variables, it starts a checksum-pinned, loopback-only JVM Kafka
broker with TLS and RS256 OAuth authentication. Linux also requires Xvfb, D-Bus
and GNOME Keyring; it creates a temporary Secret Service session. Windows runs
only on disposable Actions accounts because NSIS also changes user registration
and shortcuts. macOS copies the DMG application into an isolated directory;
Linux replaces the AppImage and launches its extracted native application.
This qualifies the native payload and credential recovery, not Linux FUSE or
desktop launcher integration. The workflow is separate from ordinary PR CI and
does not publish a release or authorize cross-account credential portability.

Within `src/features/kafka`, keep behavior with its owner. The UI's
`StreamSkopeWorkbench` composes views; `useWorkbenchActivity`, `useWorkbenchProfiles`
and `useWorkbenchTopics` own their interaction state and effects. The facade's
`ConsumptionFacadeController` owns presentation queues, flushing and terminal
notifications. The application's `KafkaSessionRequests` owns request supersession,
cancellation and stale-response checks; `KafkaApplicationSession` owns connection
and consumer lifecycles. Preserve the typed host contract between UI and facade.

Connection-bound controllers register their invalidation and shutdown drain
together in the facade's ordered `FeatureLifecycle` table. Add a feature there
once rather than maintaining separate connection-change and exit lists. Normal
connection changes invoke invalidation synchronously; shutdown invokes every
owner and waits for all cleanup, even when one owner fails. Consumption retains
its separate final flush. Command admission closes before cancellation and its
final drain follows plugin cleanup, which may still use its owned host commands.

Reviewed writes and observations use session-owned connection scopes instead of
receiving the full active adapter and comparing generation numbers themselves.
Read scopes fence results against connection changes; write scopes validate
immediately before one dispatch and retain that attempt's eventual receipt.
The observation sampler owns its bounded reader cleanup. Other features still
use their existing context contracts; migrate them only with focused acceptance
checks for their cancellation and acknowledgement policies.

`src/platform/node` owns shared backend composition, plugin loading, SSH/HTTPS adapters and
file stores. Browser development and Electron use these same modules. Electron
windows, IPC, exit prompts and operating-system secret protection stay in
`src/platform/electron`; shared Node modules cannot import Electron code.

Provider transport contracts live in `src/platform/providers`. Feature hosts keep
their command-specific execute types; raw wire values stay inside HTTP and IPC
adapters. `ProviderHostRegistry` seals registered routes with the provider's
command, response and event codecs, closes admission synchronously, and shares
one shutdown barrier across every provider. Unknown routes never fall back to
Kafka. Each provider owns its event stream, sequence and delivery queue; a failed
stream cannot change a sibling's readiness. Kafka's existing host API, gateway
paths and IPC channels remain compatibility adapters, and its named backend
options preserve existing protected profile files and connection plugins.
The hosts also compose a Core NATS backend with its own typed protocol (version
2), protected profile store and live subscription lifecycle. NATS does not import
Kafka feature code. Its workspace provides Connection Profiles and Live
Subscription within the existing product frame. Connection plugins such as EDA
and NSP remain Kafka profile extensions; a messaging provider is a built-in sibling.

Core NATS supports no authentication or token authentication, and plaintext or
verified TLS with an optional PEM CA. The engine uses the supported public SDK
and confirms SUB/UNSUB with a broker flush before reporting start/stop. It owns
late connection attempts through real cleanup. Reconnection is explicit, and
there is no subject inventory, stored history or replay. Receipt timestamps are
host observation times. Copied payloads, headers, application queues and transport
queues have independent bounds; omission counters identify their owner.

Each control snapshot carries a monotonic host-lifetime revision, separate from
profile record revisions and delivery counters. The renderer rejects older
control snapshots even when native event delivery trails command receipts.
Protocol 2 requires these revisions; protocol 1 hosts must be upgraded together
with the renderer. Kafka retains its independent protocol.

The NATS renderer uses the NATS command/event codecs and correlates each response
to its submitted command. Browser development uses the named
`/__streamskope_host/providers/nats/{commands,events}` routes and session/memory
profile storage. Native composition resolves the named preload NATS port lazily;
a missing native port is explicitly unavailable and never falls back to HTTP or
Kafka. Resolution is cached at the first NATS visit, preserving Kafka-only
development launchers. New commands/listeners require the current activation's
authority, while admitted receipts and listeners remain intact through confirmed
subscription-stop and disconnect cleanup. Failed cleanup retains the workspace
for retry. The bounded live viewer fences subscription generations and accounts
for viewer eviction separately from application/transport omissions; inspection
retains original UTF-8/base64, headers and host-received UTC evidence.

Core NATS real-server qualification uses `test/nats/provider-real.test.ts` and
the actual Vite product/provider registry in
`test/e2e/web-nats-workspace.spec.ts`, with the checksum-pinned NATS 2.15.0
token/verified-TLS fixture. The ordinary PR job allows 20 minutes: the preceding
provider-backend qualification took about 12.5 minutes before adding actual NATS
browser workflows. Existing test/global deadlines remain unchanged. Record
executed browser and server results for the exact revision; backend-only checks
do not qualify the workspace or native operating-system credential storage.
The operator guide is [NATS live subscriptions](website/docs/guide/core-nats.md).

`AccountedProviderEventQueue` owns pending and in-flight event costs. Provider
policies define record projection, generation changes, omission reporting and
control replacement; IPC and HTTP drivers own completion and finite deadlines.
An IPC lease completes only on its matching renderer acknowledgement. An HTTP
lease completes on the local socket write callback, which does not prove browser
or broker acknowledgement; socket drain separately grants write capacity. Kafka
IPC rejects overflow and preserves queued records. Each HTTP client may omit its
oldest pending records without pausing a healthy sibling client; retained bytes
and serialized event bytes have separate bounds.

Host-only `stopStream()` revokes pending starts and confirms the selected provider's
actual reader cleanup without disconnecting, deleting profiles or removing remote
capture resources. The final HTTP client leaving, or desktop event failure, closes
new-command admission until that cleanup settles. Failed cleanup requires restarting
the host; it cannot be reported as a successful stop. Whole-host shutdown joins
already-started stops and retains their failures independently of registry cleanup.

The renderer's `StreamSkopeProductApp` composes the theme and registered
workspaces. Shared UI owns the header, resource navigation presentation and
viewport layout; each feature supplies its actual destinations, availability,
content, activity and status. Kafka's `KafkaWorkspace` keeps its connection
plugins and interaction state. `StreamSkopeApp` remains a direct Kafka embedding
wrapper. Provider selection waits for confirmed stop and disconnect before
mounting another workspace. Every activation owns command admission: switching
blocks new calls, failed cleanup restores that activation, and a successful
switch permanently retires its callbacks. Already-admitted responses, including
write receipts, remain intact. Existing event listeners continue to observe
cleanup; inactive views cannot acquire new host listeners. A query import belongs
to the first Kafka visit, including development effect replay and failed cleanup.
Selecting a provider does not remove profiles or remote capture resources.

`src/plugins` owns plugin API version 4, independent SemVer, compatibility intervals
and bounded JSON/manifest validation. API 2 and API 3 remain supported for existing
installed and published packages.
`plugins/eda` owns EDA schemas, API/tunnel/session lifecycle and capture UI.
`plugins/nsp` owns NSP workflow definitions, API execution/cleanup, profile onboarding
and its UI. The NSP workflow source is
`plugins/nsp/resources/nsp-capture.workflow.yaml`; the backend embeds its exact bytes
during packaging. Each plugin's backend and renderer are standalone bundles.
UI contributions mount their own
React root through a generic interface; renderer assets are served from the
application origin without Node access or remote script loading. The backend
runs as trusted first-party code in the host process, not as a security sandbox
for arbitrary third-party extensions.

NSP uses the existing core profile test/save path. The helper definition is immutable
and adopted only after exact-content verification; owned executions have unique
request markers and must be removed before saving credentials. Non-secret recovery
identifiers are persisted through the optional `PluginBackendHost.recoveryState`
capability outside version directories. NSP requires that capability and rejects
older hosts that lack it; existing EDA API 2 packages remain compatible. Recovery
writes never contain credentials or workflow output. Removal retains the shared
helper and saved profiles, and refuses to complete when execution cleanup is uncertain.

Trust retrieval commands and identity review live in `useRemoteTrustAcquisition`;
`useTrustAcquisitionOperation` owns pending requests and candidate cleanup.
The application's `TrustAcquisitionLifecycle` owns editor leases, identity
challenges and cancellation deadlines. `KafkaProfileDraftResolver` resolves
trust and bindings; `KafkaProfileService` owns profile persistence and mutations.

The internal host protocol version is declared by `HOST_PROTOCOL_VERSION` in
[`src/features/kafka/contracts/types.ts`](src/features/kafka/contracts/types.ts).
EDA-specific commands/events now
belong to the plugin's own protocol and travel through generic `plugin.execute`
and `plugin.event` envelopes. `plugins.*` commands manage installation and exit.
Upgrade the host, preload and renderer together. Plugin API version 4 is separate
from this internal protocol; the host bridge supplies its current protocol number.
Changes to public plugin capabilities still require explicit compatibility review.

Old EDA profile metadata migrates to a generic plugin-owned source on read,
without requiring the plugin. Saving persists that new representation; older
desktop builds cannot read newly saved plugin metadata. Keep a protected profile
store backup before downgrading. Missing plugins block connect and connection
tests before a stale tunnel endpoint reaches Kafka, while preserving recovery data.

For local EDA, set `STREAMSKOPE_EDA_API_URL`, `STREAMSKOPE_EDA_API_USERNAME`,
and `STREAMSKOPE_EDA_API_PASSWORD`. Optional settings are `STREAMSKOPE_EDA_API_CA`,
`STREAMSKOPE_EDA_API_CLIENT_SECRET`, `STREAMSKOPE_EDA_CAPTURE_PRODUCER`, and
`STREAMSKOPE_EDA_CAPTURE_LOCAL_PORT` (default 19092).
`node --import tsx tools/check/eda-live.ts` skips without connection settings and fails for partial,
unreachable, or invalid configuration. It requires the matching capture app
and an exporting producer, consumes one real Kafka event, then removes its
own temporary session. Generate an event during the test if needed.
TLS verification stays enabled; credentials belong in ignored local configuration.
`dist/ci/eda-live.json` records passed, failed, or skipped. These tests run locally.

For local NSP qualification, set `STREAMSKOPE_NSP_CONFIG` to a private JSON file
containing `apiUrl`, `username`, `password` and `verifyCertificate`. Optional
`brokers` is an array of reachable `host:port` addresses; optional `authentication`
is `auto`, `tls` or `oauth`. Keep this file outside tracked source with restrictive
permissions; do not place credentials in command arguments. Certificate verification
should remain enabled except for an explicitly trusted development lab's API.
Kafka certificate verification remains enabled in either case.

```sh
STREAMSKOPE_NSP_CONFIG=/absolute/path/private-nsp.json node --import tsx tools/check/nsp-live.ts
```

The same harness runs at the end of local `npm run check`; GitHub `--ci` excludes
both live-cluster harnesses. An unset path records a skip, while invalid configuration
or an unsuccessful configured check fails. `dist/ci/nsp-live.json` records the
sanitized result. The harness exercises the generic plugin runtime and core profile
test/create/update/connect path, topic listing, hot removal and retained-profile
behavior. It creates or reuses the owned immutable helper and cleans up its execution;
it does not deploy a broker, produce messages or qualify every NSP version.

Host protocol 26 removes the obsolete `templates.*` catalog commands, `templates.changed`,
and password-only trust acquisition. SSH acquisition requires an explicit recipe.
The desktop preserves `templates/kafka-connection-templates.json` as a read-only migration
source for `recipes.legacy.preview` and `recipes.legacy.convert`; conversion writes only
`trust-acquisition-recipes.json`. Existing converted recipes and saved profiles retain their
storage schemas. Modern recipes initialize independently of missing or corrupt legacy data.
Older renderer/host pairs must be upgraded together. Command-specific response types
and `execute()` inference enforce the existing wire result shapes; this type refinement
did not increment protocol 26. Runtime validation still checks result shapes and
request identifiers at host boundaries.

## Temporary Forge security backport

`node-forge@1.4.0` has no published fix for
[GHSA-86w9-cpqp-85rv](https://github.com/advisories/GHSA-86w9-cpqp-85rv).
StreamSkope applies the narrow RSA validation change from
[upstream PR #1152](https://github.com/digitalbazaar/forge/pull/1152), pinned to
commit `ceba34402e329f0365134f23fe19898756527d65`. This is a local backport of an
unmerged upstream patch, not a new upstream version. Registry versions, lockfile
integrity, and the dependency's existing licenses remain unchanged.

The `dev`, `build`, `check`, and `package` entry points apply the backport before
using application dependencies. This also covers development dependency caches
and the fresh production install used by native packaging. Packaging verifies the
patched bytes again after extracting the final ASAR and records the hash in its
verification report. `tools/check/forge-patch.ts` pins the entire original and
patched RSA file hashes; unknown versions, changed files, missing copies, or
unlisted dependency resolutions fail verification. No install lifecycle script
or additional public npm command is needed.

Raw `npm audit` still reports the upstream advisory because the package retains
its real version. `tools/check/audit.ts` runs the registry audit and accepts this
specific advisory only after checking every installed affected copy against the
backport hash. Indirect findings are accepted only when all their underlying
advisories are accounted for. Other high/critical findings, malformed reports,
and audit-service failures still fail CI. Signature regression tests demonstrate
that stock Forge accepts the malformed signature and the patched copy rejects it;
existing JKS/PKCS12 tests continue to qualify truststore handling.

When a fixed upstream release is available, review and upgrade both direct and
transitive Forge dependencies, remove the temporary patch hooks and advisory
handling, restore the direct audit command, and retain the signature regression.

## Temporary build dependency mitigations

Two development dependencies currently have no published patched version:
[braces 3.0.3](https://github.com/advisories/GHSA-vfj7-8cjw-p6xm) and
[http-cache-semantics 4.2.0](https://github.com/advisories/GHSA-ch52-4w7c-c8xp).
The former receives a depth guard in its parser and recursive AST walkers. The
latter requires revalidation when cache safety reduced freshness to zero, even
when a client permits stale responses. Positive-lifetime public cache entries
retain normal stale-response behavior.

These are local mitigations, not upstream fixes. `check`, `build` and native
`package` apply the pinned changes before using these build tools. The helper
verifies locked package identity, development-only classification, full original
and patched file hashes and actual dependency resolution. Repeated application
is idempotent; changed or unlisted copies fail verification. Both packages remain
excluded from production dependencies. This does not alter the Forge backport
used by the desktop itself.

The complete npm audit remains mandatory. Only these exact advisory/version/path
combinations and indirect findings whose causes are all verified can be accounted
for; new advisories and unverified copies still fail. Regression tests reproduce
the stock stack overflow and unsafe cookie-cache reuse, then confirm the patched
behavior. Once fixed upstream versions are qualified, remove the corresponding
data, helper hooks and audit handling. Keep the regression tests.

## Temporary NATS runtime dependency corrections

The pinned public `@nats-io/transport-node` 3.4.0 SDK can leave its initial socket
open when connection setup fails before it marks the transport connected. Real
silent, delayed-INFO and stalled-TLS peers reproduce this through public `connect()`.
That would make StreamSkope's cancellation and shutdown guarantees untrue.
Its INFO-to-TLS upgrade also omits the selected host from Node's TLS options.
For an IP destination, Node can then verify the default `localhost` identity and
accept a trusted DNS-only certificate. An independent public TLS peer reproduces
that mismatch while confirming the matching DNS identity succeeds.

`tools/check/runtime-dependency-patch-data.ts` records a narrow exact-source
correction against the [published transport source](https://github.com/nats-io/nats.js/blob/v3.4.0/transport-node/src/node_transport.ts).
It retains the socket as soon as dialing begins and destroys it on early close,
preserving existing handshake error observation and TLS verification. Early
cleanup leaves the SDK's connected lifecycle notification untouched, so a failed
address or server does not retire a subsequent valid fallback. This is a
runtime lifecycle and TLS identity correction, separate from the build-only advisory mitigations.
It grants no npm-audit exemption.
The TLS upgrade passes the selected DNS name or IP as the verification host and
preserves the SDK's DNS SNI value; verification cannot be disabled by profiles.

`dev`, `check`, `build` and `package` apply and verify the allowlisted version,
registry integrity, consumer resolution and complete source hashes. Unknown or
altered copies fail closed. Qualification reproduces the stock failure, verifies
idempotent correction, confirms fallback and TLS identity against independent
peers, and exercises the public SDK in the bundled host. Remove
the correction and its hooks once a qualified upstream release fixes these cases;
retain the independent socket-lifecycle regression.

## GitHub builds

The **CI** workflow runs the required **CI** source/docs check on every PR using
the shared `check` command with `--ci`. It checks GitHub's temporary merge revision.
Merging to `main` does not repeat the suite. Release-note labels do not change these
checks or allow relaxed modes; the README badge reports PR runs.
Keep the repository's branch rules set to require a PR, the **CI** status and an
up-to-date branch before merging, with no bypass. These protections ensure changes
are qualified against the current base before they reach `main`.

The separate **Release** workflow is manual. Open **Actions → Release → Run workflow**,
select branch **main**, choose **desktop**, **eda** or **nsp**, and enter a bare
Semantic Version such as `0.2.0` or `0.2.0-rc.1`. The version belongs to the
selected component. Release calls the same **CI** workflow to qualify the exact
selected source again before stamping a
disposable build checkout. It does not commit a version bump to `main`. Tag pushes
do not trigger a release.

For desktop releases, native and cluster packaging follow qualification. The
native runners build, launch and package installers for Linux x64, macOS ARM64
and Windows x64. A Linux runner builds the complete unsigned EDA OCI application
with EDABuilder v26.8.2 and uploads it separately. Desktop installer packaging
belongs to the release runners; local Linux development uses `dev` and `build`.
Local EDA OCI packaging remains available through `npm run package -- eda`.
Before a local EDA build, packaging authenticates using the EDA API settings above
and reads `GET /core/about/version`, comparing `eda.version` with the declared
target. Production timestamp/Git build suffixes are recorded separately from
the release number. Missing settings, an unreachable or untrusted API, an invalid
version or a mismatch stops the build before Docker or EDABuilder runs.
`dist/ci/eda-version.json` records the observed release and full build version;
successful packages include a copy in `dist/eda-package/eda-version.json`.
GitHub builds use the declared target and record `not-checked` for the cluster;
they cannot verify your local EDA. Configured live qualification also checks
the cluster version before starting a capture.
EDA packaging needs Linux and Docker; native desktop packaging needs its matching OS.
Local soak and live EDA/NSP results should be reported in the PR, including skips.

The version entered in **Run workflow** becomes the desktop app and installer
version, or the selected plugin's manifest version. Desktop drafts use `vVERSION`;
plugin drafts use `plugins/NAME/vVERSION`. The tag identifies the qualified source
commit; release CI stamps its build checkout from the recorded workflow input.
Source archives at that tag retain the neutral development identity. To reproduce
a release, check out that tag in a disposable directory, install dependencies and
run `node --import tsx tools/package/release-version.ts COMPONENT VERSION --stamp`
using its recorded `desktop`, `eda` or `nsp` input before packaging. New release
versions reject build metadata and reserved development identities; CI run IDs and
Git commits provide traceability. Historical `+build.N` tags remain readable but
are not reused or created by the new process. Existing tags/releases are rejected.
The final draft step creates the tag atomically. If a run fails after that point,
inspect the existing tag/draft before retrying: an ordinary retry rejects the used
identity. Choose an unused version or explicitly resolve the unpublished failed
identity; the workflow never overwrites or deletes it automatically.

After qualification and native/unsigned EDA packaging pass, a desktop release
creates a **draft** with three unsigned installers, `SHA256SUMS` and reviewed
content from `website/docs/releases/unreleased.md`, plus a generated changelog.
CI assigns the title and exact
version metadata in the build checkout. A SemVer prerelease marks the draft as a
prerelease; an ordinary version does not. Signing is a separate property: these
desktop installers remain unsigned. Desktop releases do not republish plugins.
Review the draft, its source and qualification evidence. Attach the sanitized,
source-specific `qualification-vVERSION.json` report and add its digest to
`SHA256SUMS` before publication. Record each executed check, skipped/failed checks,
environment conditions and material limits; do not infer live passes from packaging.
Pages links this exact report only when it appears in the publication event's assets.
Then publish the reviewed draft on GitHub.
For stable desktop publication, select **Set as the latest release**. Publish
release candidates and independent plugin releases without changing the latest
desktop destination. Creating a draft does not make its downloads public.

Keep `package.json`, its lockfile and source plugin manifest versions at
`0.0.0-dev`. PRs change implementation, compatibility declarations and unversioned
notes; they neither choose final versions nor publish releases. Prepare reviewed
notes in `website/docs/releases/unreleased.md` with `unreleased: true`, without
`release_tag` or `release_version`. A future plugin host minimum is a compatibility
requirement, not an assigned desktop release number. Local development packages
use `0.0.0-dev.<numeric timestamp>` so rebuilt bytes receive distinct identities;
only development hosts load these packages. Release hosts reject them.

### Release notes

Use a Conventional Commit PR title, such as `fix(eda): recover capture after restart`.
Release CI collects merged PRs through the GitHub API and groups them into
Security, Breaking changes, Features, Fixes and Other changes. PR titles describe
the user-visible outcome; the draft still needs a maintainer's review. Titles and
labels do not select the next version.

Review component labels before merging:

| Label                | Include the PR in                             |
| -------------------- | --------------------------------------------- |
| `component:desktop`  | Desktop releases                              |
| `component:eda`      | EDA Connector plugin releases                 |
| `component:nsp`      | NSP Connector plugin releases                 |
| `component:shared`   | All three release families                    |
| `release-notes:skip` | None; deliberately omit it from the changelog |

Multiple component labels are allowed. Without them, these paths identify a
plugin owner:

- `plugins/eda/` and `plugins/nsp/` identify the corresponding plugin.
- Matching `eda-` or `nsp-` filenames under `test/unit/`, `test/integration/`,
  `test/architecture/` and `test/support/` identify that plugin, as do
  `website/docs/plugins/eda.md`, `website/docs/plugins/nsp.md` and the corresponding
  `website/docs/guide/eda/` or `website/docs/guide/nsp/` directory.
- `vendors/streamskope/apps/` belongs to EDA.

A PR touching only these paths belongs to every plugin represented by its files.
Any other path makes the unlabelled change shared across all three components.
Review the recorded inference, especially for documentation and tooling changes;
explicit labels override it.

The baseline is the nearest ancestral published release of the **same component**:
`vVERSION`, `plugins/eda/vVERSION` or `plugins/nsp/vVERSION`. Historical desktop
`+build.N` tags remain eligible. Stable releases use a stable baseline, so changes
already described in intervening release candidates are included again. Drafts,
unrelated component tags and releases outside the selected source history are
ineligible. With no eligible baseline, the notes explicitly cover the full history.
Collection stops at the exact selected source commit, even if `main` advances.
Commits without an associated merged PR appear separately and use the same
component selection rules; PR changes are not duplicated as commit entries.

For native GitHub stacks, the stack's target branch identifies whether a merged
PR belongs to `main`; an upper PR can retain its intermediate base branch after
merging. Each PR keeps its own title, component labels and omission label. The
collector still requires the same repository and a merge commit in the selected
mainline history. Stacks targeting another branch remain ineligible.

The prepare job freezes this selection in the `release-changelog` artifact before
qualification and packaging. The final draft combines it with reviewed highlights,
upgrade instructions and known limitations. Maintain desktop commentary in
`website/docs/releases/unreleased.md` and plugin commentary in
`plugins/eda/RELEASE_NOTES.md` or `plugins/nsp/RELEASE_NOTES.md`; keep these short
rather than duplicating every PR. Use absolute published documentation URLs because
these bodies also appear on GitHub. Package metadata and workflow evidence links
accompany the notes. Inspect the retained notes and JSON selection evidence before
publication; successful packaging does not establish live EDA/NSP qualification.

The final **published GitHub body** is the authoritative release record, including
any edits made while reviewing the draft. Copy that body unchanged into Zensical
through a normal documentation PR. For desktop, use
`website/docs/releases/vVERSION.md` with `title`, `release_version` and
`release_tag` front matter. Add `release_date` from its GitHub publication timestamp
and a short `release_summary` for the history overview; do not edit the archived body.
For a plugin, use
`website/docs/releases/plugins/NAME/vVERSION.md` with `title` front matter only;
desktop publication validators own the `release_*` fields. Desktop history and
navigation are generated from the archived pages. Keep its links usable in both GitHub and the docs
site. Pages includes the desktop publication event's body automatically in its
release snapshot. Keep the archival documentation PR for repository history and
subsequent snapshots; it does not deploy Pages or change an existing release snapshot.
The sidebar shows only the five newest published stable desktop versions and
**See all releases**; the overview retains older versions and prereleases. Keep
one `<!-- release-history -->` marker in the overview rather than a second version
list. The full internal tree preserves breadcrumbs for older pages. Stamped
release candidates remain previews until publication advances the baseline; public
builds reject any remaining pending notes. Unreleased commentary appears only in previews.

For a desktop publication, update the published download baseline. Keep desktop
release identity separate from source plugin guide scope and package availability. Reset only the commentary shipped in that component's release;
preserve notes for work merged after its source commit and other unreleased
components. Keep historical notes intact. The website's
`project.extra.desktop_release` identifies an actually published installer;
starting release CI does not change it. Release qualification can validate the
stamped notes while downloads still point to the last published desktop.

The **Pages** workflow deploys only the highest published **stable desktop SemVer**.
Prereleases remain downloadable from GitHub but do not replace the stable site. PRs, `main` pushes, tag creation, draft releases and plugin
publication do not deploy it; there is no manual Pages dispatch. They still run
the usual documentation qualification where applicable.

Before building, Pages reads all GitHub release pages and selects the highest
stable desktop SemVer, ignoring plugin tags, publication dates and the mutable
**Latest** designation. It requires an immutable release and checks that its tag
resolves to the event commit. An older event is skipped; API failure or an
unverifiable identity fails closed. Pages checks out that exact commit, even if
`main` has advanced, and repeats selection immediately before deployment.
Existing workflow runs retain their original workflow revision; these guards apply
to releases containing this policy.
It stamps a disposable checkout with that desktop version using the existing
release tool, then `npm run docs -- prepare` aligns the download baseline and
release index, copies the publication event's exact release body, and removes
that checkout's unreleased page. Main stays `0.0.0-dev`. Local and PR previews
label source guides as unreleased rather than claiming they apply to the last
published desktop; stamped release previews remain explicitly labeled previews.

Publication refuses a non-release event, prerelease, mutable release, wrong source
commit, mismatched version or incomplete notes. The build also verifies that the exact three installers and
`SHA256SUMS` exist in the published release, qualifies the site and then verifies
its public revision, key pages and bookmark redirects. Docs changes become public
with the next desktop release. To recover a failed deployment, rerun that release's
Pages run after inspecting its failure, provided it is still the latest stable desktop; never build newer main under an older
release label. Set Pages source to **GitHub Actions** and permit desktop release
tags (`v*`) in the `github-pages` environment's deployment rules.

Plugin pages use `plugin_scope: eda`, `nsp` or `all`, not a generic `unreleased` flag.
The source requirements come from the release checkout's plugin manifests.
During publication preparation, the production official catalog selects compatible
published packages for the documented desktop and verifies their manifest digests.
A source-bound, timestamped snapshot supplies the separate plugin availability
notice; builds reuse that snapshot. A catalog error fails preparation rather than
claiming that no package exists. Local/PR previews stay offline and say availability
was not checked. The catalog snapshot does not prove every source procedure shipped
in the selected plugin, nor does it qualify live behavior.

`npm run docs -- qualify` rejects ambiguous guide metadata and qualification
headings. Only the development release-notes page may carry generic `unreleased`
metadata; it is removed before publication. Source rehearsal evidence must name its
source and limits, without presenting itself as current-release qualification.
Installation links use the prepared `project.extra.desktop_release`; keep the
download marker in the installation page. The publication-only preparation
changes the build checkout, not the source tag or main.
**Launch video** remains a separate manual utility.

## Build a desktop package

1. Open a local checkout of StreamSkope on your target platform.
2. Use **Node 24.x** and install dependencies for that operating system and CPU:

   ```sh
   npm ci
   ```

3. Build on macOS ARM64, Windows x64 or Linux x64:

   ```sh
   npm run package
   ```

   The installer is written to `dist/installers/`: a DMG on macOS,
   an NSIS `Setup.exe` on Windows, or an AppImage on Linux.

4. Wait for package verification and the packaged launch test to finish.

**You should have:** the package at the printed output path. A successful build
checks the package; it does not establish signing, notarization or production support.

## Desktop plugin distribution

`npm run build` builds core and plugin bundles separately. `npm run package -- plugin`
creates both EDA and NSP artifacts under `dist/plugin-package/`. Each has a
`streamskope-NAME-vVERSION.skope-plugin` bundle and
`streamskope-NAME-vVERSION-plugin.json` manifest, where `NAME` is `eda` or `nsp`
and `VERSION` is the independent plugin SemVer described in
[Plugin versioning](website/docs/plugins/versioning.md). Local builds use
`0.0.0-dev.<numeric timestamp>`; release CI uses the entered version. For example,
a released NSP plugin could provide:

```text
streamskope-nsp-v0.1.0.skope-plugin
streamskope-nsp-portable-v0.1.0.skope-plugin
streamskope-nsp-v0.1.0-plugin.json
streamskope-nsp-v0.1.0-nsp-capture.workflow.yaml
```

Production plugin packaging requires `STREAMSKOPE_PLUGIN_SIGNING_KEY_B64`, a
base64-encoded Ed25519 PKCS8 PEM private key matching a public key in
`src/platform/node/plugins/publishers.ts`. Release CI supplies it only to the
selected plugin packaging step after qualification. Missing or mismatched keys
fail packaging. Local development builds remain unsigned and do not need this
secret. Keep the protected private-key backup outside Git and preserve it across
repository resets; GitHub cannot return an existing secret's value.

The signed portable envelope covers a domain-separated publisher identity and the
exact primary-package bytes. Release assembly verifies its signature, primary
payload equality and shared manifest before drafting a release. Do not publish
another manifest for the portable asset. The host bounds the portable envelope
at 48 MiB and its enclosed original package at 32 MiB, retains the original archive
and verifies it again before loading. Tests inject ephemeral public-only trust
registries at the host composition boundary; no renderer or environment input can
replace production publisher trust. Key rotation requires a desktop containing
the new public key before signing packages with it. Desktop plugin signing and EDA
cluster-app signing are separate trust domains.

The NSP bundle contains `nsp-capture.workflow.yaml` as a declared SHA256-verified
resource; the separate YAML download contains identical bytes. Maintain that
single source and its manifest digest together. Its deployed helper identity is
immutable: changing behavior or formatting requires a new helper identity and a
reviewed migration. The formatter excludes this YAML to preserve the existing
ownership fingerprint, and Git enforces LF line endings on every platform.
Packages accept only bounded, declared, flat data
resources; missing, extra, duplicate or altered resource files are rejected.
Use `npm run package -- plugin nsp` or `npm run package -- plugin eda` to select
one plugin. Every invocation clears the previous package output before writing
its selected artifacts. Packaging verifies the manifest identity, the exact EDA
target where applicable, and that the installed backend loads independently. The desktop installer excludes these
optional plugin bundles. The integration suite also exercises the actual packaged
renderer in Chromium. Browser installation coverage is available through
`node tools/package/e2e.mjs web test/e2e/web-plugin-installation.spec.ts`.

Release users install through **Preferences → Plugins**, using published assets from
`asadarafat/streamskope`. The installer verifies GitHub asset SHA256 digests and
the manifest/API contract and supported desktop interval, then writes immutable files
and atomically selects the active version. This verifies official repository
provenance and integrity; it does not
claim publisher cryptographic signing. Installed plugins work offline. Catalog refresh and downloads require GitHub;
native file inspection accepts only signed portable packages, and cached archives
are selected explicitly by ID, version and digest. Opaque review receipts pin
verified bytes and expire; cleanup consent is bound to the candidate and current
activation/work state. Cancellation discards a review without replacing code.
See [offline installation](website/docs/plugins/offline.md). Installation, updates and removal
apply immediately in the running application. Each activation has its own request
identity and renderer assets; callbacks from retired views cannot operate the new
instance. Changes serialize with shutdown and owned profile connections.

Active EDA work requires confirmation before cancellation and temporary-resource
cleanup. Cleanup failure leaves the current plugin available for retry. Successful
removal deletes its installation files while retaining saved profile metadata.
Failed backend or renderer activation restores the previous verified version when
available; a stopped capture must be resumed explicitly. Unrelated Kafka sessions
remain available. API 2 introduced these lifecycle hooks; API 3 added explicit
compatibility identities and resources. API 4 uses independent SemVer and an
inclusive minimum/exclusive maximum desktop interval. Host protocol 28 introduced
activation identities and versioned snapshots; the current paired host protocol is 52. Existing API 2 and API 3 packages
retain their legacy versions and asset names and remain loadable on the new host.
A compatible API 4 package supersedes either legacy generation; within API 4,
updates follow SemVer precedence. Earlier unreleased API 1 packages
must be rebuilt; profile metadata stays at version 1.

NSP follows the same hot lifecycle. It retains a reusable shared helper workflow
instead of deploying a broker or tunnel. Cancelling, updating or removing the
plugin must confirm owned execution cleanup; failures retain durable recovery
identifiers for a later attempt. The catalog selects the highest compatible version
for each plugin independently and verifies each manifest/package digest. The current
catalog examines the first 100 GitHub releases, with at most 16 manifest candidates
per plugin within that window.

To release a desktop plugin, open **Actions → Release → Run workflow** on
**main**, select **eda** or **nsp**, and enter its next independent SemVer. Do not
bump its source manifest or push a release tag manually. Keep its supported
desktop interval and target bounds explicit; changing plugin contents, workflow
bytes or compatibility declarations requires a new release version. Release CI
runs shared checks, stamps and packages only that plugin, then creates its tagged
draft. Review and publish it. Native desktop/EDA OCI packaging and Pages do not
run for a plugin release. A desktop release is not required for a plugin fix
supported by the current host API.

The development source implements plugin API **4**. EDA/NSP source manifests each
declare explicit host compatibility; these bounds do not assign the desktop's
next version. Publish a compatible supporting desktop before announcing API 4
plugins as usable. The original **v0.1.0+build.1** and its API **3** assets remain
published and immutable. Keep source-only changes marked unreleased until their
supporting desktop and plugin releases are available.

Desktop plugin packaging does not require cluster access. It validates the
manifest and includes the declared compatibility range, but does not prove that
range works. Qualify every supported target release before widening the inclusive
numeric bounds; do not infer support from a wildcard, an API version or a newer
product's release date. The EDA plugin reads `/core/about/version` before discovery,
cluster application checks/installation and capture deployment. The NSP plugin
reads `/sdn/api/v4/system/version` before creating or executing its helper workflow.
Both reject unknown or out-of-range product versions before those operations.
The separate EDA cluster application build and configured live checks retain their
API-based target validation. Owned NSP execution cleanup remains available for
recovery after a target upgrade; cleanup does not create a new execution.

Browser development stores plugin installations under `.cache/development-plugins`;
desktop installations live under the application's user-data `plugins` directory.
Development uses the same immediate installation lifecycle with current-API
development packages only. Source changes alone do not replace an installed
artifact; repackage to obtain a new development identity, then install it.
Released packages require a matching release desktop for qualification.

For source plugin QA, run `npm run package -- plugin` to build both plugins, then
`npm run dev`. Open **Preferences → Plugins → Check for updates** and choose **Install**
or **Update to** the new development version. The browser development host reads
`dist/plugin-package/` again on refresh; startup neither builds plugins nor downloads
published packages. Rebuild after source changes, then refresh and update normally.
Preserve `.cache/development-plugins` and its capture/recovery state. If it contains
an old release-versioned package, use the previous compatible build to complete
owned-work cleanup and remove it explicitly before installing the development
package; do not reset the cache to bypass cleanup.

The **Publish EDA application** workflow is manual from main. Configure the
`eda-production` environment with `EDA_APP_SIGNING_KEY_B64` and
`EDA_APP_SIGNING_KEY_PASSWORD`, matching the public key shipped with the app.
Its GitHub token needs repository and GHCR write access. It builds, signs and
verifies the image, then publishes a generated versioned catalog branch and tags.
Existing image/catalog versions are rejected. If publication stops halfway,
inspect the registry and catalog before recovering; the workflow does not overwrite them.
The unsigned OCI artifact from local/manual desktop-release builds is for development, while this workflow
produces the signed EDA Store application.

## Versions and clean public history

Final versions are supplied to release CI. A desktop release stamps the app and
installer from the same input; an EDA/NSP plugin release stamps only that plugin.
Source versions remain **0.0.0-dev**. For example, an EDA-only fix could release
**0.1.1** while NSP stays at **0.1.0** and the desktop at **0.2.0**; these illustrate
independence, not current publication or reserved future versions.

A released API 4 manifest separates identity from requirements (example):

```json
{
  "version": "0.1.0",
  "apiVersion": 4,
  "compatibility": {
    "streamskope": { "minimum": "0.2.0", "maximumExclusive": "0.3.0" },
    "target": { "system": "eda", "minimum": "26.8.2", "maximum": "26.8.2" }
  }
}
```

There is no API 4 `revision` or duplicated desktop release field. Use SemVer
patch/minor/major changes according to compatibility; below **1.0.0**, put breaking
changes in a new minor version and compatible fixes in a patch. Removing a supported
host or target release is a breaking change. A qualified wider target interval can
ship as a minor release. Keep `-rc.N` prereleases distinct; do not create new
`+build.N` release identities. Immutable plugin identity is its ID plus version,
including compatibility and resource declarations: never reuse it with new bytes.

Preview hosts require an explicit prerelease minimum with the same core version,
such as `0.2.0-rc.1` for host `0.2.0-rc.2`. A stable interval ending before `0.3.0`
does not implicitly support `0.3.0-rc.1`. Stable desktop catalogs exclude prerelease
plugins; preview desktops can select them when explicitly compatible. Update the
manifest for a qualified RC before packaging it.

The current source targets EDA **26.8.2–26.8.2** and NSP **26.4.0–26.4.0**. Host
bounds are inclusive/exclusive; target bounds remain inclusive/inclusive and exact
numeric versions. Record live qualification before claiming observed support.
Read [Plugin versioning](website/docs/plugins/versioning.md) for selection,
legacy-generation migration and component responsibilities. Installed API 2/3
packages and original release names remain unchanged; do not rewrite their version
strings or compare their numeric values directly with API 4 SemVer.

The **EDA cluster application is a separate artifact** and retains the exact full
target EDA version: **EDA 26.8.2 → app v26.8.2**. `EDA_TARGET_VERSION` in
`plugins/eda/contracts/eda-capture-types.ts` is authoritative; the cluster manifest,
bundled agent and both EDA desktop plugin target bounds must match it. App fixes do not
increment the cluster application version independently or add suffixes. Existing
published versions remain protected from overwrites; inspect partial publications
before recovery. EDABuilder v26.8.2 and EDA Core API v6.0.0 are separate tool/API
versions. Regenerate signed catalog output through EDABuilder.

To start a new GitHub repository from the current source without old local
commits, export into a new directory. Deleting and recreating the same GitHub
repository does not free previously published immutable tag names; use a new,
unused tag there.

```bash
npm run check
node tools/maintenance/export-source.mjs /absolute/path/to/streamskope-public
cd /absolute/path/to/streamskope-public
npm ci
npm run check
# Review the source before creating the first public commit and tag.
git add .
git diff --cached --stat
git commit -m "chore: initialize StreamSkope source"
# Select an unused release tag before publication.
```

The export includes current uncommitted public source and starts `main` with no
commits, tags or remotes. It excludes ignored files and historical generated
`apps/` projections, and rejects symlinks/existing destinations. Review for secrets
before publication. Add the replacement remote and push only after arranging the
GitHub repository replacement. Exporting leaves development history intact.
