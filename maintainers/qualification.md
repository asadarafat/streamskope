# Qualification and release evidence

Run focused local checks for the behavior changed, then require all three GitHub
lanes and the final **CI** gate on an up-to-date PR. A duplicate full local run is
not mandatory for each PR. Additional real-provider, security, native and soak
checks follow the [impact matrix](#choose-evidence-by-impact); the PR gate does
not establish behavior outside its executed scope.

`npm run check` runs core shared checks, the 60-second application-pipeline soak
and docs. Vendor credentials do not implicitly select live tests. Use `--full`
to include configured EDA/NSP checks or `--live eda|nsp|all` to require selected
vendors. Each local run records a separate source-bound qualification bundle
under `.artifacts/qualification/` and prints its location. Keep the complete
bundle; a console transcript alone does not establish release acceptance.

Use `npm run check -- --ci` with Docker and Chromium to reproduce the GitHub
shared, docs and real-provider/browser lanes. GitHub runs those lanes in parallel
and retains their reports and aggregate index in the `qualification` artifact.
The existing five npm commands remain the entry points.

## Check matrix

Choose the smallest maintained scope that verifies the behavior changed.
Use the [development guide](development.md) for toolchain and fixture prerequisites
and [documentation guide](documentation.md) for site-specific checks.

| Run                                    | Included                                                                       | Important limits                                                                                             |
| -------------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| Focused owner tests                    | The selected contract, service or UI behavior.                                 | Development feedback; not a substitute for the required PR gate.                                             |
| `npm run check`                        | Core: shared checks, 60-second soak and docs, in that order.                   | Stops at the first failure. No vendor live tests or disposable runtime lane.                                 |
| `npm run check -- --full`              | Core, then configured EDA and NSP checks.                                      | Unconfigured vendors are skipped; does not include the disposable runtime lane.                              |
| `npm run check -- --live eda`          | EDA only; use `--live nsp` for NSP or `--live all` for both.                   | Every selected vendor requires configuration. No core or runtime checks are implied.                         |
| `npm run check -- --ci`                | Shared, docs and runtime lanes sequentially.                                   | Does not include the local soak or live EDA/NSP harnesses.                                                   |
| `npm run check -- --ci --lane runtime` | One selected lane; `shared` and `docs` are also valid.                         | Qualifying one lane does not qualify the others.                                                             |
| GitHub PR and reused release CI        | The three lanes in parallel, then the required **CI** evidence gate.           | All lanes must succeed with matching source evidence. Main pushes do not repeat CI.                          |
| Accepted routine Dependabot tooling    | The same three GitHub lanes and protected merge, with automated policy review. | Only the explicit manifest/lockfile allowlist permits automation. No live, soak or native result is implied. |
| Native recovery or packaging rehearsal | The explicitly selected installed app, OS/CPU or container image.              | Scope is limited to the actual source, platform, target and scenario that ran.                               |

[tools/check.sh](../tools/check.sh) owns the maintained stages. Shared checks
include workflows, formatting, lint, all four TypeScript projects, architecture,
unit/integration tests, EDA source/Go agent checks and dependency checks. The
runtime lane exercises real Kafka and NATS plus selected production-browser,
workbench and plugin-lifecycle scenarios; it does not run every system test.

The soak drives the application pipeline at 1,000 records/s with mixed payloads
and clone round trips. It is a synthetic pipeline measurement, not broker
throughput, network endurance or rendered UI interaction latency. Choose an
additional real-system test for those claims. For example, developer CLI/sandbox
and relationship scenarios have their own tests under `test/kafka/`; a shared
unit-test pass does not establish their live behavior.

## Evidence locations

| Output                                                                          | Use                                                                                |
| ------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `.artifacts/qualification/RUN_ID/qualification.json` and its evidence directory | Complete per-run local receipt and sanitized copied evidence.                      |
| `.artifacts/ci/` and GitHub's `qualification` artifact                          | Lane reports and aggregate source/run/hash index.                                  |
| `dist/performance/qualification-soak.json`                                      | Synthetic pipeline measurements and declared limits.                               |
| `.artifacts/website/`                                                           | Docs qualification, browser, search and screenshot evidence.                       |
| `dist/ci/eda-live.json`, `dist/ci/nsp-live.json`                                | Local harness outcomes and completed checks; inspect as private diagnostic output. |
| `test-results/web/`                                                             | Separate browser-suite reports and traces.                                         |
| `dist/native-recovery/PLATFORM-ARCH.json`                                       | Sanitized installed-recovery outcome, installer identity and cleanup evidence.     |

Raw harness files, browser traces and app-data directories are not automatically
safe publication assets. Use the collector's approved projection for sharing.
Focused tests can write a separate report path so they do not overwrite an
ongoing qualification's inputs. Keep source and receipt files unchanged while a
maintained run is active.

## Read a local result

The receipt records the source revision and Git tree, whether the checkout was
dirty, source agreement before and after execution, platform/architecture and
Node version. Each stage has an explicit outcome and hashes of its copied
evidence. Schema-2 receipts declare the selected scope: unselected stages are
`not-run` with reason `not-selected`, rather than successful or skipped checks.
A failed stage leaves later selected stages unexecuted. `--full` records
unconfigured live systems as `skipped` with reason `not-configured`; explicit
`--live` selection fails without the required configuration. An overall pass
qualifies only the selected, executed scope. Historical schema-1 receipts remain
supported with their original five-stage scope and meaning.

For release acceptance, run from a clean committed checkout and leave its source
unchanged until qualification finishes. A dirty run is useful development
feedback, but cannot establish release acceptance. Git index flags that hide
changes (`assume-unchanged` or `skip-worktree`) are rejected. A normal interruption
records failure and leaves later stages unexecuted; host loss or forced termination
can leave an incomplete bundle, which is rejected. Do not edit receipts, copy old
results into a new run, or relabel the execution revision after a squash merge.

If a scope fails, correct its environment or source and rerun that scope into a
new bundle. On unchanged source, an NSP failure can be retried with
`npm run check -- --live nsp` without repeating core checks. Keep the failed
receipt and the retry; neither changes the other's outcome. A PR may report
the actual checks across these receipts, with their source identities and limits.
This does not synthesize a passing full receipt or authorize release attachment
of a collection of partial bundles.

Private connection settings stay outside the bundle. The collector retains safe
target-version and certificate-verification facts, check outcomes and declared
measurement limits. It excludes connection credentials, endpoint addresses,
broker payloads and raw exception text from the shareable report.

## Choose evidence by impact

Select local checks from the affected production path, not merely the changed
filename. Shared authentication or cleanup changes can affect both plugins even
when no plugin-owned file changes. Missing required live access is an explicit
qualification gap; unrelated changes do not need EDA/NSP access.

| Change or claim                                                         | Additional evidence beyond the required PR gate                                                                                                                            |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Documentation or behavior-neutral tooling                               | Owning docs, configuration or tooling validator; no vendor live check or soak unless its behavior is affected.                                                             |
| Core UI, contracts or application behavior                              | Focused owning regressions; a real-provider or browser scenario when mocks cannot establish the changed boundary.                                                          |
| Record pipeline, buffering, scheduling or performance                   | Owning regressions and the unchanged 60-second soak; add real-provider/rendered endurance for those claims.                                                                |
| Authentication, trust, protected storage or security boundaries         | Failure-path and secret-leak checks plus the real protocol/native rehearsal for the affected boundary; run EDA/NSP live checks if their path is affected.                  |
| EDA/NSP plugin, generated profile, connection, lock or cleanup behavior | The affected vendor's configured live check, known record receipt and confirmed owned-resource cleanup; shared paths can require both.                                     |
| Native packaging, browser storage or installed recovery                 | The affected OS/architecture's package or install/upgrade/rollback rehearsal, including existing storage and cleanup obligations.                                          |
| Milestone or release integration claims                                 | Core acceptance and the claimed vendor/real-provider/native scenarios against the selected source and targets. Compatibility expansion needs evidence for each new target. |

## Review release acceptance

Release CI assembles the qualification report from its own source index, package
outputs and native browser-installer receipts before creating the draft. It
verifies source/run identity and evidence hashes, and records local checks as
unrecorded until a compatible local bundle is supplied. Publication never turns
an unexecuted check into a pass.

| Change or claim                                        | Evidence required                                                                                                                              |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Any source change                                      | Successful required PR CI for the current base, focused local checks and applicable impact-based evidence; report unexecuted scope explicitly  |
| Desktop distribution                                   | Exact release source, three native package/launch jobs and the downloaded installer hashes                                                     |
| Browser distribution                                   | Native AMD64/ARM64 archive and installer lifecycle jobs, image identity, read-only data inspection, encrypted persistence and graceful restart |
| Local pipeline performance                             | The unchanged 60-second pipeline soak; this does not measure broker throughput or rendered UI endurance                                        |
| EDA or NSP connection/lifecycle behavior               | Configured live checks for the affected target version, known record receipt and confirmed owned-resource cleanup                              |
| Native protected-storage or installed upgrade behavior | An explicit installed/native rehearsal; source-host live checks do not establish it                                                            |
| Long-running broker or UI behavior                     | A separate real-provider/rendered-product endurance run; a one-minute pipeline run does not establish it                                       |

The report separates these scopes. Review gaps against the changed behavior before
publishing. A passing source gate alone is insufficient evidence for a native,
live-system or recovery claim. Existing historical reports retain their original
scope and identity.

The [routine dependency policy](dependencies.md#routine-tooling-acceptance) governs
automatic review/merge eligibility, not a different CI gate or release evidence.
Release qualification uses the exact selected source and its own checks. Run the
local soak and affected live/native checks when the change or claim requires them;
unexecuted scope remains explicit. Automatic dependency merging never creates a
local qualification receipt.

## Production connection profile matrix

The runtime lane includes `test/kafka/production-connection-profiles-real.test.ts`
and retains `.artifacts/ci/connection-profiles-real.json` in its source-bound report.
It starts an isolated, checksum-pinned Kafka 4.3.1 JVM broker on loopback. Java 17+
(including `keytool`) and OpenSSL are required; it does not change a running AIO,
EDA or NSP fixture. Owned process, temporary credentials and broker logs are removed
on completion or handled failure.

The matrix exercises PLAIN and SCRAM-SHA-256/512 over verified TLS, mutual TLS and
explicit plaintext; mutual TLS without SASL; and OAuth with or without a broker
client certificate. Profiles
connect, reconnect and restore through the actual passphrase vault and file store
after locking/unlocking and recreating the host. Separate HTTPS protocol fixtures
exercise Registry/Connect Basic, bearer and OAuth-client authentication, distinct
CAs, service client identities and safe failures. Their error bodies deliberately
contain generated test secrets to catch unintended diagnostic reflection.

A passing matrix establishes those protocol and protected-storage paths for the
executed source. The HTTPS endpoints are controlled protocol fixtures, not full
Registry/Connect vendor deployments. The existing vendor fixtures qualify their
own feature scope. Vault restart does not establish native OS keychain migration,
installed-package upgrade or managed-service certification.

## Structured record consistency

The existing runtime report group also includes
`test/kafka/topic-administration-real.test.ts`: an isolated pinned Kafka 4.3.1
broker establishes preview without writes, actual partition expansion and UUID
deletion, host read-only refusal, exact confirmation, duplicate-apply coalescing,
stale partition/replaced UUID refusal and real permission denial. Independent
fault/ownership regressions retain admitted acknowledgements across revocation,
unknown replies, failed readback and unresolved cleanup. The production-built
browser workbench suite includes `web-topic-administration.spec.ts` for keyboard
entry, review/confirmation, visible receipts, real inventory and dialog accessibility.
These cases do not certify managed services, older unsupported topic APIs or
atomic exclusion of concurrent name-based partition expansion.

The runtime lane includes `test/kafka/structured-records-real.test.ts` and retains
`.artifacts/ci/structured-records-real.json`. It uses an isolated real Kafka broker,
a controlled read-only Schema Registry protocol fixture, independently encoded
JSON/Avro/Protobuf records and the actual codec worker. It checks mixed writer
identities, host serialization, immutable original bytes, duplicate headers,
malformed records, missing schemas, tombstones, empty values, bounded search,
tracing, schema-3 export and selective masking before matching.

The same runtime command and report also include
`test/kafka/resumable-search-real.test.ts`. Its owned broker proves continuation
past 10,000 records across three partitions, exact cumulative coverage, fixed
snapshot ends despite new arrivals, and cancellation followed by continuation
from delivered records without missing or duplicate positions. Separate cases
change retention, replace a topic with the same name, and add a partition to
require rejection. Protected JSON/Avro/Protobuf pages include malformed records,
tombstones and duplicate headers, with unavailable counts and masking preserved.
Fresh-read and reconnect cases check token revocation. This is host-session
continuation evidence; it does not qualify durable restart recovery or a complete
topic export. The controlled Registry fixture retains the vendor-scope limitation
below.

That protocol fixture does not qualify a Registry vendor deployment. The separate
`test/kafka/record-codec-real.test.ts` uses the owned AIO Kafka and real Registry,
including referenced schemas. The required runtime browser structured-events suite
owns a separate native Kafka broker and controlled Registry protocol endpoint; it
does not require a pre-existing AIO deployment. It covers saved codec selection,
profile connection, record inspection, export, comparison, tracing and schema sample
generation and retains
`test-results/web/structured-records/playwright-results.json`.
A development-host restart only establishes its declared storage mode; durable
codec migration and rollback need the actual native browser/desktop storage
rehearsal for the affected platform. Keep those receipts separate from protocol
fixture and source-level unit results.

## Live EDA

Use a test cluster with the matching capture app already installed and a producer
that exports real events. The harness verifies the running EDA version through
its API against the declared target. It creates temporary capture resources and
a local tunnel, consumes an actual event, checks health/relationship metadata,
and confirms the selected producer remains unchanged. It closes the consumer and
tunnel, removes its owned session, confirms absence and tests repeat removal.
This is not a read-only cluster check.

Supply `STREAMSKOPE_EDA_API_URL`, `STREAMSKOPE_EDA_API_USERNAME` and
`STREAMSKOPE_EDA_API_PASSWORD` together through private local configuration.
Optional settings are `STREAMSKOPE_EDA_API_CA` (CA file),
`STREAMSKOPE_EDA_API_CLIENT_SECRET`, `STREAMSKOPE_EDA_CAPTURE_PRODUCER` and
`STREAMSKOPE_EDA_CAPTURE_LOCAL_PORT` (default `19092`). API certificate
verification stays enabled. Keep credentials out of tracked files and command
arguments.

```sh
npm run check -- --live eda
```

Explicit live selection requires configuration; missing, partial, unreachable or
invalid settings fail. Only `--full` permits an unconfigured vendor skip.
Generate an event in the selected producer during the test if necessary.
Inspect `dist/ci/eda-live.json` for completed checks. A pass establishes this
source-level capture path and cleanup on the tested target, not installed native
storage, all plugin UI paths or every EDA version.

## Live NSP

The NSP harness **creates one uniquely owned temporary Kafka topic, produces one
generated marker, reads it through the host, and confirms topic deletion**. Use a
test system and an account authorized for those topic operations and the helper
workflow lifecycle. Business topics are inspected as metadata only; their
payloads are not read or written. The harness does not deploy a broker.

Set `STREAMSKOPE_NSP_CONFIG` to a private JSON file containing `apiUrl`, `username`,
`password` and `verifyCertificate`. Optional `brokers` is an array of reachable
`host:port` addresses, and `authentication` is `auto`, `tls` or `oauth`. Keep the
file outside tracked source with restrictive permissions. API verification should
stay enabled; an explicit trusted-development-lab exception does not disable
Kafka certificate verification.

```sh
STREAMSKOPE_NSP_CONFIG=/absolute/path/private-nsp.json npm run check -- --live nsp
```

Explicit selection fails if the path is unset, configuration is invalid or a
configured check fails. `--full` runs the same harness after core and EDA, with a
skip permitted only when NSP is unconfigured. Default core and GitHub `--ci`
exclude both live-cluster harnesses.

The test uses the production package loader with an isolated catalog and
in-memory profile store. It exercises profile test/create/update/connect, helper
reuse and execution cleanup, hot update/removal/reinstallation, and recovery
after interrupting a real accepted workflow execution. The hot-update candidate
uses the same built code with a newer qualification-only manifest; this is not
proof of an upgrade between two published plugin implementations.

The immutable reusable helper definition remains in NSP. Owned execution cleanup
and owned topic deletion are separate obligations. Pending execution cleanup
blocks plugin removal; a recovery journal is retained on failure. Inspect the
reported retained state and reconcile it through the supported plugin flow;
deleting the journal is not cleanup. Review `dist/ci/nsp-live.json` and the final
receipt before claiming success. In-memory source-host checks do not establish
native credential persistence, installed-app behavior or other NSP versions.

## Native recovery

Use the manual [Native recovery workflow](../.github/workflows/native-recovery.yml)
for installer replacement on Linux x64, macOS ARM64 and Windows x64. Select a
published baseline and either current source or another published release. It
creates isolated application data, saves a protected profile and query, replaces
the app and restores the complete old backup. Reconnection and a filtered
known-record export must pass without re-entering credentials.

For a local native rehearsal, replace the version placeholders:

```sh
node --import tsx tools/check/native-recovery.ts --from BASELINE_VERSION --to TARGET_VERSION
```

For an unreleased candidate on macOS ARM64:

```sh
node --import tsx tools/check/native-recovery.ts --from BASELINE_VERSION --candidate dist/installers/StreamSkope-0.0.0-dev-darwin-arm64.dmg --candidate-version 0.0.0-dev
```

Use the matching installer filename on other supported platforms. Candidate mode
requires a clean committed checkout, builds the candidate and retains its neutral
development version; it does not publish a release. Published installers are
verified against `SHA256SUMS`. The report records exact installer hashes and the
candidate source revision where applicable.

Candidate mode backs up the baseline after quitting it and requires the candidate
to reconnect after replacement, another restart and backup restoration. It omits
baseline restart; published-to-published mode also requires that baseline restart.
Candidate success therefore does not retroactively qualify the old build. Keep
historical outcomes in the [operator qualification record](../website/docs/guide/qualification.md)
rather than treating them as current-source results.

The rehearsal needs Java 17+ and an unlocked OS credential service. Without all
three explicit fixture variables (`STREAMSKOPE_TEST_KAFKA_ENDPOINT`,
`STREAMSKOPE_TEST_OAUTH_ENDPOINT`, `STREAMSKOPE_TEST_CA_PATH`), supply none and let
it start its checksum-pinned loopback JVM Kafka fixture with TLS/RS256 OAuth.
Linux additionally needs Xvfb, D-Bus and GNOME Keyring. Windows runs only on a
disposable GitHub Actions account because installer registration affects the
user account. macOS copies the DMG app into an isolated directory; Linux launches
the extracted AppImage, which does not qualify FUSE or desktop launcher integration.

Only sanitized reports are uploaded; owned app data, credentials, traces and
broker logs are removed. Confirm cleanup outcomes. This separate rehearsal does
not establish cross-account credential portability, live EDA/NSP behavior or
long-running renderer endurance.

## Browser data compatibility

Browser images contain a standalone read-only data inspector. Native archive and
installer qualification run it after a confirmed graceful stop, using the exact
image ID with a read-only root and data mount, no network or capabilities, and the
application's non-root UID/GID. Qualification compares the complete disposable
data inventory before and after inspection, then restarts and unlocks the host to
verify the saved profile. The receipt binds inspection to the source, image and
architecture that actually ran.

The inspector checks supported outer storage formats and retained plugin package
integrity without opening the vault or executing plugins. It reports encrypted
content authenticity, protected profile schemas, remote resource cleanup and host
quiescence as unverified. The native lifecycle checks establish their own stop and
unlock results; a standalone successful inspection does not establish those facts.
Unsafe paths, unknown formats, pending plugin changes, nonempty recovery journals
and saved plugin-managed profile sources block inspection. Preserve those records
and resolve ownership through the supported application or recovery procedure;
deleting profile metadata is not proof of cleanup.

Current browser manifests declare the data contract and inspector in schema 4.
Historical manifests remain readable for pinned resume; they do not acquire this
capability retrospectively. New release assembly validates the current inspector
and exact installer bytes. An older schema 3 installer requires qualification from
its own source, rather than reconstruction using today's template.

Local staged-image receipts are separate from public-registry delivery evidence
and cannot satisfy release acceptance. The installer uses this inspection as one
part of its [owned maintenance transaction](../website/docs/guide/browser-host.md#upgrade-an-installer-managed-host),
alongside confirmed graceful shutdown, the held original vault lease, a complete
private backup and locked target readiness.

Native installer qualification separately exercises an actual transition from
the exact reviewed 0.10.3 image: initialize a vault and saved profile, check and
upgrade while unlocked, unlock the target and verify persistence/native workers,
roll back, unlock the predecessor and verify a pinned rerun. Both transitions
must have complete backup integrity evidence. Public receipts require the exact
public target image and anonymous registry delivery; a source build or sealed
local rehearsal does not establish that scope. A local rehearsal may use a real
locally staged target without publishing an image, but its receipt remains local.

The first reviewed legacy predecessor is 0.10.3; these checks do not establish
automatic migration from every historical release. Readiness proves the locked
host started, while the authenticated native fixture establishes its own saved
profile and unlock result. Neither establishes remote plugin-resource cleanup.

## Attach local acceptance to a draft

After Release CI creates the draft, use the existing package entry point with
one passing local bundle path:

```sh
npm run package -- qualification vVERSION --local .artifacts/qualification/RUN_ID
```

For a plugin, use its release tag, such as `plugins/eda/vVERSION`. The command
requires a draft with matching source and verified payloads. A clean PR commit can
qualify a squash-merged release only when Git independently verifies identical
trees; the report retains both commit identities and the equivalence decision.
Different trees require a new run. The single bundle must contain passed shared,
soak and docs stages. A passing core bundle is eligible for a desktop draft;
attaching evidence to a plugin draft also requires that plugin's live stage to
have passed in the same bundle, so use `--full` with that vendor configured.
Standalone live receipts and multiple partial bundles cannot be merged by the
attachment command. A desktop report retains live `not-run` or skipped outcomes
without converting them into passes. Review those gaps against the release's
actual integration claims.

The command updates only the qualification report and its checksums. It refuses
published/immutable releases and preserves the packaged payloads. If an upload is
interrupted, inspect the reported recovery information and the draft's two
metadata assets before publication; updating two GitHub assets is not an atomic
operation. Originals are retained under the reported
`.artifacts/qualification-drafts/update-*/original/` directory. If only one upload
succeeded, confirm the release is still a draft and restore those two original
metadata files before retrying. Do not overwrite concurrently changed packages or
restore metadata to a published release.

Review the report, `SHA256SUMS`, source and remaining gaps, then publish the draft
through the normal release process. The published documentation links the exact
qualification asset delivered with that release.

## Schema record authoring

The existing structured-record runtime report also includes
`test/kafka/schema-authoring-real.test.ts`. It starts the pinned Karapace 6.1.0
server against an isolated Kafka broker, registers exact Avro and Protobuf
reference graphs and a JSON Schema writer, then validates edited values using
the production isolated worker. It checks that validation and sample generation
leave the destination empty, publishes a reviewed finite batch, compares original
broker bytes and decodes them using separately compiled libraries. A deleted and
recreated subject/version must reject the former writer ID. This is vendor
reference-registration evidence for Avro/Protobuf; JSON reference validation is
covered independently at worker level, not claimed as vendor registration.

`test/e2e/web-schema-authoring.spec.ts` uses the built browser renderer, encrypted
vault gateway, isolated real Kafka and a controlled read-only Registry endpoint.
It edits a generated starting value, validates it, invalidates a destination
review by editing, and explicitly confirms a fresh review before publication.
An independent broker reader checks the resulting writer framing and exact int64
value. Its browser scope and controlled Registry limitation remain distinct from
the real Registry case. Both cases run in the existing runtime lane and reports;
no new CI lane or npm command is introduced.

Guided evolution is qualified separately by `test/kafka/schema-evolution-real.test.ts`
and `test/e2e/web-schema-evolution.spec.ts`. Both use the real isolated Registry:
stale policy blocks registration, each review admits at most one attempt, malformed
new subjects are rejected, transitive checks use pinned visible history, and the
browser reads back the new writer before authoring and independently decoding its
published record. Controlled faults cover denied writes, connection revocation,
lost acknowledgements and unavailable/mismatched readback; the anonymous Registry
fixture does not establish vendor RBAC or managed-service certification.

Reviewed subject compatibility changes are qualified by
`test/kafka/schema-policy-real.test.ts` and `test/e2e/web-schema-policy.spec.ts`,
using the same owned Registry. They exercise review without mutation, exact
confirmation, stale global policy and writer refusal, retained repeat receipts,
explicit no-op, subject override, inheritance and independent HTTP readback.
Policy and registration share reviewed write admission; unit regressions qualify
its serialization, bounded reads, expired authority, denied/uncertain writes and
acknowledgements preserved after connection/invalidation/readback loss. The browser
checks the policy table and confirmation, refresh without mutation, and restoration
of inherited policy. The anonymous Registry does not certify vendor RBAC or
managed-service permissions. Global changes and advanced Registry rules are outside
policy-write support. These cases join existing runtime report groups and add no
qualification lane.

## Structured replay and writer translation

The existing structured runtime group includes `record-replay-real.test.ts`.
Its translation case uses an isolated real Kafka broker and two independent real
Registries. It deliberately collides numeric IDs, registers missing destination
writers through the reviewed schema-change contract with fresh readback, checks
that replay preview writes neither schema nor record, independently decodes
Avro/Protobuf output with referenced destination definitions, and checks JSON,
tombstones, duplicate headers and timestamps. A receipt-persistence fault follows
an actual acknowledged send; a fresh host continues only the frozen unsent suffix
with the source Registry offline. A deleted destination writer stops before send.
The production browser recovery case exercises structured JSON editing and
protected history through the actual built renderer and unlocked browser host.

Journal format3 requires actual installed format2-to3 upgrade, pure legacy list,
explicit migration, encrypted predecessor preservation, uncertain-history restart,
old-host refusal before downtime, complete backup restoration and actual old-host
rollback. `browser-repair-recovery-smoke.ts` binds the retained qualified format2
image and source; `browser-upgrade-smoke.ts` reports the explicit format pair.
Keep these local staged Linux ARM64 receipts separate from public delivery,
desktop keychain migration and real-broker publication evidence. Earlier format1
to2 receipts retain their original source and scope.
