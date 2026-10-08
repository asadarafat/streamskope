# Qualification evidence

Use the exact desktop release and plugin identity when deciding whether a
procedure has been exercised for your environment. A passing documentation build
checks the site; it does not establish live integration, permissions or recovery.

## Find evidence for your version

Open the exact desktop version in [release history](../releases/index.md). New
release notes include a **Build evidence** link to the executed release workflow
and its source commit. That run records shared CI, each native installer and the
unsigned EDA application build. Use its job results and the release's `SHA256SUMS`
to assess the downloaded packages. Releases with browser assets also require
native AMD64 and ARM64 jobs that load the Docker-save archive and exercise
browser authentication, vault persistence, compiled workers and graceful restart.
Their metadata records identity and checksums, not live-target success. A published release or a green badge alone
does not qualify operations outside those checks.

For a development checkout, `0.0.0-dev` is an unassigned version. Use the PR's
successful CI result for its exact revision; that check does not build native
installers. Results from an earlier revision do not qualify changed source.

| Check                                                     | Evidence to use                                                                                                                                               |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Static, unit, architecture, non-live integration and docs | The exact successful PR CI or release CI run                                                                                                                  |
| Linux, macOS and Windows installers                       | The release's native build/launch jobs and installer checksums                                                                                                |
| Linux browser images                                      | Native archive load/lifecycle jobs, matching image metadata and archive/topology checksums; a desktop installer does not establish browser-image availability |
| 60-second performance soak                                | A local report tied to the source revision; release CI does not run it                                                                                        |
| EDA 26.8.2 capture and cleanup                            | API version, received record and verified owned-resource removal in the exact source-specific report                                                          |
| NSP 26.4.0 setup and cleanup                              | API version, profile reuse, Kafka access and execution removal in the exact source-specific report                                                            |
| API 2/3 to API 4 upgrade and rollback                     | Installed desktop, old/new package digests, profiles and rollback backup; no installed native rehearsal is recorded                                           |
| Native plugin dialogs and credential-backed restore       | Source-specific candidate lifecycle/recovery evidence, with release-source equivalence and exact artifact limits                                              |

<span id="current-source-qualification"></span>

## Unpublished Containerlab rehearsal: 2026-10-07

The [sanitized browser-host qualification report](../assets/qualification/containerlab-2026-10-07.json)
records source `1d10ac2` on Linux ARM64. Its disposable image was stamped `0.9.3`
to exercise unchanged publisher-signed EDA and NSP plugin 0.1.1 packages. That
stamp is not a published release; main remains `0.0.0-dev`. The report records
the tested source identity, image digest, archive checksum and remaining limits.

| Check                           | Executed result                                                                                                                                   |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Shared checks and documentation | 375 test files, 3,249 tests, static/types/dependency checks and 57 documentation browser routes passed                                            |
| 60-second pipeline soak         | 59,997 generated records admitted and published, zero host display drops; bounded retention of 1,000 records                                      |
| Native ARM64 browser archive    | Build, Docker save/load identity, authentication, encrypted profiles, compiled workers and graceful restart passed                                |
| Containerlab lifecycle          | Non-root, no-new-privileges, one CPU, 1 GiB, loopback publication and NAT inspected; graceful exit and owned cleanup passed                       |
| Signed portable plugins         | Real browser upload, signature review, idempotent import, hot removal, cache reinstall and restoration after vault unlock passed for both plugins |
| Compiled Kafka/NATS connections | Real TLS/OAuth Kafka and TLS/token NATS profiles, record fidelity, HTTP/SSE and stop/disconnect passed using host-network fixtures                |
| Live EDA                        | Discovery, capture, record receipt, source preservation and owned-resource cleanup passed through the source host                                 |
| Live NSP                        | Blocked: the configured API timed out before any completed live checks; the full local command exited unsuccessfully                              |

The pipeline soak measures application ingestion, not broker fetch, browser
interaction or native IPC. Signed-file tests do not establish live target access
from the container. Native AMD64 acceptance is configured in Release CI but was
not run locally; Mac browser reachability and native installers were not tested.
The vault encrypts protected credentials and trust material, not the whole data
directory. Required PR CI qualifies the final head separately.

## Pre-release source qualification: 2026-10-06

The [sanitized local qualification report](../assets/qualification/pre-release-2026-10-06.json)
records the checks completed after published v0.8.0. Shared, real-provider,
browser, host integration and soak checks used source `7fc86df`; corrected native
test flows used `94b97f1`. Their application, plugin, build configuration, tooling,
fixture and package-manifest sources are identical. The native test harness
changed to confirm the required plugin review before installation and update.
The report retains both checkpoints, source-tree identities and report hashes.
Required PR CI qualifies its final head separately.

| Check                            | Executed result                                                                                                                                                                        |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Local shared qualification       | 3,088 tests in 357 files; static/types, Go race, dependency/advisory policy and docs passed; 54 HTML pages and 49 browser routes including media                                       |
| Original 60-second pipeline soak | Passed unchanged budgets: zero host display drops, 32.6% one-core CPU and 23.6 ms event-loop p99                                                                                       |
| Real providers                   | Three Kafka and four NATS cases passed, including broker recovery, verified TLS, authentication/permission failures and confirmed stop                                                 |
| Browser workflows                | 11 passed: Monitor/recovery/layout, NATS, plugin lifecycle/offline installation and minified production startup; none skipped                                                          |
| Native Linux ARM64 workflows     | Six scenarios passed: live EDA, live NSP, NATS protected-profile restart, plugin lifecycle, signed-file/cache offline installation and isolated Electron proxy transport; none skipped |
| Live EDA 26.8.2                  | 12 host checks and 26 native named checks passed, including known record receipt, source preservation and owned capture cleanup                                                        |
| Live NSP 26.4.0 build 200        | 23 host checks and 12 native named checks passed, including generated record receipt, profile reuse, interruption recovery and owned execution/topic cleanup                           |
| Real NATS 60-second soak         | 60,000 records published and received; zero duplicates, invalid records or omissions; 13.1% one-core CPU and 19.0 ms event-loop p99                                                    |

These are source-bundle rehearsals on Linux ARM64, with genuine GNOME credential
storage. They do not qualify new macOS/Windows installers, an installed desktop
upgrade/rollback or OS process sandbox enforcement. EDA used its existing capture
application. Plugin catalogs were isolated local fixtures; update packages used
the same code with a newer manifest. NSP API certificate verification was disabled
in the lab; Kafka verification remained enabled. Neither soak measures React
interaction or native IPC latency, and the NATS soak does not measure server
resource usage or internal queue high-water marks.

The report preserves the earlier production-renderer startup failure and its fix
in PR [#75](https://github.com/asadarafat/streamskope/pull/75), plus the corrected
test-launcher and native-review-flow attempts. Historical failed Monitor results
below remain unchanged. No release version was assigned by this qualification.

## Source-bound rehearsal evidence

The following rehearsals exercised the listed source candidates on **2026-10-03 UTC**.
They are source-bound evidence, not an automatic pass for the desktop named at the
top of this page. Use that desktop’s **Published release** section and its exact
qualification report to establish source equivalence and remaining limits.
These records do not establish a published-to-published upgrade or public plugin
package availability. Plugin publication remains independent.
The [sanitized qualification summary](../assets/qualification/lifecycle-2026-10-04.json)
records the individual source revisions, package and retained-report hashes,
results and limits. Plugin checks used built API 4 packages through an isolated
local catalog; they do not establish public package availability.

| Check                               | Recorded result                                                                                                                                                                                | Source and evidence                                                                                |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| EDA 26.8.2 installed lifecycle      | 26 checks passed on Linux ARM64 with genuine GNOME credential storage: install, occupied-port recovery, known record in the UI, reconnect, active update, removal, reinstall and owned cleanup | Source `f26f406`; [sanitized summary](../assets/qualification/lifecycle-2026-10-04.json)           |
| NSP 26.4.0 installed host lifecycle | 23 checks passed: workflow trust retrieval, profile reuse, known generated record, update/removal/reinstall, interruption recovery, ownership refusal and cleanup                              | Source `e40e3b7`; [sanitized summary](../assets/qualification/lifecycle-2026-10-04.json)           |
| NSP 26.4.0 installed native UI      | 12 checks passed on Linux ARM64 with genuine GNOME credential storage: install, refresh/reuse, known record in the UI, hot update, removal/reinstall/reconnect and owned cleanup               | Source `99f7f2d`; [sanitized summary](../assets/qualification/lifecycle-2026-10-04.json)           |
| Linux x64 installer recovery        | Passed: published 0.6.0 AppImage replaced by a source `0.0.0-dev` installer; candidate restart, full baseline-backup restoration and filtered export without re-entering credentials           | Source `c284afc`; [hosted run](https://github.com/asadarafat/streamskope/actions/runs/37160807855) |
| macOS ARM64 installer recovery      | Passed: published 0.6.0 DMG replaced by a source `0.0.0-dev` installer; candidate restart, full baseline-backup restoration and filtered export without re-entering credentials                | Source `c284afc`; [hosted run](https://github.com/asadarafat/streamskope/actions/runs/37160807855) |
| Windows x64 installer recovery      | Passed: published 0.6.0 NSIS installer replaced by a source `0.0.0-dev` installer; candidate restart, full baseline-backup restoration and filtered export using Windows DPAPI                 | Source `c284afc`; [hosted run](https://github.com/asadarafat/streamskope/actions/runs/37160807855) |

At the time of those rehearsals, the candidate application and plugin source,
plus npm manifests and lockfile, matched the passing EDA, NSP native UI and native
installer runs listed above. The summary records those historical Git object
identities; they do not establish equivalence to later source changes. A complete local check on tree `10c0b1e`
(identical to merged `552de73`) passed 2,379 tests across 290 files, Go race and
dependency checks, the original 60-second soak, 50 HTML pages and 45 documentation
browser routes including media. Configured live EDA (12 checks) and NSP (23 checks)
also passed on that integrated tree; these do not replace the separate native UI runs.

The NSP host run used an isolated in-memory profile store; the separate native UI
run exercised OS-protected profiles on later core source. Their results are not
combined into a single run. EDA used the existing cluster application and verified
that its original Producer stayed unchanged. NSP used uniquely owned fixture
topics and generated records; it removed its executions and topics while retaining
the shared immutable helper. Both plugins retained the desktop process/window
through hot lifecycle changes. Update fixtures used unchanged built code with a
newer manifest, not a published upgrade. EDA API certificate verification remained
enabled with the lab CA trusted by the host; Kafka used its loopback tunnel over
an authenticated TLS WebSocket. NSP lab API certificate verification was disabled;
NSP Kafka broker TLS verification remained enabled. These results do not
qualify NSP Kafka OAuth, fresh EDA cluster-app installation, API 2/3 migration or
OS process sandbox enforcement.

The native [recovery procedure](#repeat-the-same-account-recovery-rehearsal) uses
the same OS account and credential service, and deliberately starts from the
baseline's pre-restart backup. Candidate restart passed; baseline restart is not
claimed. Linux launches the AppImage's extracted payload, leaving FUSE and desktop
launcher integration outside this check. None of these installer recovery results
establishes cross-account credential transfer, lost-keyring recovery or plugin
rollback. Historical published-release evidence below remains unchanged.

## Topic Monitor candidate: 2026-10-04

The Monitor changes in PRs [#52](https://github.com/asadarafat/streamskope/pull/52),
[#53](https://github.com/asadarafat/streamskope/pull/53) and
[#54](https://github.com/asadarafat/streamskope/pull/54) change application behavior.
The [integrated shared CI and Monitor browser workflow](https://github.com/asadarafat/streamskope/actions/runs/37199802840)
passed. Those checks do not include the local performance soak or live EDA.

The [retained candidate report](../assets/qualification/topic-monitor-2026-10-04.json)
records the 60-second soak source `63c1d7a`, its application source-tree identity,
the original report digest, configuration, budgets and failing results. The
merged Monitor implementation has the same application source tree. Documentation
edits do not convert these results into passing qualification.

| Check                               | Candidate result                                                                                         |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Display omissions                   | **Failed:** 28.56%; maximum 2%                                                                           |
| CPU usage                           | **Failed:** 78.99% of one CPU core; maximum 60%                                                          |
| Event-loop p99                      | **Failed:** 265.16 ms; maximum 150 ms                                                                    |
| Memory bounds and record accounting | Passed in the same replay                                                                                |
| Live EDA                            | Incomplete: capture readiness and lease renewal passed; Kafka connection timed out before record receipt |
| EDA cleanup                         | Stop, owned-resource removal and repeat Stop passed                                                      |
| Installed native upgrade/rollback   | No new rehearsal for the changed Monitor source                                                          |

The replay exercised production ingestion, the facade and reducer with mixed
payloads and clone round trips; it did not measure Kafka fetch, actual desktop
IPC, React rendering or product interaction latency. The host was under memory
pressure, but no controlled comparison establishes that as the cause. The budgets
were not relaxed. Earlier passing lifecycle, soak and recovery results above remain
historical evidence, rather than qualification of this candidate.

<!-- publication-qualification -->

## Published release: v0.10.2

These pages describe [v0.10.2](../releases/v0.10.2.md) at source [`a6996f1`](https://github.com/asadarafat/streamskope/commit/a6996f10cc931724115449c93f76961e040a9ae6). The release notes link the packaging workflow; earlier release results below are historical.

The [source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.10.2/qualification-v0.10.2.json) was included in the publication event. Read its executed checks, source identity, environment and limitations; the link alone does not establish that every check passed.
<!-- /publication-qualification -->

## Historical qualification: v0.10.1

These pages describe [v0.10.1](../releases/v0.10.1.md) at source [`7350cf1`](https://github.com/asadarafat/streamskope/commit/7350cf1c98e7b59a8558a2ca81812ae9b1b1bb91). The release notes link the packaging workflow; earlier release results below are historical.

The [source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.10.1/qualification-v0.10.1.json) was included in the publication event. Read its executed checks, source identity, environment and limitations; the link alone does not establish that every check passed.

## Historical qualification: v0.10.0

These pages describe [v0.10.0](../releases/v0.10.0.md) at source [`0e12476`](https://github.com/asadarafat/streamskope/commit/0e12476bf0f79c46de8cf5a1212374f8d8923af0). The release notes link the packaging workflow; earlier release results below are historical.

The [source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.10.0/qualification-v0.10.0.json) records exact source and artifact identities, executed checks, resolved failed attempts and remaining limits. Its digest is included in `SHA256SUMS`.

Static checks, 3,277 shared tests, documentation qualification and the local
60-second application-ingestion soak passed. The exact ARM64 browser image passed
compiled Kafka/NATS authentication and TLS checks, signed portable plugin
lifecycle checks, encrypted-vault restoration after Containerlab recreation and
an actual Mac Chrome workflow. Both public image architectures matched their
offline archives after independent anonymous pulls. Native packaging and launch
checks passed in the release workflow.

Live EDA 26.8.2 capture received a real event, preserved the original Producer
and removed owned temporary resources through the compiled browser host. That
local-lab check used a container hostname mapping and disabled EDA API certificate
verification; it does not establish certificate trust. Live NSP was blocked by an
API connection timeout; no live NSP pass is claimed. Installed native desktop
live capture and upgrade/rollback were not rehearsed for this release.

## Historical qualification: v0.9.2

These pages describe [v0.9.2](../releases/v0.9.2.md) at source [`0acc38c`](https://github.com/asadarafat/streamskope/commit/0acc38c2d5db9c7592cf9adfe835de6d8f0ded93). The release notes link the packaging workflow; earlier release results below are historical.

The [source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.9.2/qualification-v0.9.2.json) was included in the publication event. Read its executed checks, source identity, environment and limitations; the link alone does not establish that every check passed.

## Historical qualification: v0.7.1

[v0.7.1](../releases/v0.7.1.md) was published on **2026-10-04** from source
[`be138de`](https://github.com/asadarafat/streamskope/commit/be138de69618f4cd136015dad22e93f831a71caa).
The [source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.7.1/qualification-v0.7.1.json)
separates the release checks from earlier source-bound lifecycle, recovery and
performance runs. The [release workflow](https://github.com/asadarafat/streamskope/actions/runs/37181432250)
passed. All installers remain unsigned.

| Check                           | Recorded result and evidence                                                                                                                                                   |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Shared release qualification    | 2,384 tests across 291 files; 49 Python documentation tests and four JavaScript accessibility tests; 50 HTML pages and 45 browser routes, including media                      |
| Native packages                 | Linux x64 AppImage, macOS ARM64 DMG and Windows x64 installer build and packaged-app checks passed on native runners                                                           |
| Live installed plugin lifecycle | Separate source-bound EDA 26-check native run, NSP 23-check host run and NSP 12-check native run; source and environment limits are recorded above                             |
| Native credential recovery      | Earlier published 0.6.0 to source-candidate rehearsals passed on all three platforms; candidate restart and full same-account backup restoration passed                        |
| Integrated local qualification  | The identical application source passed shared checks, the original 60-second soak, documentation qualification and configured live EDA/NSP checks                             |
| Unsigned EDA application        | Complete OCI packaging passed; signed publication remains separate                                                                                                             |
| Download integrity              | [SHA256SUMS](https://github.com/asadarafat/streamskope/releases/download/v0.7.1/SHA256SUMS) matches the three uploaded installer digests and includes the qualification report |

This is not a recorded installed upgrade from published 0.7.0 to published 0.7.1.
The earlier native rehearsals used a `0.0.0-dev` candidate with identical application
source; test and documentation trees differ. They cover the same OS account and
credential service, with Linux using the extracted AppImage payload. They do not
establish cross-account recovery, lost-keyring recovery, FUSE integration or
plugin rollback. The soak measures application ingestion, not UI, real IPC or
Kafka latency. No existing plugin release or compatibility manifest was changed.

## Historical qualification: v0.7.0

[v0.7.0](../releases/v0.7.0.md) was published on **2026-10-03** from source
[`05c972c`](https://github.com/asadarafat/streamskope/commit/05c972c615de1417b0243bc309ccb93e267c1809).
The [release workflow](https://github.com/asadarafat/streamskope/actions/runs/37149711415) passed. The
[source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.7.0/qualification-v0.7.0.json) records the identical qualified
source tree, executed checks, environment conditions, remaining gaps and evidence hashes.

| Check                                     | Recorded result and evidence                                                                                                                                                                                                                                                                                       |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Shared qualification                      | [CI passed](https://github.com/asadarafat/streamskope/actions/runs/37149711415/job/111280882960): 2,363 tests across 288 files; 48 docs tooling tests, 49 HTML pages and 44 browser routes including media                                                                                                         |
| Native installers and packaged-app checks | [Linux x64](https://github.com/asadarafat/streamskope/actions/runs/37149711415/job/111282386959), [macOS ARM64](https://github.com/asadarafat/streamskope/actions/runs/37149711415/job/111282386944) and [Windows x64](https://github.com/asadarafat/streamskope/actions/runs/37149711415/job/111282386927) passed |
| Real observation and relationship checks  | Four scenarios across three files passed: actual offsets/lag/ISR and protected sampling, restart history and denied-group unknowns; Connect/group/Registry references and potential impact; offset-reset denial, stale-state and read-back regressions                                                             |
| Browser diagnosis and graph               | Nine real rising-lag observations, held-out forecast, skew/key analysis, stopped collection and graph selection passed; no detected Axe violations or browser diagnostics                                                                                                                                          |
| Local docs and production build           | All 44 documentation routes, search, themes, mobile, accessibility and media passed; production build passed                                                                                                                                                                                                       |
| 60-second performance soak                | 59,992 generated/delivered, zero host display drops, 29.17% of one CPU core, 674 MiB peak RSS and 28.75 ms event-loop p99; original limits passed under the isolation conditions below                                                                                                                             |
| Live EDA 26.8.2                           | Twelve checks passed: API/application versions, discovery, capture readiness, lease, actual Kafka receipt, observed health/relationship metadata, source preservation, stop, owned cleanup and repeated stop                                                                                                       |
| Live NSP 26.4.0                           | Twelve checks passed: version, combined workflow, tested/saved/reused profile, execution cleanup, saved connection and 176 topics, observed health/relationship metadata, hot removal preserving the profile and secret-free renderer events                                                                       |
| Unsigned EDA OCI application              | [Packaging passed](https://github.com/asadarafat/streamskope/actions/runs/37149711415/job/111282386967); signing and publication remain separate                                                                                                                                                                   |
| Download integrity                        | [SHA256SUMS](https://github.com/asadarafat/streamskope/releases/download/v0.7.0/SHA256SUMS) matches all three installer digests and includes the qualification report                                                                                                                                              |

Local qualification passed in stages on the identical final source. Shared checks,
docs, the original soak and build used CPU affinity `6,7` with the colocated EDA lab
temporarily paused. The lab was restored before live checks. Its Kafka application
API initially returned 404 after resume; EDA registered its manifest again and the
complete live EDA/NSP rerun passed without application or cluster changes. The
report preserves that failed attempt and the earlier corrected test expectations
for the plugin host ceiling. No performance budgets were relaxed. Concurrent heavy
local-lab operation remains outside the performance qualification.

The soak measures application ingestion, not real IPC, Kafka fetch or UI interaction
latency. Observations measure offset positions and client request cost; forecasts,
anomalies and hypotheses are bounded heuristics. Relationships are partial evidence
and never approve schema changes. The vendor metadata reads do not qualify every
group, schema mapping or diagnosis. Installed native plugin migration and
credential-service recovery were not rehearsed; NSP Kafka SASL OAuth and other
vendor policies remain outside the live result. History is private, unencrypted
local JSON. Installers are unsigned and plugin publication remains independent.

## Historical qualification: v0.6.0

[v0.6.0](../releases/v0.6.0.md) was published on **2026-10-03** from source
[`a5ee948`](https://github.com/asadarafat/streamskope/commit/a5ee94876d5cc1c71a847582ea5eaabf3ae8226e).
The [release workflow](https://github.com/asadarafat/streamskope/actions/runs/37137468007) passed. The
[source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.6.0/qualification-v0.6.0.json) records the identical qualified
source tree, actual checks, environment conditions, limits and evidence hashes.

| Check                                     | Recorded result and evidence                                                                                                                                                                                                                                                                                            |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Shared qualification                      | [CI passed](https://github.com/asadarafat/streamskope/actions/runs/37137468007/job/111244903601): 2,335 tests across 280 files; 46 documentation pages and 41 browser routes                                                                                                                                            |
| Native installers and packaged-app checks | [Linux x64](https://github.com/asadarafat/streamskope/actions/runs/37137468007/job/111246605763), [macOS ARM64](https://github.com/asadarafat/streamskope/actions/runs/37137468007/job/111246605724) and [Windows x64](https://github.com/asadarafat/streamskope/actions/runs/37137468007/job/111246605756) passed      |
| Real integrations and developer tools     | Four scenarios passed: authenticated TLS Connect lifecycle, validation, failed-task recovery and DLQ byte replay; two-cluster promotion and drift rejection; Registry-generated client round-trip and protected CLI reads/exports; owned sandbox repeat startup, transforms, down/up reset and foreign-resource refusal |
| Browser review and apply                  | Real Connect creation and imported-snapshot promotion passed; validation/review made no broker changes, exact confirmation was required and read-back matched the selected change; no detected Axe violations or browser diagnostics                                                                                    |
| Local docs and production bundle          | All 41 documentation routes, search, themes, mobile, accessibility and media passed; production build and isolated bundled-client generation/syntax/provenance/license passed                                                                                                                                           |
| 60-second performance soak                | 59,998 generated/delivered, zero host display drops, 31.14% of one CPU core, 672 MiB peak RSS and 25.76 ms event-loop p99; original limits passed under the isolation conditions below                                                                                                                                  |
| Live EDA 26.8.2                           | API version, installed cluster application, capture readiness, lease, actual Kafka receipt, source preservation, stop, owned cleanup and repeated stop passed after lab restoration                                                                                                                                     |
| Live NSP 26.4.0                           | Combined workflow, tested/saved profile, idempotent reuse, saved connection and 176 topics, execution cleanup and hot removal preserving the profile passed; renderer events excluded secrets                                                                                                                           |
| Unsigned EDA OCI application              | [Packaging passed](https://github.com/asadarafat/streamskope/actions/runs/37137468007/job/111246605687); signing and publication remain separate                                                                                                                                                                        |
| Download integrity                        | [SHA256SUMS](https://github.com/asadarafat/streamskope/releases/download/v0.6.0/SHA256SUMS) matches all three installer digests and includes the qualification report                                                                                                                                                   |

Local qualification completed in stages. The first full run passed shared tests but
stopped at the soak on the busy, paging host; the unchanged 0.5.0 baseline also
failed under contention. The same soak passed with the local EDA test-lab container
temporarily paused and CPU affinity `6,7`. Docs/build/disposable fixtures were also
isolated after a media timeout. The lab was restored and readiness verified before
live EDA/NSP tests. No source or test limits were changed to obtain these results.
Concurrent heavy local-lab operation remains outside this performance qualification.

The soak measures application ingestion, not real IPC, Kafka fetch or UI interaction
latency. Connect/DLQ support depends on the connector; environment promotion covers
supported existing topic settings. CLI/sandbox tools run from matching source, and
client generation supports bounded JSON Schema draft-07/CommonJS only. Installed
native plugin migrations and credential-service recovery were not rehearsed. NSP
Kafka SASL OAuth and other vendor policies remain outside the live result.
Installers are unsigned; plugin releases remain independent. Back up the complete
profile store before saving Connect fields if a downgrade may be required.

## Historical qualification: v0.5.0

[v0.5.0](../releases/v0.5.0.md) was published on **2026-10-03** from source
[`0253681`](https://github.com/asadarafat/streamskope/commit/0253681d2761c66cfe2d20ec1e1438ad948b0ce2).
The [release workflow](https://github.com/asadarafat/streamskope/actions/runs/37122792469) passed. The
[source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.5.0/qualification-v0.5.0.json)
records the identical qualified source tree, local and native checks, environment
versions, limits and evidence hashes.

| Check                                     | Recorded result and evidence                                                                                                                                                                                                                                                                                       |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Shared qualification                      | [CI passed](https://github.com/asadarafat/streamskope/actions/runs/37122792469/job/111202135645): 2,310 tests across 272 files; 40 documentation pages and 35 browser routes, including media playback                                                                                                             |
| Native installers and packaged-app checks | [Linux x64](https://github.com/asadarafat/streamskope/actions/runs/37122792469/job/111203511387), [macOS ARM64](https://github.com/asadarafat/streamskope/actions/runs/37122792469/job/111203511411) and [Windows x64](https://github.com/asadarafat/streamskope/actions/runs/37122792469/job/111203511393) passed |
| Real recovery behavior                    | Four tests passed against owned Apache Kafka 4.3.1 fixtures: stopped-group reset/read-back, stale and denied operations, same-topic/other-topic/separate-broker replay with tombstones and duplicate headers, and exact ACL changes with StandardAuthorizer access behavior                                        |
| Browser recovery                          | Replay, offset reset and ACL review/application passed through the real browser host; source reader remained active after replay; no detected Axe violations in all three dialogs or browser diagnostics                                                                                                           |
| 60-second performance soak                | 59,991 records generated and delivered, zero host display drops, 30.56% of one CPU core, 679 MiB peak RSS and 23.82 ms event-loop p99; all unchanged limits passed                                                                                                                                                 |
| Live EDA 26.8.2                           | API version, installed cluster app, capture readiness, lease, real Kafka receipt, source preservation, owned-resource cleanup and repeated stop passed                                                                                                                                                             |
| Live NSP 26.4.0                           | Combined workflow, tested/saved profile, idempotent profile reuse, connection and discovery of 176 topics, execution cleanup, and hot removal preserving the profile passed; renderer events excluded secrets                                                                                                      |
| Unsigned EDA OCI application              | [Packaging passed](https://github.com/asadarafat/streamskope/actions/runs/37122792469/job/111203511367); signing and publication remain separate                                                                                                                                                                   |
| Download integrity                        | [SHA256SUMS](https://github.com/asadarafat/streamskope/releases/download/v0.5.0/SHA256SUMS) matches all three installer digests and includes the qualification report                                                                                                                                              |

The complete local check passed with one test worker after an earlier four-worker
run was interrupted by memory-pressure timeouts. Limits and tests were not relaxed.
The soak measures application ingestion, not real Electron IPC, Kafka network-fetch
or UI interaction latency. Live capture checks exercise the source plugin/API
paths; installed native plugin migrations and credential-service recovery were
not rehearsed. NSP Kafka SASL OAuth and other vendor permission policies remain
outside this result. Installers are unsigned; plugin releases remain independent.

## Historical qualification: v0.4.0

[v0.4.0](../releases/v0.4.0.md) was published on **2026-10-03** from source
[`f1cddce`](https://github.com/asadarafat/streamskope/commit/f1cddce18a1cb97a78e17f6531e914f39449989d).
The [release workflow](https://github.com/asadarafat/streamskope/actions/runs/37110089585) passed on its first attempt. The
[source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.4.0/qualification-v0.4.0.json)
records the source/tree, executed checks, limits and evidence hashes.

| Check                                     | Recorded result and evidence                                                                                                                                                                                                                                                                                       |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Shared qualification                      | [CI passed](https://github.com/asadarafat/streamskope/actions/runs/37110089585/job/111166213587): 2,267 tests across 266 files; 36 documentation pages and 31 browser routes, including media playback                                                                                                             |
| Native installers and packaged-app checks | [Linux x64](https://github.com/asadarafat/streamskope/actions/runs/37110089585/job/111167119621), [macOS ARM64](https://github.com/asadarafat/streamskope/actions/runs/37110089585/job/111167119636) and [Windows x64](https://github.com/asadarafat/streamskope/actions/runs/37110089585/job/111167119616) passed |
| Real Kafka and Registry                   | Two scenarios passed on Apache Kafka 4.3.1 and Karapace 5.0.3: writer IDs/references, exact Avro/Protobuf bytes and int64 values, no-write sample previews, reviewed publication without duplicate sends, cross-topic correlation and explicit permission denial                                                   |
| Browser and bundled worker                | Decoding, pinned comparison, trace coverage, active-reader continuity, schema references and sample preview passed; no detected dialog accessibility violations. An isolated copy of the bundled worker generated and decoded JSON/Avro/Protobuf without repository dependencies                                   |
| 60-second performance soak                | Passed configured limits: 59,987 generated records, 59,136 delivered, 851 accounted display-buffer drops, 40.30% of one CPU core and 739 MiB peak RSS. This measures application ingestion; real IPC, Kafka network-fetch and UI interaction latency remain unmeasured                                             |
| Unsigned EDA OCI application              | [Packaging passed](https://github.com/asadarafat/streamskope/actions/runs/37110089585/job/111167119589); no live-cluster or signing result implied                                                                                                                                                                 |
| Download integrity                        | [SHA256SUMS](https://github.com/asadarafat/streamskope/releases/download/v0.4.0/SHA256SUMS) matches all three installer digests and includes the qualification report                                                                                                                                              |

Live EDA/NSP were unconfigured and skipped. Installed native plugin migrations and
credential-backed recovery were not rehearsed for this source. The installers are
unsigned; plugin packages remain separately versioned and published. Avro-reference
behavior was checked with bounded adapter/worker fixtures; Karapace 5.0.3 does not
support that live scenario.

## Historical qualification: v0.3.0

[v0.3.0](../releases/v0.3.0.md) was published on **2026-10-03** from source
[`002430b`](https://github.com/asadarafat/streamskope/commit/002430b024944f1209fba2747389909f38545571).
The [release workflow](https://github.com/asadarafat/streamskope/actions/runs/37081001855)
passed on its first attempt. The release retains a downloadable
[source-specific qualification report](https://github.com/asadarafat/streamskope/releases/download/v0.3.0/qualification-v0.3.0.json)
with the qualified source/tree, commands, scoped outcomes and log hashes.

| Check                                                   | Recorded result and evidence                                                                                                                                                                                                                                                                                    |
| ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Shared qualification                                    | [CI passed](https://github.com/asadarafat/streamskope/actions/runs/37081001855/job/111081408338): 2,215 tests, 34 documentation pages and 29 browser routes, including media playback                                                                                                                           |
| Native installers, package inspection and launch checks | [Linux x64](https://github.com/asadarafat/streamskope/actions/runs/37081001855/job/111083200942), [macOS ARM64](https://github.com/asadarafat/streamskope/actions/runs/37081001855/job/111083201015), [Windows x64](https://github.com/asadarafat/streamskope/actions/runs/37081001855/job/111083200955) passed |
| Core live qualification                                 | 10 scenarios passed on Apache Kafka 4.3.1 and Karapace 5.0.3: original bytes, protection, reviewed writes, configuration, groups, exact ACLs/denials, Registry compatibility and Protobuf references; see the source-specific report above                                                                      |
| 60-second performance soak                              | Passed: zero host delivery drops, 29.74% of one CPU core and 646 MiB peak RSS. Production-session/facade replay; UI interaction and network-fetch latency are not measured                                                                                                                                      |
| Unsigned EDA OCI application                            | [Packaging passed](https://github.com/asadarafat/streamskope/actions/runs/37081001855/job/111083200958); not a live-cluster or signing result                                                                                                                                                                   |
| Download integrity                                      | [SHA256SUMS](https://github.com/asadarafat/streamskope/releases/download/v0.3.0/SHA256SUMS) matches the three uploaded installers' GitHub SHA-256 digests                                                                                                                                                       |

The installers are unsigned. Live EDA/NSP checks were unconfigured and skipped;
installed native plugin upgrades and credential-backed recovery were not rehearsed
for this source. Plugin API 4 packages remain separately versioned and published.

## Historical qualification: v0.2.0

[v0.2.0](../releases/v0.2.0.md) was published on **2026-10-02** from source
[`f3b7a3e`](https://github.com/asadarafat/streamskope/commit/f3b7a3ebb62d7a01ab269705f9b888bbaa70f6c7).
The [release workflow](https://github.com/asadarafat/streamskope/actions/runs/37048395196)
passed on its second attempt after the first runner timed out downloading Ubuntu
packages for video playback checks. The source and qualification gates were unchanged.

| Check                                                   | Recorded result and evidence                                                                                                                                                                                                                                                                                       |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Shared qualification                                    | [CI passed](https://github.com/asadarafat/streamskope/actions/runs/37048395196/job/110982283219): 2,122 tests, 33 documentation pages and 28 browser routes, including video playback                                                                                                                              |
| Native installers, package inspection and launch checks | [Linux x64](https://github.com/asadarafat/streamskope/actions/runs/37048395196/job/110984596249), [macOS ARM64](https://github.com/asadarafat/streamskope/actions/runs/37048395196/job/110984596071) and [Windows x64](https://github.com/asadarafat/streamskope/actions/runs/37048395196/job/110984596120) passed |
| Unsigned EDA OCI application                            | [Packaging passed](https://github.com/asadarafat/streamskope/actions/runs/37048395196/job/110984596070); this is not a live-cluster or signing result                                                                                                                                                              |
| Download integrity                                      | [SHA256SUMS](https://github.com/asadarafat/streamskope/releases/download/v0.2.0/SHA256SUMS) matches the three uploaded installers' GitHub SHA-256 digests                                                                                                                                                          |

The installers are unsigned. No live EDA/NSP result or installed legacy-plugin
upgrade and credential-recovery rehearsal is recorded for this release. Those
results must be recorded separately; the successful release does not establish them.

## Historical qualification: v0.1.0+build.1

The [v0.1.0+build.1 prerelease](https://github.com/asadarafat/streamskope/releases/tag/v0.1.0%2Bbuild.1),
application **0.1.0**, was published on **2026-10-01** from source
[`c0f2bfb586709e50027f0a690b8843974f9cc2c5`](https://github.com/asadarafat/streamskope/commit/c0f2bfb586709e50027f0a690b8843974f9cc2c5).
Its unsigned Linux x64, macOS ARM64 and Windows x64 installers and
[SHA256SUMS](https://github.com/asadarafat/streamskope/releases/download/v0.1.0%2Bbuild.1/SHA256SUMS)
are available. Publication establishes download availability; qualification below
is based on evidence for that exact source.

### Recorded automated checks

The [release CI run](https://github.com/asadarafat/streamskope/actions/runs/36916274566)
completed successfully. Its jobs record the following results:

| Check                                                          | Result                                                              | Evidence                                                                                             |
| -------------------------------------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Static, unit, architecture, non-live integration and docs      | Passed: 1,894 tests; documentation browser checks covered 27 routes | [Shared CI job](https://github.com/asadarafat/streamskope/actions/runs/36916274566/job/110550906276) |
| Linux x64 native build, package inspection and launch checks   | Passed                                                              | [Linux job](https://github.com/asadarafat/streamskope/actions/runs/36916274566/job/110553618086)     |
| macOS ARM64 native build, package inspection and launch checks | Passed                                                              | [macOS job](https://github.com/asadarafat/streamskope/actions/runs/36916274566/job/110553618088)     |
| Windows x64 native build, package inspection and launch checks | Passed                                                              | [Windows job](https://github.com/asadarafat/streamskope/actions/runs/36916274566/job/110553618117)   |

Native launch checks exercise the production Electron boundary and packaged app.
They do not exercise installed plugin onboarding or real credential-service recovery.

### Evidence still to be recorded

These gaps remain after publication. Results from trial releases are not carried
forward as qualification for this source.

| Check                                               | Required evidence                                                                          | Current record                                                                                                                                    |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| 60-second performance soak                          | Local report tied to the exact source                                                      | [Release notes](https://github.com/asadarafat/streamskope/releases/tag/v0.1.0%2Bbuild.1) report a local pass; no source-specific report is linked |
| EDA 26.8.2 capture and cleanup                      | Configured local cluster, API version, received record and verified owned-resource removal | No live result recorded for this release                                                                                                          |
| NSP 26.4.0 setup and cleanup                        | Configured target, API version, profile reuse, Kafka access and verified execution removal | No live result recorded for this release                                                                                                          |
| Native plugin dialogs and credential-backed restore | Installed desktop, OS/architecture and real credential-service rehearsal                   | No installed native workflow result recorded for this release                                                                                     |

Qualification CI runs on pull requests; merging to `main` does not repeat it.
The manual Release workflow calls the same CI to qualify its selected source again
before assigning a version and packaging. The
[CI workflow](https://github.com/asadarafat/streamskope/actions/workflows/ci.yml)
is a place to find results, not a substitute for linking the exact successful run.
Local `npm run check` also runs the soak and configured live checks. An unconfigured
live check is skipped, not passed. Signed EDA cluster-app publication is a separate
operation from desktop release packaging.

## Qualification boundaries

Declared plugin compatibility identifies the target versions the implementation
accepts. It does not certify every operation, platform or permission policy. Read
[the compatibility matrix](../start/compatibility.md) and each plugin's prerequisites
before using an integration.

Package launch checks do not qualify installed EDA/NSP onboarding. Native
credential-backed restore, cross-version upgrades/downgrades, OS-key-loss recovery,
vendor-specific least-privilege policies, live NSP Kafka SASL OAuth and EDA
controller-outage cleanup each need explicit evidence. A successful TLS connection
to one NSP listener does not establish OAuth broker support.

## Repeat the core investigation rehearsal

Start the owned [AIO development fixture](../start/development.md), then run:

```sh
node tools/package/e2e.mjs web test/e2e/web-real-connection.spec.ts test/e2e/web-authentication-failures.spec.ts --grep 'connects, inspects, exports|rejects invalid OAuth'
```

This exercises the browser UI and real Kafka host together: save a protected
connection profile, read and inspect a known record, stop consumption, export a
bounded read, disconnect, reconnect with the same profile, read again, and remove
the profile. It checks the downloaded record's payload, partition and offset, and
confirms that credentials are absent from the export and activity. A second scenario
verifies rejected OAuth credentials leave the application disconnected with
actionable diagnostics. The read scenario creates a uniquely named fixture topic
and deletes it during teardown.

Retain the exact source revision, fixture revision from `aio-kafka/SOURCE_COMMIT`,
broker image/version, platform and the sanitized results in
`test-results/web/playwright-results.json`. Report failed or skipped scenarios
explicitly. These checks use session-only profile storage in the development host;
they do not establish native credential restore, installed-app upgrades or live
EDA/NSP qualification. Run the corresponding rehearsals separately.

## Repeat the same-account recovery rehearsal

Use the manual [Native recovery workflow](https://github.com/asadarafat/streamskope/actions/workflows/native-recovery.yml)
for actual installer replacement on Linux x64, macOS ARM64 and Windows x64. Select
**current-source** and a published baseline such as `0.6.0`. It builds the clean
selected source as `0.0.0-dev`, verifies the published baseline against its
`SHA256SUMS`, and records both installer hashes plus the candidate source revision.
It does not assign a release version or publish installers.

The rehearsal uses isolated installation and app-data directories. It saves a
protected profile and absolute historical query in the baseline, replaces the
application, reconnects, restarts the candidate, then deletes and restores the
profile and query from the complete baseline backup. A known filtered record must
be exported without re-entering credentials. Opening the restored query must not
connect or read until explicitly requested. The real OS credential service must
be available; deterministic test encryption and plaintext backends are rejected.

Candidate mode explicitly omits the baseline restart and starts from its
pre-restart backup. It still requires candidate restart and restored-profile
reconnection. The published `0.6.0` baseline failed its restart check on macOS
because its preference directory collided with Chromium's file; that failure is
retained separately and is not converted into a pass by a fixed candidate.
**published-release** mode requires baseline restart as well as replacement with
the selected published target. The `0.6.0` → `0.7.0` run stopped at that baseline
failure before reaching the target installer.

To run the same candidate check locally on macOS ARM64, install the
[development prerequisites](../start/development.md), Java 17+ and unlock the OS
credential service, then run from a clean checkout:

```sh
node --import tsx tools/check/native-recovery.ts --from 0.6.0 --candidate dist/installers/StreamSkope-0.0.0-dev-darwin-arm64.dmg --candidate-version 0.0.0-dev
```

Use the matching native filename on Linux, where Xvfb, D-Bus and GNOME Keyring
are also required. Windows installer rehearsals run only in disposable Actions
accounts because NSIS modifies user registration and shortcuts. Without explicit
fixture connection variables, the runner starts a checksum-pinned, loopback-only
Kafka broker with TLS and RS256 OAuth. macOS replaces an isolated `.app`; Linux
replaces the AppImage and starts its extracted native payload. Linux FUSE and
desktop launcher integration are outside this rehearsal.

The sanitized report under `dist/native-recovery/` records outcome and cleanup;
only this report is uploaded by the workflow. Record the exact source, OS/CPU,
Electron version, credential backend, baseline/candidate versions, installer
hashes and reconnect results. Recipes, rules, plugin rollback, other OS accounts
and lost credential stores still require separate evidence. Before relying on a
downgrade, rehearse its exact source/target builds.

### Source-only recovery checks

For a focused development rehearsal with the [AIO fixture](../start/development.md):

```sh
npm run build
STREAMSKOPE_NATIVE_RECOVERY=1 node tools/package/e2e.mjs electron test/e2e/electron-profile-recovery.spec.ts
```

PowerShell users can set `$env:STREAMSKOPE_NATIVE_RECOVERY = "1"` before the same
`node` command. The default fixture is `streamskope-kafka`; set
`STREAMSKOPE_TEST_FIXTURE_NAME` to another owned disposable AIO fixture when needed.
This launches the source build and qualifies same-build recovery only.
`STREAMSKOPE_UPGRADE_FROM_EXECUTABLE` can provide an older executable with the same
credential identity, but that mode does not replace an installer and still requires
the older executable to pass baseline restart. It does not use your normal profile
directory. Ordinary PR CI leaves this rehearsal skipped unless explicitly configured;
a skip is not a pass.

## Repeat the restricted-account rehearsal

Have the administrator prepare a **disposable**, default-deny Kafka broker with
separate administrator and inspection principals. Create two topics, seed one known
record in `docs-inspect`, and leave `docs-hidden` unauthorized for the inspection
principal. On that broker only, the administrator can grant the documented scope:

```sh
kafka-acls.sh --bootstrap-server LAB_ADMIN_HOST:9093 \
  --command-config /path/to/lab-admin.properties --add \
  --allow-principal User:docs-inspector --operation Read --operation Describe \
  --topic docs-inspect
kafka-acls.sh --bootstrap-server LAB_ADMIN_HOST:9093 \
  --command-config /path/to/lab-admin.properties --add \
  --allow-principal User:docs-inspector --operation Read \
  --group streamskope- --resource-pattern-type prefixed
```

These are Apache Kafka CLI examples; use a supported client authentication method
for the inspection principal. Protect the administrator properties file and never
attach it to an issue. The prefixed grant accommodates the new UUID used by each desktop read.

1. Connect as the inspection principal. Expect `docs-inspect`, not `docs-hidden`.
2. Read **First N**, limit `1`, and compare its payload to the seeded record.
3. In this disposable environment, attempt a latency probe, a topic configuration
   change and an ACL creation. Expect authorization failures, not successful writes.
4. As administrator, confirm topic contents/configuration and ACLs remain unchanged.
   Confirm no application consumer-group offsets were changed.
5. Record both positive reads and denied writes, broker/auth versions and exact
   grants. A successful connection alone is not qualification. Remove the disposable
   test resources when finished.

Record whether a denied write was attempted through the desktop UI or directly
through the adapter. An adapter result alone does not qualify the complete UI workflow.

## Record a new rehearsal

Record the UTC time, exact committed source revision, desktop download tag and
checksum, plugin package identity and digest, target API version, host platform,
procedure, observed outcomes and limitations. If the checkout is dirty, retain its
patch and digest privately and explicitly mark the result as an uncommitted-source
rehearsal. Preserve a sanitized dated summary and link the exact CI run or immutable
release asset. Do not use an overwriteable local report path as the sole historical
reference, and do not publish credentials or raw workflow responses. Update the
table above only after inspecting the new evidence.
