# Architecture and change boundaries

StreamSkope has two built-in messaging providers, Kafka and NATS. They share the
product frame, connection-profile catalog and host transport infrastructure;
each provider keeps its own wire contracts, application state and protocol
adapter. EDA and NSP are downloadable **Kafka connection plugins**, not messaging
providers. This distinction is the starting point for an extension.

## Read the repository by responsibility

| Owner                     | Responsibility                                                               | Start here                                                                                                                                                                         |
| ------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Product composition       | Shared navigation, profiles and active provider lifetime                     | [ProviderApplication](../src/platform/ui/ProviderApplication.tsx), [provider workspace ports](../src/platform/ui/provider-workspaces.ts)                                           |
| Kafka feature             | Typed commands, feature services, Kafka adapter and UI                       | [Kafka contracts](../src/features/kafka/contracts/index.ts), [facade](../src/features/kafka/facade/facade.ts), [application session](../src/features/kafka/application/session.ts) |
| NATS feature              | Independent profiles, connection and bounded live subscriptions              | [NATS contracts](../src/features/nats/contracts/index.ts), [session](../src/features/nats/application/session.ts), [engine](../src/features/nats/engine/engine.ts)                 |
| Shared Node host          | Backend composition, provider routing, files, vault and plugin runtime       | [Kafka backend](../src/platform/node/kafka-backend.ts), [NATS backend](../src/platform/node/nats-backend.ts), [provider registry](../src/platform/node/provider-host.ts)           |
| Host-specific integration | Electron windows/preload/IPC; production browser HTTP/vault; source dev host | [Electron entry](../src/platform/electron/main/electron-entry.ts), [browser entry](../src/platform/node/browser-entry.ts), [development entry](../tools/dev/start.ts)              |
| Optional plugins          | Separately built backend and renderer, declared resources and compatibility  | [plugin API](../src/plugins/api.ts), [renderer API](../src/plugins/renderer-api.ts), [EDA](../plugins/eda), [NSP](../plugins/nsp)                                                  |

A typical feature request travels through these boundaries:

```text
feature UI
  -> typed host command / exact parser
  -> IPC or HTTP provider route
  -> feature facade + access protection
  -> application service + session-owned authority
  -> engine / external protocol client
  <- correlated response or bounded provider event
```

Application code depends on contracts and its own ports, rather than importing
Electron or concrete engine implementations. Engines implement application ports;
platform composition supplies the concrete owners. Shared Node modules do not
import Electron code. The [import boundary rules](../eslint.config.mjs) and
[architecture tests](../test/architecture/import-boundaries.test.ts) enforce the
actual allowed dependencies; this diagram is a reading guide, not a replacement
for those rules.

[Source ownership](../test/architecture/source-ownership.test.ts) follows imports
from real renderer, host, worker and separately built plugin entrypoints. Test-only
imports and disconnected import cycles do not demonstrate production reachability.
Generated output belongs under build directories; EDA catalog publication has its
own generated branch, separate from development source.

## Trace one reviewed ACL change

Read this path before changing shared orchestration:

1. [AclReviewDialog](../src/features/kafka/ui/AclReviewDialog.tsx) requests
   `acls.change.review`, shows the plan and sends `acls.change.apply` with the exact
   confirmation. [use-host-command](../src/features/kafka/ui/use-host-command.ts)
   owns the UI request boundary.
2. [ACL command declarations](../src/features/kafka/contracts/acl-review-commands.ts)
   define the three commands' payload/result parsers and read/write classification.
   Central command registration derives from those declarations. This is a bounded
   pilot; other command families still use their existing registration.
3. [Command protection](../src/features/kafka/facade/command-protection.ts) checks
   access before dispatch. [AclReviewFacade](../src/features/kafka/facade/acl-review-facade.ts)
   dispatches through a typed handler table, validates the result envelope and
   attaches the host's correlation ID. It records the resulting Activity entry.
4. [AclReviewService](../src/features/kafka/application/acl-review-service.ts) owns
   the plan lifetime, baseline recheck, confirmation and acknowledged/uncertain
   outcome. It receives an ACL scope, not the raw broker client.
5. [KafkaConnectionScopes](../src/features/kafka/application/connection-scope.ts)
   fences reads and admits each mutation synchronously against the current
   connection. The [engine](../src/features/kafka/engine/engine.ts) delegates the
   actual operation to the [Kafka admin adapter](../src/features/kafka/engine/platformatic-admin.ts).

Start verification with the [wire contract](../test/unit/acl-review-command-contract.test.ts),
[composed facade](../test/unit/acl-review-facade.test.ts),
[plan behavior](../test/unit/acl-review.test.ts) and
[scope races](../test/unit/connection-scopes.test.ts). The
[real ACL suite](../test/kafka/acl-review-real.test.ts) provides separate broker
acceptance; mocked admission alone does not prove broker permissions or side effects.

## Preserve ownership through cancellation

The session owns connections and consumer lifecycles.
[KafkaSessionRequests](../src/features/kafka/application/session-requests.ts) owns
request supersession, cancellation and stale-response checks. Feature services
should receive concrete capabilities exposing only their needed operations.

Reviewed writes, batches, replay, ACL changes, offset resets and observations use
session scopes. Read scopes fence before and after awaited work. A mutation checks
authority synchronously at dispatch: a rejected admission stays unsent; an admitted
attempt retains its eventual acknowledgement or uncertainty even after disconnect.
Cancellation cannot turn a begun network operation into proof that nothing happened.
Review confirmation and expiry stay with the feature that owns the plan.

Saved replay destinations own a narrow reviewed scope and a coalesced close.
Closing revokes new work; cleanup failures remain observable. Observation sampling
owns its bounded record reader. Connect, environment comparison and other legacy
context consumers have not all migrated to these scopes. Extend them deliberately,
with evidence for cancellation, stale results and begun writes, rather than exposing
an unrestricted connection under a new interface name.

Connection-bound features register invalidation and drain together in
[FeatureLifecycle](../src/features/kafka/facade/feature-lifecycle.ts). Connection
changes invalidate synchronously. Shutdown attempts every owner and waits for
cleanup, even after a sibling fails. Consumption retains its final flush. Preserve
the host's admission/cleanup ordering: plugin cleanup can still need its owned host
commands before the final command drain completes.

## Trace a finite record export

[RecordExportService](../src/features/kafka/application/record-export-service.ts)
owns the captured query, bounded read passes, accepted-row acknowledgements and
cleanup through a narrow session read scope and artifact sink. It receives every
late-opened reader before checking authority, so cancellation cannot lose the
handle that must be closed. A user cancellation may seal a verified partial file;
connection changes and host lock revoke file access immediately. Replacement
waits for cleanup, and unresolved cleanup remains visible.

Commands and events carry metadata only. The Node factory supplies one
[encrypted temporary artifact owner](../src/platform/node/record-export-artifacts.ts)
to the application and a separate, host-only delivery port to each host. Browser
downloads use an authenticated same-origin attachment route; native Save accepts
an opaque artifact reference, rechecks authority after the file dialog and streams
to a private sibling file before rename. Artifact bytes never enter provider RPC
or a renderer Blob. The pure [cross-host file contracts](../src/platform/desktop/contracts.ts)
can be shared by Node and renderer code without importing Electron.

Start changes with the [protocol tests](../test/unit/record-export-contract.test.ts),
[application fault tests](../test/unit/record-export-service.test.ts) and
[real artifact tests](../test/integration/record-export-artifacts.test.ts).
The [real broker export](../test/kafka/streaming-export-real.test.ts),
[production browser download](../test/e2e/web-streaming-export.spec.ts) and
[native Save journey](../test/e2e/electron-record-export.spec.ts) establish separate
reader, authorization and delivery evidence. This export is transient; it does
not introduce a durable job journal or restart resume.

## Messaging providers and connection plugins

The [provider registry](../src/platform/node/provider-host.ts) seals each route with
its provider's command, response and event codecs. Unknown routes do not fall back
to Kafka. Each provider owns its event stream, readiness and delivery queue; a failed
stream cannot mark its sibling unavailable. The registry closes admission
synchronously and shares one shutdown barrier across providers.

The shared Connection Profiles screen exposes safe summaries. Credentials and
protocol configuration stay with the provider. Connecting a profile activates that
provider's workspace only after the previous workspace confirms cleanup. Failed
cleanup keeps the current workspace available for retry. Profile-management lifetime
is separate from stream authority; a retired renderer cannot acquire new work.

Kafka's [query connection handoff](../src/features/kafka/ui/query-connection-handoff.ts)
retains one validated settings intent for an explicitly selected profile. After
confirmed cleanup and connection, the new activation restores its topic, bounds,
limit and filters without starting a read. Other profiles, provider changes and
explicit disconnect discard the intent. Only settings cross this transition;
credentials, messages and the old activation's callbacks remain with their owners.

After the original host confirms disconnect, Kafka invalidates the catalog's cached
connection and cluster evidence before replacing the workspace. IPC event delivery
can follow the command response, so the new activation must not start from an old
connected snapshot. Event sequence watermarks survive invalidation and continue to
reject stale replays.

NATS is an independent sibling under `src/features/nats`. It inspects the configured
remote NATS server; it is not StreamSkope's internal bus. Its implemented protocol
is live core subscription traffic, with bounded payloads and token/TLS support,
explicit reconnection and broker-confirmed SUB/UNSUB. It does not provide JetStream
history, subject inventory or replay. Host-lifetime control revisions, profile
revisions and message-delivery counters have separate meanings.

To add a messaging provider, follow the NATS composition through its own contracts,
application, engine, facade, profile facet, host endpoint and renderer transport.
Register it in each supported host and prove isolated readiness, request correlation,
handoff and cleanup. The existing provider infrastructure is a seam for built-in
siblings; it is not a promise that arbitrary providers can be downloaded through
the current plugin installer. A new provider still needs its actual protocol,
capability model, UI and real-server acceptance.

EDA and NSP instead extend Kafka profile creation through the
[plugin contracts](../src/plugins/contracts.ts). Core source cannot import their
implementation bundles. The installed backend performs platform-specific discovery
and owned work; the renderer contributes the corresponding UI. The host supplies
bounded capabilities and verifies declared package resources/compatibility. Keep
platform-specific API or workflow behavior in its plugin rather than adding another
platform branch to the Kafka engine.

## Plugin lifecycle is a transaction boundary

[Installer](../src/platform/node/plugins/installer.ts) owns package acquisition,
verification, review and consent. [Store](../src/platform/node/plugins/store.ts)
owns retained immutable files and selected installations.
[Runtime](../src/platform/node/plugins/runtime.ts) coordinates activation and cleanup;
[transition controller](../src/platform/node/plugins/transition.ts) owns serialized
operations, admission, phases and diagnostic watchdogs. The
[inventory projection](../src/platform/node/plugins/runtime-inventory.ts) allows
status reads without waiting for a stalled backend hook.

- Bind each change to reviewed bytes, plugin identity, activation and current work.
  Revalidate consent at the authority boundary; a stale dialog is not permission
  to replace newer work.
- Queued operations do not block dispatch until their mutation starts. Review
  hooks preserve recovery commands. A watchdog can report waiting but cannot
  resolve the operation, release its barrier, force deletion or activate a sibling.
- Respect the storage commit boundary. Failure before commit keeps the old
  selection. Retirement failure after commit must report the actual selection and
  retain recovery evidence, not claim the previous installation is still active.
- Renderer lifetimes depend on activation identity, not progress revisions.
  Retired callbacks cannot operate a replacement; progress must not remount or
  unload a healthy plugin.
- Successful removal retains saved profile metadata. Platform-owned resource
  cleanup and local installation removal are distinct outcomes; preserve uncertain
  cleanup for an explicit retry.

Full startup still waits for backend activation; an available inventory does not
mean the host is ready. The [runtime transition tests](../test/unit/plugin-runtime-transitions.test.ts)
and [controller tests](../test/unit/plugin-transition.test.ts) cover waiting,
late completion and authority; the [renderer tests](../test/unit/plugin-transition-status.test.tsx)
cover visible status. [Source plugin development](development.md#build-source-and-test-a-development-plugin)
and [live qualification](qualification.md) own execution procedures.

Host protocol compatibility and plugin API compatibility are separate. Change
paired host/preload/renderer codecs together when their wire contract changes;
follow the actual strict parsers instead of assuming an optional field is compatible
with an older exact-key parser. Plugin manifests declare their own API and desktop
interval. See [release/version rules](releases.md) before changing either boundary.

## Investigation library ownership

Saved views and local topic annotations share one bounded document and one
[serialized application owner](../src/features/kafka/application/query-library.ts).
A mutation reads the entire current state, compares expected annotations, validates
capacity and commits atomically without dropping the other feature's data.
Legacy reads are pure; a real mutation preserves the actual predecessor before
writing format 4. Browser preflight and maintenance inspect the same format and
backup families.

[Topic catalog scope](../src/features/kafka/application/connection-scope.ts) exposes
metadata-only identity lookup from one captured connection. The
[catalog service](../src/features/kafka/application/topic-catalog-service.ts)
rechecks the broker's cluster/topic UUID and connection generation before local
commit admission. A later disconnect cannot erase an admitted disk-write receipt.
Listing and removing orphaned notes need no broker connection; names alone never
establish identity.

[Portable-view contracts](../src/features/kafka/contracts/view-transfer.ts) strip
local identities and contain no record bodies, credentials or catalog notes.
Parsing and review do not create IDs or dispatch host operations. Explicit opening
creates new transient bookmark IDs and uses the existing passive restore/confirmed
stop path; only Save changes durable storage. Host protocol 61 adds the catalog
commands; older paired hosts/renderers are rejected by the strict protocol parser.
Plugin API compatibility is unchanged.

## Choose a safe first change

| Change                                          | Review focus and evidence                                                                                                                                                                        |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Bounded feature view or pure parser/calculation | Its owner and focused tests; shared shell/navigation only when affected                                                                                                                          |
| Host command or access policy                   | Typed contract, exact parser, correlated response and composed protection tests                                                                                                                  |
| Session admission, queues or cancellation       | [Scope tests](../test/unit/connection-scopes.test.ts), [lifecycle tests](../test/unit/feature-lifecycle.test.ts), actual begun-operation/cleanup outcomes                                        |
| Plugin runtime, installer or persisted recovery | [Runtime lifecycle](../test/unit/plugin-runtime-lifecycle.test.ts), signed package/resource checks, affected live target cleanup                                                                 |
| Vault, storage or browser maintenance           | [Vault owner](../src/platform/node/vault/passphrase-vault.ts), [maintenance engine](../tools/package/browser-maintenance.py), interrupted transaction tests and native upgrade/rollback evidence |
| Release identity, package assembly or installer | [Release runbook](releases.md), immutable source/payload binding and actual native delivery evidence                                                                                             |

Finite [operational diagnostics](../src/platform/diagnostics.ts) identify the real
owner and stage and retain correlation. Do not feed arbitrary upstream errors,
credentials or stack traces into their safe Activity projection. A diagnostic is
not permission to release another owner's resources.

Read one complete feature trace, identify its state owner and failure/cleanup
boundary, then make a small reviewed change. Use [qualification](qualification.md)
to choose additional acceptance evidence. A passing synthetic pipeline soak does
not establish network, renderer or installed-upgrade endurance.

## Structured repair ownership

The [structured replay service](../src/features/kafka/application/structured-replay-service.ts)
decodes immutable original bytes with the same canonical decoder as inspection,
applies bounded declarative JSON Pointer edits and encodes with the shared schema
authoring worker. Saved destinations capture Registry, read and reviewed-write
authority from the same isolated connection. ID equality across Registries is
never writer identity. Each record's dispatch rechecks the pinned destination
subject/version, ID and complete bounded reference graph fingerprint.

Missing writers use the existing reviewed schema-change owner through an explicit
destination-profile registration journey; replay review never registers schemas.
Continuation uses frozen output bytes and writer evidence, without decoding or
re-encoding source records. It retains prior uncertainty and original cleanup
ownership. See [repair operation](../website/docs/guide/record-replay.md).

Host protocol 71 includes explicit Connect configuration set/removal reviews and closed topic, consumer-group and client-quota administration reviews/outcomes alongside
the structured transform/evidence contracts; older peers are refused. Protected journal format 3 accepts read-only legacy formats
1/2, preserves the exact encrypted predecessor on the first explicit mutation,
and refuses structured evidence mislabeled as a legacy document. Browser
inspection and maintenance recognize both encrypted predecessor generations;
old installed format2 hosts refuse format3 before stopping their current owner.

Topic administration reuses the bounded connection-pinned review owner. Its
dedicated adapter owns admitted clients through acknowledgement and confirmed
cleanup, joins them on connection close and fences new work after unresolved
cleanup. Deletion uses the SDK's public UUID-based wire API; expansion uses Kafka's
name-based API after fresh identity/assignment checks, with its concurrent
replacement limitation explicit. Readback never invents an acknowledgement.

Consumer-group resets resolve bounded earliest/end/time selectors through the
broker, freeze explicit offsets, and pin cluster and topic UUIDs. Examples use the
same record preparation and protection path as grid records. Inactive deletion
pins the group's complete bounded committed-offset fingerprint; groups have no
UUID, so identical concurrent recreation remains a documented limit. Each
review/apply adapter uses `OwnedKafkaResources` to drain original clients and
preserve admitted replies through revocation. ACK, readback and cleanup remain
separate; failed cleanup fences the owner. No durable format or plugin API changes
are introduced by these protocol commands.

Client quotas inspect one strict, explicit user/client-ID entity, including actual
default entries. The dedicated service freezes the complete explicit baseline,
preserves untouched keys and uses the same captured connection and review owner.
The adapter revalidates cluster/entity/API/value evidence on its mutation client,
interprets the SDK's exact per-entity receipt and drains original clients through
`OwnedKafkaResources`. An ACK does not imply verified readback or confirmed cleanup.
Effective and inherited quotas are not inferred, and Kafka has no atomic quota
compare-and-set. See [client quota operation](../website/docs/guide/client-quotas.md).

Connect configuration editing accepts independent bounded set and removal deltas.
Only the host merges the actual complete configuration; omitted credentials stay
protected. Reviews include the connection name and exact field lists and compare
canonical config/task state before one attempt. The shared reply-correlation owner
checks command identity and workflow-specific scope. The renderer fences stale
connection responses and requires explicit receipt dismissal before another edit.
Actual HTTP acknowledgement and asynchronous worker state remain distinct; original
HTTP request drain and connector-offset recovery are subsequent work.
