# Qualification evidence

Use the exact desktop release and plugin identity when deciding whether a
procedure has been exercised for your environment. A passing documentation build
checks the site; it does not establish live integration, permissions or recovery.

## First-release qualification

The initial source targets **v0.1.0+build.1**, application **0.1.0**. Release
packaging and publication are pending. This page does not carry forward results
from trial releases as evidence for this release.

| Check                                                            | Required evidence                                                                          | Current record                                   |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------ |
| Static, unit, architecture, non-live integration and docs checks | Successful CI for the exact committed source                                               | To be recorded for the first release             |
| Linux x64, macOS ARM64 and Windows x64 installers                | Release CI, native launch checks and installer checksums                                   | To be recorded after packaging                   |
| 60-second performance soak                                       | Local report tied to the exact source                                                      | To be recorded for the first release             |
| EDA 26.8.2 capture and cleanup                                   | Configured local cluster, API version, received record and verified owned-resource removal | No first-release live result recorded            |
| NSP 26.4.0 setup and cleanup                                     | Configured target, API version, profile reuse, Kafka access and verified execution removal | No first-release live result recorded            |
| Native plugin dialogs and credential-backed restore              | Installed desktop, OS/architecture and real credential-service rehearsal                   | No first-release native workflow result recorded |

CI runs on pull requests, `main` and release tags. The
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

## Repeat the same-account recovery rehearsal

Use a disposable workstation account/session with an unlocked real credential
service and the [AIO development fixture](../start/development.md). Do not substitute
the deterministic encryption used by other profile tests. The rehearsal creates
its own temporary app-data directories and removes them after the test; it never
restores over your normal desktop profile. It also creates and deletes one uniquely
named fixture topic for the recent-window/export check.

After setting up the development prerequisites:

```sh
npm run build
STREAMSKOPE_NATIVE_RECOVERY=1 node tools/package/e2e.mjs electron test/e2e/electron-profile-recovery.spec.ts
```

PowerShell users can set `$env:STREAMSKOPE_NATIVE_RECOVERY = "1"` before the same
`node` command. The default fixture is `streamskope-kafka`; set
`STREAMSKOPE_TEST_FIXTURE_NAME` to another owned disposable AIO fixture when needed.
The test refuses unavailable or plaintext credential protection. Ordinary CI leaves
this rehearsal skipped unless explicitly configured; a skip is not a pass.

For each qualified platform, record OS/architecture, Electron version, credential
backend, application commit, and the successful reconnect after restoring. The
current automated scenario covers a profile with protected credentials; separate
recipes/rules/plugin rollback still require their own evidence. Before relying on
an upgrade or downgrade, also rehearse its exact source/target builds.

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
