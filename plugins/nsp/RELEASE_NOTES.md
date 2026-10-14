## Release highlights

One API 4 NSP Connector package admits NSP 26.4.0 and 26.11.0. Its manifest uses
26.4.0–26.11.0 enclosing bounds for existing StreamSkope 0.11.x hosts; the plugin
also checks an exact release allowlist before creating or reusing a workflow or
refreshing a profile. Other products and unqualified patch versions are rejected.

The plugin reads the legacy version endpoint first. Only an HTTP 404 permits
fallback to the release-version endpoint used by the NSP 26.11 banner. Sign-in,
authorization, network, certificate and malformed-response failures do not
trigger fallback. The shared helper workflow, saved-profile identity and cleanup
ownership are retained.

## Install and upgrade

The plugin release publishes a publisher-signed portable `.skope-plugin`, its
manifest, bundled workflow and checksums. Use **Preferences → Plugins → Install
from file**, choose the asset whose name contains **portable**, and review its
publisher and compatibility before installing. No manual workflow upload is
required. See https://asadarafat.github.io/streamskope/plugins/offline/.

The package retains its declared StreamSkope `>=0.11.0, <0.12.0` bounds and plugin
API 4. It does not require an API 5 host. Complete pending execution cleanup before
updating; saved profiles and the immutable shared helper are retained. The Kafka
reader-preparation fix belongs to the StreamSkope app and is not delivered by a
plugin update.

## Qualification limits

NSP 26.4.0 build 200 has prior source-host lifecycle evidence. On NSP 26.11.0
build 113, version reading, helper reuse, combined trust retrieval and execution
cleanup were exercised, but the external broker path from the qualification host
was unavailable. Successful topic discovery reported from another network does
not establish complete consumption, reconnect or lifecycle qualification.

Until a passing source-bound 26.11 live receipt is recorded, publish this change
only as a prerelease for manual testing. Packaging and fixture checks do not
establish live integration, installed macOS/Windows behavior or managed-service
support. The 26.11 banner endpoint is internal and may change in a later NSP build.
