# StreamSkope Capture for Nokia EDA

This directory is the authoritative EDABuilder project source for the
StreamSkope Capture application. It supplies the EDA-managed control plane used
to create bounded, temporary Kafka capture sessions for StreamSkope.

Development, validation and release tooling consumes the physical path
`vendors/streamskope/apps/capture`. The published Git tree must not include a
provider-navigation symlink to this directory: EDA traverses catalog trees and
cannot resolve the link outside its virtual root. EDABuilder may create a
generated projection at `apps/capture.streamskope.io/manifest.yaml` on a
publication branch, but EDA discovers this app from the canonical `vendors`
path. Generated catalog manifests and signatures are excluded from main and
development source; they are not a second editable source.

## Publication contract

An installable release includes:

- `streamskope-eda-app`, the signed OCI bundle installed by the EDA App Store;
- its bundled in-cluster service, which manages private capture sessions and
  their cleanup (built locally, not published as a separate package);
- the EDABuilder publication tag; and
- the EDA Store version tag
  `vendors/streamskope/apps/capture/<target-eda-version>` pointing to the same
  release commit.

The application version is available to StreamSkope only after release
verification confirms both artifacts. Directory contents on an untagged commit
are not an installable release.

The app version matches the full target EDA version exactly: EDA 26.8.2 uses
app v26.8.2. The manifest, bundled agent and desktop contract share this version.
App fixes do not introduce separate patch numbers or suffixes. Existing published
versions are not overwritten by the release workflow.

## Compatibility

- Application API: `capture.streamskope.io/v1alpha1`
- EDA Core API: `v6.0.0`
- Catalog: `https://github.com/asadarafat/streamskope.git`
- OCI registry: `ghcr.io/asadarafat`

The EDA administrator must trust the StreamSkope application signing public key
before installing a production release. StreamSkope never disables EDA digest,
signature, registry TLS, requirement, or authorization checks.

## Runtime contract

EDA exposes the installed agent through the authenticated `streamskope-capture`
HTTP proxy. The desktop creates one `CaptureSession`, renews its finite lease while
capture is active, and stops it explicitly when the user ends capture. The agent
creates a private single-node broker and a copied exporter resource; it never
modifies the selected exporter. The Kafka tunnel is bounded to eight simultaneous
connections and 16 MiB WebSocket messages.

The lease is the recovery boundary. If the desktop disappears, the in-cluster
reconciler removes the copied exporter, StatefulSet, ClusterIP Service, and ephemeral
broker storage after expiry. Cleanup refuses resources that do not carry the exact
session ownership labels. Explicit stop first expires the session so retries
cannot recreate resources that are being deleted.
Repeating removal succeeds when the agent confirms that the session is already
absent, allowing desktop recovery after expiry or a lost removal response.
