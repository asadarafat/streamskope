## Release highlights

Requires StreamSkope 0.11.x. Earlier hosts cannot supply the expanded protected
connection-profile model used by this plugin; keep the previous plugin with an
older host. The declared NSP target remains 26.4.0.

Updating an NSP-generated profile retains its SASL credentials, client identity
and independently configured service authentication. Connection tests use the
retained protected values when updating an existing profile.

After upgrading the host, install the compatible plugin through **Plugins**.
For an offline upgrade, download the signed portable package beforehand and
import it after the host upgrade. The bundled retrieval workflow is unchanged.
See [offline installation](https://asadarafat.github.io/streamskope/plugins/offline/).
