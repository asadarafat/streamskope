## Release highlights

Requires StreamSkope 0.11.x. Earlier hosts cannot supply the expanded protected
connection-profile model used by this plugin; keep the previous plugin with an
older host. The declared EDA target remains 26.8.2.

Updating an EDA-generated profile retains its SASL credentials and independently
configured service authentication. Tunnel socket failures are handled by their
owner instead of escaping as unhandled errors.

After upgrading the host, install the compatible plugin through **Plugins**.
For an offline upgrade, download the signed portable package beforehand and
import it after the host upgrade. See [offline installation](https://asadarafat.github.io/streamskope/plugins/offline/).
