## Signed portable installation

Download the signed portable asset from this plugin release for **Preferences →
Plugins → Install from file** on a supported desktop. It contains the same code
and resources as the primary catalog package and is authenticated by the
publisher key shipped with StreamSkope. Desktop installer releases do not carry
these plugin downloads.

## Upgrade and compatibility

This plugin uses API 4 and independent Semantic Versioning. Install it on a
StreamSkope desktop within the compatibility interval below. The plugin checks
the running EDA version through its API before setup. The EDA cluster application
keeps its own target-aligned version.

Back up application data before upgrading from an API 2 or API 3 plugin. See
[plugin migration](https://asadarafat.github.io/streamskope/plugins/versioning/)
for compatibility and rollback instructions.

## Known limitations

Release CI does not have access to a live EDA cluster. Review the recorded
[qualification evidence](https://asadarafat.github.io/streamskope/guide/qualification/)
before rollout; successful packaging does not establish live capture or cleanup
qualification for the release source.
