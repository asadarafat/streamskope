# Match the guide to your release

| Desktop release                     | App/installer version | Contents                                                                                             |
| ----------------------------------- | --------------------- | ---------------------------------------------------------------------------------------------------- |
| [v0.1.0+build.1](v0.1.0+build.1.md) | 0.1.0                 | First release: Kafka workbench, hot plugin lifecycle, EDA Capture and NSP Capture using plugin API 3 |

The first release is being prepared. Consult
[GitHub Releases](https://github.com/asadarafat/streamskope/releases) for published
assets; a source tag or this page alone does not establish download availability.

Record the full download release tag as well as the app version. Build metadata
can distinguish releases that share app versions and installer filenames. Match
the installer against `SHA256SUMS` from its own GitHub release. If an existing
installation's build is unknown, preserve a full backup before replacing it.

The notice on each guide identifies its documented desktop build and links to
plugin requirements. **Development documentation** comes from a checkout or PR
and may contain changes not yet published. **Published documentation** is the
qualified site built from current `main`, with release availability verified before
deployment.

After the first desktop release is published, merging documentation changes to
`main` qualifies and publishes Pages without another desktop release. Maintainers
can also run **Pages** manually from `main`. Desktop release publication triggers
the same current-main build; an old release event cannot replace the site with an
old checkout. EDA plugin and signed cluster-app publication remain separate.

Before each later desktop release, retain existing release pages, add the exact
new tag's notes, update the documented release in the website configuration, and
review the [compatibility matrix](../start/compatibility.md) and
[qualification record](../guide/qualification.md). Record qualification only after
inspecting evidence for the exact source and environment.
