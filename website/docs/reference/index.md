# Settings, formats and limits

Use this section to look up supported behavior and boundaries. To connect and
read records, use [Everyday tasks](../guide/index.md). For diagnosis and recovery
workflows, use [Under the Skope](../skope/index.md).

| Information                                                 | Reference                                           |
| ----------------------------------------------------------- | --------------------------------------------------- |
| Providers, tested baselines and support boundaries          | [Compatibility](../start/compatibility.md)          |
| Encoding detection, writer formats and decoding bounds      | [Record formats](record-formats.md)                 |
| Replay fidelity, limits, outcomes and retry semantics       | [Replay behavior](replay.md)                        |
| Broker authentication and trust ownership for each endpoint | [Connection security](connection-security.md)       |
| Access requirements and read-only behavior                  | [Permissions and security](../guide/security.md)    |
| Stored data, exports, masking and message limits            | [Data and storage](../guide/data-handling.md)       |
| Recorded qualification and unverified scope                 | [Qualification evidence](../guide/qualification.md) |
| Plugin host/target compatibility and package identity       | [Plugin versioning](../plugins/versioning.md)       |

## Deployment and recovery information

The [stored-data inventory](../guide/data-handling.md#stored-data) and
[browser data inventory](../guide/data-handling.md#production-browser-data)
describe files and protection boundaries. Restore and upgrade instructions belong
to [desktop recovery](../guide/recovery.md) and
[browser host recovery](../guide/browser-host.md#back-up-and-restore).

For offline installation, custom networking and host maintenance, follow the
[manual deployment guide](../guide/browser-deployment.md). For development and
command-line procedures, use the [read-only CLI](../guide/read-only-cli.md),
[source workbench](../start/development.md) or
[consumer sandbox](../guide/developer-sandbox.md).
Related Under the Skope workflows also link to these maintained guides where
they are needed.
