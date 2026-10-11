# StreamSkope Capture

StreamSkope Capture creates temporary, explicitly authorized Kafka capture
sessions for Nokia EDA sources. StreamSkope connects only after the application
reports that the selected source and bounded capture endpoint are ready.

Capture sessions have a finite lease. Stop or expiry asks the running application
reconciler to remove the broker, exporter copy, service and storage owned by that
session. Elapsed time alone does not prove deletion: confirm the session status
and owned-resource absence, especially after cluster or application failure.
The selected source exporter is not modified or deleted.

See [Support](SUPPORT.md) for safe diagnostic reporting.
