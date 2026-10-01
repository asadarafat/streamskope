# StreamSkope Capture

StreamSkope Capture creates temporary, explicitly authorized Kafka capture
sessions for Nokia EDA sources. StreamSkope connects only after the application
reports that the selected source and bounded capture endpoint are ready.

Capture sessions have a finite lease. Stopping a session or allowing its lease
to expire removes only the broker, exporter copy, service, and storage owned by
that session. The selected source exporter is not modified or deleted.

See [Support](SUPPORT.md) for safe diagnostic reporting.
