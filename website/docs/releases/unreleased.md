---
title: Unreleased changes
unreleased: true
---

# Unreleased changes

- Use **NATS** for the built-in messaging-system label and describe its supported live-subscription capabilities separately. StreamSkope connects to an external server supplied in a profile; no embedded NATS server is required.
- Put external NATS connection instructions before the separate development-fixture walkthrough.
- Use **EDA Connector** / **NSP Connector** in current plugin documentation and distinguish API-assisted connection setup from temporary EDA capture. Plugin display changes ship in their own compatible releases; existing identities and lifecycle records remain unchanged.

Desktop versions are assigned during release CI. Pending plugin changes remain
in each plugin's release commentary and are released independently.
