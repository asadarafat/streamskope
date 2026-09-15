# Inspect access and transforms

Use these workflows after [connecting a profile](connections.md). Administrative
operations require the corresponding service permissions.

## Access Control Lists

1. Open **Access Control Lists** in the left navigation.
2. Find the principal and resource you want to inspect.
3. Read the resource name and pattern, operation, permission and host.
4. Compare the entries with the action that principal needs to perform.

**You should have:** the visible authorization rules for that resource.

To change an ACL, review those same fields before confirming its creation or
deletion. Then verify the intended principal's access. A successful administrative
request alone does not prove that the desired access works. Use an isolated
cluster for access-policy experiments.

## Redpanda transforms

This workflow needs **Redpanda** and an accessible **Admin API** configured in
the connection profile.

1. Connect the configured profile and open **Transforms**.
2. Select a deployed transform.
3. Inspect its status, partition health and lag.
4. Open its recent logs to investigate a reported failure.

**You should see:** operational evidence from the deployed transform. The
workspace provides deletion with confirmation; review the selected transform
before confirming it.

Transform inspection does not deploy transformations to Apache Kafka. To check
message content locally, follow [Test a message rule](messages.md#rules-and-configuration).
