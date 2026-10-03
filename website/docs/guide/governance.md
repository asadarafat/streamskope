# Inspect access and transforms

Use these workflows after [connecting a profile](connections.md). Administrative
operations require the corresponding service permissions. Review the
[workflow access and effects table](security.md#kafka-access-and-effects) before
granting configuration, ACL or service mutation rights.

## Access Control Lists

1. Open **Access Control Lists** in the left navigation.
2. Find the principal and resource you want to inspect.
3. Read the resource name and pattern, operation, permission and host.
4. Choose **Explain access** for a concrete topic, principal and broker-observed
   client IP. Inspect supported READ decisions and any unknown policy.

**You should have:** the visible authorization rules for that resource.

To change an ACL, preview the exact binding and its supported before/after impact,
then confirm the full change identity. Follow [access review](access-review.md)
for stale plans, permission requirements and unknown outcomes. Verify the intended principal's access. A successful administrative
request alone does not prove that the desired access works. Use an isolated
cluster for access-policy experiments.

Creation and deletion require the action and complete binding identity, including
pattern, principal, host, operation and permission. After a change, StreamSkope reconciles that exact
binding with refreshed inventory. If the broker acknowledged but the refresh
failed, Activity records the acknowledgement and a warning. Refresh the inventory
without repeating the mutation. An unacknowledged change may have reached Kafka;
inspect the exact binding before another attempt. Authorization failures remain
separate from an empty ACL inventory.

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
