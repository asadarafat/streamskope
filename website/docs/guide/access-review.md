# Explain topic readers and review ACL changes

Use **Access Control Lists** after [connecting a profile](connections.md). The
connected identity needs permission to describe ACLs; reading broker configuration
adds evidence about authorization policy. Missing policy is reported as unknown.
This view does not impersonate another identity or attempt to authenticate as it.

## Explain one reader

1. Choose **Explain access**.
2. Enter a concrete topic, the exact Kafka principal such as `User:alice`, and the
   client IP address as observed by the broker. Account for principal mapping and
   NAT yourself; StreamSkope does not infer these from a login name or profile.
3. Choose **Explain access** in the dialog.
4. Read the ACL decision, effective topic READ result, matching bindings and
   broker-policy evidence together.

The model evaluates Kafka topic `LITERAL` and `PREFIXED` bindings, literal `*`
resource names, `User:*` principals, wildcard hosts, and `READ`/`ALL` operations.
A matching DENY takes precedence over ALLOW for a non-superuser. The no-ACL
default applies only when **no ACL matches the topic resource**, even if the
existing bindings concern another principal or operation.

Effective access is calculated only from observed Apache Kafka
`StandardAuthorizer` policy. A configured superuser bypasses ACL denials. Missing
or redacted policy, custom authorizers and inconsistent broker results can make
effective access **unknown**. An ACL allow can still be explained when the
superuser list is unknown, provided every observed broker uses StandardAuthorizer.
See [Kafka's authorization model](https://kafka.apache.org/41/security/authorization-and-acls/)
for the underlying semantics.

**This is a snapshot of topic READ for the supplied identity and address.** It does
not enumerate all users, verify authentication or external RBAC, evaluate consumer
group permissions, or guarantee a successful end-to-end read. Validate the actual
client workflow separately.

## Preview and apply an exact binding

1. Choose **Create ACL** and fill the binding, or choose **Delete** on an existing
   row. No mutation is sent at this step.
2. For a topic binding, supply one concrete topic reader covered by the binding.
   Prefix and wildcard changes may affect many additional topics and readers;
   the example does not represent their complete impact.
3. Choose **Preview ACL change**. Review binding presence before/after and the
   supported topic READ implications. Other resource types show exact bindings
   with an explicit unmodeled-access notice.
4. Type the displayed **Exact change confirmation**, including the action and
   complete binding identity, then choose **Apply reviewed ACL change**.
5. Inspect the outcome, refreshed inventory and the intended client's behavior.

The two-minute plan is tied to its connection and exact input. The host compares
the complete bounded ACL inventory and visible broker policy again before sending
one exact create/delete request. Changed or unreadable baselines require another
review. The limit is 10,000 bindings and 2 MiB of inventory text; an over-limit
inventory prevents review rather than silently excluding policy. Policy inspection
covers up to 32 brokers. At most 50 matching bindings are displayed; evaluation
still includes every binding in the accepted inventory.

Read-only mode permits review and blocks application. This is a local confirmation
step, not a multi-user approval workflow. Kafka has no atomic compare-and-change
ACL API: another administrator can race the final check, and unobservable policy
changes cannot be detected.

| Outcome                   | Meaning and next action                                                                                                                            |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Acknowledged, verified    | Kafka accepted the exact change and read-back matched, or the binding already had the reviewed desired state. Test the intended access separately. |
| Acknowledged, unavailable | The change was accepted but verification failed. Refresh inventory; do not resend to retry the read-back.                                          |
| Rejected                  | The baseline changed, could not be rechecked, or Kafka explicitly denied the operation. Review the cause before creating another plan.             |
| Unknown                   | The request may have been applied. Reconcile the exact binding before another attempt.                                                             |

Repeating the same plan returns its recorded outcome without sending it again.
The UI disables application after an attempt, including a lost host response.
Raw logs record the exact approved scope and outcome. Remote policy remains
authoritative; a preview never grants additional permission.

See [security and permissions](security.md) for operation rights and
[access and transforms](governance.md) for the surrounding workflow.
