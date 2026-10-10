---
title: Inspect and change client quotas
description: Inspect exact Kafka quota entities and apply reviewed configuration changes with explicit receipts.
---

# Inspect and change client quotas

Use **Overview → Client quotas…** to inspect the explicit Kafka quota settings for
one user, client ID or user/client-ID pair. Connect to the intended cluster first.

The view reads configuration, not traffic or actual throttle duration. It does
not calculate effective limits from Kafka's quota precedence rules. A successful
empty response means this exact entity has no explicit entries; it does not mean
the client is unlimited. [Kafka's quota documentation](https://kafka.apache.org/43/operations/basic-kafka-operations/#setting-quotas)
explains named and default entries.

## Inspect the exact entity

1. Select **Include user**, **Include client ID**, or both.
2. Enter the exact principal/client-ID name for each selected dimension. To read
   an actual default entry, select **Default user entry** or **Default client-ID
   entry** explicitly. A name containing `(default)` is still a named entity.
3. Select **Inspect exact quotas**. Check the displayed entity and cluster ID.

The query is strict: a user-only query does not combine settings from separate
user/client-ID pairs. There is no wildcard or IP-quota editor. Missing permission,
an unavailable DescribeClientQuotas API, or an inconsistent response is reported
as unavailable, never as an empty configuration.

| Explicit key               | Unit                                 | Meaning                                                                                                  |
| -------------------------- | ------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| `producer_byte_rate`       | Bytes per second per broker          | Producer traffic quota                                                                                   |
| `consumer_byte_rate`       | Bytes per second per broker          | Consumer traffic quota                                                                                   |
| `request_percentage`       | Request-thread percentage per broker | Time allowed on Kafka network and request-handler threads; this is not a percentage of total cluster CPU |
| `controller_mutation_rate` | Mutations per second                 | Controller mutation quota; this is not message throughput                                                |

Unknown returned keys remain visible and are preserved; this editor changes only
the four keys listed above. The view admits at most 32 explicit keys and one or
two distinct entity dimensions.

## Review and apply

For each supported key, choose **Keep unchanged**, **Set explicit value**, or
**Remove explicit key**. Set requires a finite positive number. Byte rates require safe whole numbers. **Zero is refused
as a set value; removal is explicit.** Kafka remains authoritative for supported
values and combinations. Removing a key restores Kafka's normal quota resolution;
the resulting inherited limit is not measured here.

Select **Review quota changes** and check the before/after table. Inspection and
review do not change Kafka. Type the exact phrase shown under **Confirm exact
quota change**, then select **Apply reviewed quota changes**. Default entries
can affect many clients; check their scope before applying.

Reviews expire after two minutes. The host rechecks cluster identity, exact
entity, API support and the complete explicit baseline immediately before
admission. A changed baseline is refused as **unsent**; inspect and review again.
Kafka has no atomic compare-and-set for quotas, so another operator can still
change the same entity between that check and the broker operation.

| Receipt field                                     | What it proves                                                                                      |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `acknowledged`                                    | Kafka returned a successful receipt identifying the exact entity                                    |
| `rejected`                                        | Kafka returned an explicit error for that entity; inspect permissions and supported values          |
| `unknown`                                         | An admitted request has no usable exact receipt; its effect remains uncertain                       |
| `unsent`                                          | The operation was refused before mutation admission                                                 |
| Readback `verified` / `different` / `unavailable` | A subsequent exact read matched the expected complete values, differed, or could not establish them |
| Cleanup `confirmed` / `unresolved`                | The original operation's local client closed successfully, or cleanup could not be established      |

Kafka metadata propagation can delay readback after an ACK. Readback never
invents an ACK. Each review permits one attempt, including repeated clicks;
uncertain results are not automatically resent. Keep the receipt visible until
you have checked it, then **Close**. For an unknown result or unresolved cleanup,
inspect Activity and the exact entity before considering another review. Original
cleanup must be resolved before this connection's quota owner admits more work.

## Permissions and qualification

Quota inspection needs cluster `DescribeConfigs`; changes also need cluster
`AlterConfigs`. Successful inspection does not prove write permission. Application
read-only mode permits inspection and review but blocks apply. See [Security and
permissions](security.md).

The owning real-broker regression covers Kafka 4.3.1 named/default/combined
entities, local zero refusal, explicit removal, preserved keys, stale reviews, read-only
protection and broker authorization denial. This does not establish managed-service
compatibility or effective quota behavior; negotiate the actual broker APIs and
test the intended account in a disposable environment.
