import { createHash } from "node:crypto";

import { Admin, AclOperations, findErrorBy } from "@platformatic/kafka";

import {
  parseGroupAdministrationSnapshot,
  groupDeletionAllowed,
  sameGroupBaseline,
  type GroupAdministrationSnapshot,
  type GroupAdministrationOutcome,
} from "../contracts/group-administration";

import { platformaticClientOptions } from "./platformatic-options";
import { requireConsumerGroupProtocol } from "./platformatic-group-protocol";
import { OwnedKafkaResources } from "./owned-kafka-resources";
import type { KafkaClientInput } from "./types";

export class PlatformaticGroupAdministration {
  private readonly resources: OwnedKafkaResources;
  constructor(
    private readonly input: KafkaClientInput,
    private readonly lifetime: AbortSignal,
  ) {
    this.resources = new OwnedKafkaResources(lifetime);
  }
  private owned<T>(run: (admin: Admin) => Promise<T>): Promise<{ value: T; cleaned: boolean }> {
    return this.resources.run(
      () => new Admin(platformaticClientOptions(this.input, "streamskope-group-administration")),
      run,
    );
  }
  close(): Promise<void> {
    return this.resources.close();
  }
  private async inspect(admin: Admin, groupId: string): Promise<GroupAdministrationSnapshot> {
    const groupBase = await requireConsumerGroupProtocol(admin, groupId);
    const [metadata, apis, groups, offsets] = await Promise.all([
      admin.metadata({ topics: [], forceUpdate: true, autocreateTopics: false }),
      admin.listApis(),
      admin.describeGroups({ groups: [groupId], includeAuthorizedOperations: true }),
      admin.listConsumerGroupOffsets({ groups: [groupId], requireStable: false }),
    ]);
    const group = groups.get(groupId),
      positions = offsets.find((g) => g.groupId === groupId);
    if (!group || group.error || !positions)
      throw new Error("Complete group identity and committed offsets are unavailable.");
    const entries = positions.topics.flatMap((t) =>
      t.partitions.map((p) => [t.name, p.partitionIndex, p.committedOffset.toString()] as const),
    );
    if (entries.length > 4096)
      throw new Error("Group deletion review is bounded to 4096 committed partitions.");
    entries.sort((a, b) => a[0].localeCompare(b[0], "en-US") || a[1] - b[1]);
    return parseGroupAdministrationSnapshot({
      groupId,
      clusterId: metadata.id,
      protocolType: groupBase.protocolType,
      state: group.state,
      members: group.members.size,
      offsetsSha256: createHash("sha256").update(JSON.stringify(entries)).digest("hex"),
      deletePermission:
        group.authorizedOperations < 0
          ? "unknown"
          : (group.authorizedOperations & (1 << AclOperations.DELETE)) !== 0
            ? "allowed"
            : "denied",
      deleteSupported: apis.some((a) => a.apiKey === 42),
    });
  }
  async snapshot(groupId: string): Promise<GroupAdministrationSnapshot> {
    const { value, cleaned } = await this.owned((admin) => this.inspect(admin, groupId));
    if (!cleaned) throw new Error("Group inspection cleanup is unresolved.");
    this.lifetime.throwIfAborted();
    return value;
  }
  async apply(baseline: GroupAdministrationSnapshot): Promise<GroupAdministrationOutcome> {
    if (!this.resources.available)
      return {
        groupId: baseline.groupId,
        state: "unsent",
        verification: "unavailable",
        cleanup: this.resources.cleanupUnresolved ? "unresolved" : "confirmed",
        detail:
          "The connection is revoked or original cleanup remains unresolved. No mutation was sent.",
      };
    const { value, cleaned } = await this.owned(async (admin) => {
      let dispatched = false;
      let result: GroupAdministrationOutcome = {
        groupId: baseline.groupId,
        state: "unsent",
        verification: "unavailable",
        cleanup: "confirmed",
        detail: "Group revalidation failed before admission. Review again.",
      };
      try {
        const fresh = await this.inspect(admin, baseline.groupId);
        if (
          this.lifetime.aborted ||
          !sameGroupBaseline(baseline, fresh) ||
          !groupDeletionAllowed(fresh)
        )
          return {
            ...result,
            detail:
              "Group identity, inactivity, offsets, permissions or connection changed. No deletion was sent; review again.",
          };
        dispatched = true;
        await admin.deleteGroups({ groups: [baseline.groupId] });
        result = {
          ...result,
          state: "acknowledged",
          detail:
            "Kafka acknowledged group deletion. Refresh and inspect the result before another operation.",
        };
        if (!this.lifetime.aborted)
          try {
            const group = (
              await admin.describeGroups({
                groups: [baseline.groupId],
                includeAuthorizedOperations: true,
              })
            ).get(baseline.groupId);
            if (group?.error) throw group.error;
            if (group) {
              if (["Dead", "DEAD"].includes(group.state) && group.members.size === 0) {
                const positions = (
                  await admin.listConsumerGroupOffsets({
                    groups: [baseline.groupId],
                    requireStable: false,
                  })
                ).find((g) => g.groupId === baseline.groupId);
                if (positions)
                  result = {
                    ...result,
                    verification: positions.topics.every((t) =>
                      t.partitions.every((p) => p.committedOffset < 0n),
                    )
                      ? "verified"
                      : "different",
                    detail:
                      "Kafka acknowledged deletion. Direct coordinator readback reports a dead group with no members; committed-offset readback establishes whether offsets remain.",
                  };
              } else result = { ...result, verification: "different" };
            }
          } catch (error) {
            if (
              findErrorBy(
                error instanceof Error ? error : undefined,
                "apiId",
                "GROUP_ID_NOT_FOUND",
              ) !== null
            )
              result = {
                ...result,
                verification: "verified",
                detail:
                  "Kafka acknowledged deletion; direct group readback returned GROUP_ID_NOT_FOUND.",
              };
          }
      } catch (error) {
        if (result.state !== "acknowledged") {
          const rejected = [
            "GROUP_AUTHORIZATION_FAILED",
            "CLUSTER_AUTHORIZATION_FAILED",
            "NON_EMPTY_GROUP",
            "GROUP_ID_NOT_FOUND",
            "INVALID_GROUP_ID",
            "INVALID_REQUEST",
          ].some(
            (id) => findErrorBy(error instanceof Error ? error : undefined, "apiId", id) !== null,
          );
          result = {
            ...result,
            state: !dispatched ? "unsent" : rejected ? "rejected" : "unknown",
            detail: !dispatched
              ? result.detail
              : rejected
                ? "Kafka rejected group deletion. Inspect the group and permissions before reviewing again."
                : "Deletion may have reached Kafka. Inspect the exact group before another attempt; no automatic retry was made.",
          };
        }
      }
      return result;
    });
    return { ...value, cleanup: cleaned ? "confirmed" : "unresolved" };
  }
}
