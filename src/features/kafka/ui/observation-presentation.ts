import type { ObservationHint } from "../contracts/observation-analysis";
import type { ObservationPartition } from "../contracts/observations";

export function observationPartitionIssues(
  partition: ObservationPartition,
  groupSelected: boolean,
): string[] {
  return [
    ...(partition.leader === null ? ["Leader unknown"] : []),
    ...(partition.inSyncReplicas < partition.replicas ? ["Under-replicated"] : []),
    ...(partition.endOffset === null ? ["End position unknown"] : []),
    ...(groupSelected && partition.lag === null ? ["Lag unknown"] : []),
    ...(partition.lag !== null && BigInt(partition.lag) > 0n ? ["Consumer behind"] : []),
  ];
}
export function findingPriority(hint: ObservationHint): number {
  if (hint.title === "Replication evidence needs attention") return 0;
  if (hint.title === "No visible consumer members" || hint.title === "Commits appear stalled")
    return 1;
  if (hint.title === "Append positions outpace commits") return 2;
  return 3;
}
export function observationNumber(value: number | null): string {
  return value === null ? "Unknown" : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
