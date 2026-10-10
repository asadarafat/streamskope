import { HostContractValidationError } from "./validation-error";
import {
  record,
  exactKeys,
  text,
  boundedText,
  declaredValue,
  nonNegativeInteger,
  truth,
} from "./validation-primitives";

export interface GroupAdministrationSnapshot {
  readonly groupId: string;
  readonly clusterId: string;
  readonly protocolType: string;
  readonly state: string;
  readonly members: number;
  readonly offsetsSha256: string;
  readonly deletePermission: "allowed" | "denied" | "unknown";
  readonly deleteSupported: boolean;
}
export interface GroupAdministrationReview {
  readonly planId: string;
  readonly connectionName: string;
  readonly expiresAt: string;
  readonly baseline: GroupAdministrationSnapshot;
  readonly confirmation: string;
}
export interface GroupAdministrationOutcome {
  readonly groupId: string;
  readonly state: "acknowledged" | "rejected" | "unknown" | "unsent";
  readonly verification: "verified" | "different" | "unavailable";
  readonly cleanup: "confirmed" | "unresolved";
  readonly detail: string;
}
export function parseGroupAdministrationInput(value: unknown): { readonly groupId: string } {
  const v = record(value, "groupDelete");
  exactKeys(v, ["groupId"], "groupDelete");
  return { groupId: text(v.groupId, "groupId", 512) };
}
export function parseGroupAdministrationSnapshot(value: unknown): GroupAdministrationSnapshot {
  const v = record(value, "groupBaseline");
  exactKeys(
    v,
    [
      "groupId",
      "clusterId",
      "protocolType",
      "state",
      "members",
      "offsetsSha256",
      "deletePermission",
      "deleteSupported",
    ],
    "groupBaseline",
  );
  const offsetsSha256 = text(v.offsetsSha256, "offsetsSha256", 64);
  if (!/^[a-f0-9]{64}$/u.test(offsetsSha256))
    throw new HostContractValidationError(
      "offsetsSha256",
      "requires a complete offset fingerprint",
    );
  const protocolType = boundedText(v.protocolType, "protocolType", 64);
  if (protocolType !== "" && protocolType !== "consumer")
    throw new HostContractValidationError(
      "protocolType",
      "requires a supported consumer coordination protocol",
    );
  return {
    groupId: text(v.groupId, "groupId", 512),
    clusterId: text(v.clusterId, "clusterId", 128),
    protocolType,
    state: text(v.state, "state", 64),
    members: nonNegativeInteger(v.members, "members"),
    offsetsSha256,
    deleteSupported: truth(v.deleteSupported, "deleteSupported"),
    deletePermission: declaredValue(
      v.deletePermission,
      ["allowed", "denied", "unknown"] as const,
      "deletePermission",
    ),
  };
}
export function groupDeleteConfirmation(groupId: string): string {
  return `DELETE GROUP ${groupId}`;
}
export function sameGroupBaseline(
  a: GroupAdministrationSnapshot,
  b: GroupAdministrationSnapshot,
): boolean {
  return (
    a.groupId === b.groupId &&
    a.clusterId === b.clusterId &&
    a.protocolType === b.protocolType &&
    a.state === b.state &&
    a.members === b.members &&
    a.offsetsSha256 === b.offsetsSha256 &&
    a.deletePermission === b.deletePermission &&
    a.deleteSupported === b.deleteSupported
  );
}
export function groupDeletionAllowed(b: GroupAdministrationSnapshot): boolean {
  return (
    ["Empty", "EMPTY"].includes(b.state) &&
    b.members === 0 &&
    b.deleteSupported &&
    b.deletePermission !== "denied"
  );
}
export function parseGroupAdministrationReview(value: unknown): GroupAdministrationReview {
  const v = record(value, "groupReview");
  exactKeys(
    v,
    ["planId", "connectionName", "expiresAt", "baseline", "confirmation"],
    "groupReview",
  );
  const baseline = parseGroupAdministrationSnapshot(v.baseline),
    confirmation = text(v.confirmation, "confirmation", 524);
  if (confirmation !== groupDeleteConfirmation(baseline.groupId))
    throw new HostContractValidationError("confirmation", "must match the reviewed group");
  return {
    planId: text(v.planId, "planId", 128),
    connectionName: text(v.connectionName, "connectionName", 256),
    expiresAt: text(v.expiresAt, "expiresAt", 64),
    baseline,
    confirmation,
  };
}
export function parseGroupAdministrationOutcome(value: unknown): GroupAdministrationOutcome {
  const v = record(value, "groupOutcome");
  exactKeys(v, ["groupId", "state", "verification", "cleanup", "detail"], "groupOutcome");
  return {
    groupId: text(v.groupId, "groupId", 512),
    state: declaredValue(
      v.state,
      ["acknowledged", "rejected", "unknown", "unsent"] as const,
      "state",
    ),
    verification: declaredValue(
      v.verification,
      ["verified", "different", "unavailable"] as const,
      "verification",
    ),
    cleanup: declaredValue(v.cleanup, ["confirmed", "unresolved"] as const, "cleanup"),
    detail: text(v.detail, "detail", 2048),
  };
}
