import { join } from "node:path";

import { UnavailableNatsProfileStore } from "../../../features/nats/application/profile-service";
import type { NatsProfileStore } from "../../../features/nats/application/profile-types";
import { AtomicNatsProfileFileStore } from "../../node/nats-profile-file-store";

import type { ElectronProfileProtection } from "./electron-profile-protection";

export interface ElectronNatsProfileStoreOptions {
  readonly userDataPath: string;
  readonly profileProtection: ElectronProfileProtection;
}

export function createElectronNatsProfileStore(
  options: ElectronNatsProfileStoreOptions,
): NatsProfileStore {
  const protection = options.profileProtection;
  return protection.protector === undefined || protection.capability.state !== "ready"
    ? new UnavailableNatsProfileStore(protection.capability)
    : new AtomicNatsProfileFileStore(
        join(options.userDataPath, "profiles", "nats-profiles.json"),
        protection.protector,
      );
}
