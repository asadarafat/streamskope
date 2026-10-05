import {
  NatsApplicationSession,
  InMemoryNatsProfileStore,
  NatsProfileService,
  type NatsProfileServiceOptions,
  type NatsProfileStore,
  type NatsEngine,
} from "../../features/nats/application";
import { NatsBackendFacade, type NatsBackendFacadeOptions } from "../../features/nats/facade";
import { StreamSkopeNatsEngine } from "../../features/nats/engine/engine";

export interface NatsBackendOptions {
  readonly profileStore?: NatsProfileStore;
  readonly engine?: NatsEngine;
  readonly profileServiceOptions?: Omit<NatsProfileServiceOptions, "isProfileInUse">;
  readonly facadeOptions?: NatsBackendFacadeOptions;
}

/** Real built-in Core NATS composition; browser profiles are explicitly session-only. */
export function createNatsBackend(options: NatsBackendOptions = {}): NatsBackendFacade {
  const engine = options.engine ?? new StreamSkopeNatsEngine();
  const session = new NatsApplicationSession(engine);
  const profiles = new NatsProfileService(options.profileStore ?? new InMemoryNatsProfileStore(), {
    ...options.profileServiceOptions,
    isProfileInUse: (id): boolean => session.isProfileInUse(id),
  });
  return new NatsBackendFacade(session, profiles, options.facadeOptions);
}
