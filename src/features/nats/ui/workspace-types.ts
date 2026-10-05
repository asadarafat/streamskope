import type { NatsHost } from "../contracts";

/** Trusted composition resolves a real provider lazily; absence owns no host resources. */
export type NatsWorkspaceSource =
  | { readonly state: "ready"; readonly host: NatsHost }
  | { readonly state: "unavailable"; readonly recovery: string };
