import {
  HOST_PROTOCOL_VERSION,
  type CommandTemplateCatalog,
  type ConnectionTemplateSnapshot,
  type HostCommand,
  type ProfileTrustKind,
  type RemoteSshTargetInput,
  type RemoteTrustAcquisitionSummary,
} from "../contracts";
import type {
  RemoteSshAuthentication,
  TrustAcquisitionEditor,
} from "../contracts/remote-trust-types";

import { formatUtcTimestamp } from "./timestamp-presentation";

export interface SshTargetDraft {
  readonly host: string;
  readonly port: string;
  readonly username: string;
}

export type TargetField = keyof SshTargetDraft | "password";
export type BusyOperation = "discard" | "discover-material" | "material" | "apply";

export interface AcquisitionPlan {
  readonly identityId: string;
  readonly editor: TrustAcquisitionEditor;
  readonly hostKeyFingerprint: string;
  readonly materialTemplateName: string;
  readonly passwordTemplateName?: string;
}

export const emptyTarget: SshTargetDraft = {
  host: "",
  port: "22",
  username: "",
};

export function selectedTemplate(
  snapshot: ConnectionTemplateSnapshot | null,
  catalog: CommandTemplateCatalog,
): { readonly name: string; readonly template: string } | undefined {
  if (snapshot?.store.state !== "ready") {
    return undefined;
  }
  const selectedCatalog = snapshot.catalogs.find((item) => item.catalog === catalog);
  return selectedCatalog?.entries.find((entry) => entry.name === selectedCatalog.selectedName);
}

export function issueField(path: string): TargetField | undefined {
  if (path.endsWith(".username")) {
    return "username";
  }
  if (path.endsWith(".password")) {
    return "password";
  }
  if (path.endsWith(".port")) {
    return "port";
  }
  return path.endsWith(".host") ? "host" : undefined;
}

export function targetInput(
  draft: SshTargetDraft,
  hostKeyFingerprint: string,
  authentication: RemoteSshAuthentication,
): RemoteSshTargetInput {
  return {
    host: draft.host,
    hostKeyFingerprint,
    ...(authentication.mode === "password"
      ? { password: authentication.password }
      : { authentication }),
    port: Number(draft.port),
    username: draft.username,
  };
}

export function hostKeyDiscoveryCommand(
  draft: SshTargetDraft,
  editor: TrustAcquisitionEditor,
): HostCommand {
  return {
    command: "trustAcquisition.hostKey.discover",
    id: globalThis.crypto.randomUUID(),
    payload: {
      editor,
      target: {
        host: draft.host,
        port: Number(draft.port),
      },
    },
    version: HOST_PROTOCOL_VERSION,
  };
}

export function acquisitionCommand(
  target: RemoteSshTargetInput,
  kind: ProfileTrustKind,
  label: string,
  editor: TrustAcquisitionEditor,
): Extract<HostCommand, { readonly command: "trustAcquisition.material.fetch" }> {
  return {
    command: "trustAcquisition.material.fetch",
    id: globalThis.crypto.randomUUID(),
    payload: {
      editor,
      kind,
      label,
      target,
    },
    version: HOST_PROTOCOL_VERSION,
  };
}

export function acquisitionStatus(acquisition: RemoteTrustAcquisitionSummary | null): string {
  if (acquisition === null) {
    return "No remote trust acquired.";
  }
  const expiry = formatUtcTimestamp(acquisition.expiresAt);
  if (acquisition.material !== null) {
    return `${acquisition.material.kind.toUpperCase()} trust material acquired (${acquisition.material.byteCount.toLocaleString()} bytes). Expires ${expiry}.`;
  }
  return `Trust acquisition is incomplete. Discard it and acquire the trust material again. Expires ${expiry}.`;
}
