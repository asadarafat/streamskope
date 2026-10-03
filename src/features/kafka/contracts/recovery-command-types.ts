import type { HostCommandBase } from "./types";

export type RecoveryHostCommand =
  | (HostCommandBase & {
      readonly command: "acls.access.explain";
      readonly payload: import("./acl-review").TopicAccessInput;
    })
  | (HostCommandBase & {
      readonly command: "acls.change.review";
      readonly payload: import("./acl-review").AclChangeInput;
    })
  | (HostCommandBase & {
      readonly command: "acls.change.apply";
      readonly payload: { readonly planId: string; readonly confirmation: string };
    })
  | (HostCommandBase & {
      readonly command: "records.replay.review";
      readonly payload: import("./record-replay").RecordReplayInput;
    })
  | (HostCommandBase & {
      readonly command: "records.replay.apply";
      readonly payload: { readonly planId: string; readonly confirmation: string };
    })
  | (HostCommandBase & {
      readonly command: "records.replay.cancel";
      readonly payload: { readonly planId: string };
    })
  | (HostCommandBase & {
      readonly command: "consumerGroups.reset.review";
      readonly payload: import("./offset-reset").OffsetResetInput;
    })
  | (HostCommandBase & {
      readonly command: "consumerGroups.reset.apply";
      readonly payload: { readonly planId: string; readonly confirmation: string };
    });
