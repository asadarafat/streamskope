import type { HostCommandBase } from "./types";

export type RecoveryHostCommand =
  | (HostCommandBase & {
      readonly command: "topics.change.review";
      readonly payload: import("./topic-administration").TopicAdministrationInput;
    })
  | (HostCommandBase & {
      readonly command: "topics.change.apply";
      readonly payload: { readonly planId: string; readonly confirmation: string };
    })
  | (HostCommandBase & {
      readonly command: "records.repair.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | import("./acl-review-commands").AclReviewCommand
  | (HostCommandBase & {
      readonly command: "records.repair.review";
      readonly payload: import("./repair-recovery").RepairContinuationInput;
    })
  | (HostCommandBase & {
      readonly command: "records.repair.reconcile";
      readonly payload: import("./repair-recovery").RepairReconciliationInput;
    })
  | (HostCommandBase & {
      readonly command: "records.repair.archive";
      readonly payload: import("./repair-recovery").RepairArchiveInput;
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
