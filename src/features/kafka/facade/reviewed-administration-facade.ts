import type { KafkaApplicationSession } from "../application";
import type { HostCommand, HostCommandResponse } from "../contracts";
import { isAclReviewCommand, type AclReviewCommand } from "../contracts/acl-review-commands";

import { AclReviewFacade } from "./acl-review-facade";
import { ClientQuotaFacade } from "./client-quota-facade";
import { GroupAdministrationFacade } from "./group-administration-facade";
import { OffsetResetFacade } from "./offset-reset-facade";
import { TopicAdministrationFacade } from "./topic-administration-facade";
import type { ActivityInput } from "./facade-support";

type ReviewedAdministrationCommand =
  | AclReviewCommand
  | Extract<
      HostCommand,
      {
        command:
          | "quotas.inspect"
          | "quotas.change.review"
          | "quotas.change.apply"
          | "consumerGroups.delete.review"
          | "consumerGroups.delete.apply"
          | "consumerGroups.reset.review"
          | "consumerGroups.reset.apply"
          | "topics.change.review"
          | "topics.change.apply";
      }
    >;

export function isReviewedAdministrationCommand(
  command: HostCommand,
): command is ReviewedAdministrationCommand {
  return (
    isAclReviewCommand(command) ||
    command.command === "quotas.inspect" ||
    command.command === "quotas.change.review" ||
    command.command === "quotas.change.apply" ||
    command.command === "consumerGroups.delete.review" ||
    command.command === "consumerGroups.delete.apply" ||
    command.command === "consumerGroups.reset.review" ||
    command.command === "consumerGroups.reset.apply" ||
    command.command === "topics.change.review" ||
    command.command === "topics.change.apply"
  );
}

/** One routing boundary for remote changes that require a host-owned review. */
export class ReviewedAdministrationFacade {
  private readonly acls: AclReviewFacade;
  private readonly offsets: OffsetResetFacade;
  private readonly groups: GroupAdministrationFacade;
  private readonly quotas: ClientQuotaFacade;
  private readonly topics: TopicAdministrationFacade;

  constructor(session: KafkaApplicationSession, activity: (input: ActivityInput) => void) {
    this.acls = new AclReviewFacade(session, activity);
    this.offsets = new OffsetResetFacade(session, activity);
    this.groups = new GroupAdministrationFacade(session, activity);
    this.quotas = new ClientQuotaFacade(session, activity);
    this.topics = new TopicAdministrationFacade(session, activity);
  }

  execute(
    command: ReviewedAdministrationCommand,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    switch (command.command) {
      case "quotas.inspect":
      case "quotas.change.review":
      case "quotas.change.apply":
        return this.quotas.execute(command, correlationId);
      case "consumerGroups.delete.review":
      case "consumerGroups.delete.apply":
        return this.groups.execute(command, correlationId);
      case "consumerGroups.reset.review":
      case "consumerGroups.reset.apply":
        return this.offsets.execute(command, correlationId);
      case "topics.change.review":
      case "topics.change.apply":
        return this.topics.execute(command, correlationId);
      default:
        return this.acls.execute(command, correlationId);
    }
  }
}
