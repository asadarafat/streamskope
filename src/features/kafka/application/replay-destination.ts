import type { KafkaProfileService } from "./profile-service";
import {
  KafkaConnectionScopes,
  type ReviewedWriteScope,
  type RecordReadScope,
  type SchemaRegistryReviewScope,
} from "./connection-scope";
import type { KafkaActiveConnection, KafkaConnectionPort } from "./types";

export interface ReplayDestination {
  readonly connection: KafkaActiveConnection;
  readonly name: string;
  current(): boolean;
  close(): Promise<void>;
}
export interface ReplayDestinationPort {
  open(id: string, revision: number, signal: AbortSignal): Promise<ReplayDestination>;
}
export interface ReviewedReplayDestination {
  readonly scope: ReviewedWriteScope;
  readonly readScope?: RecordReadScope;
  readonly registryScope?: SchemaRegistryReviewScope;
  close(): Promise<void>;
}
export interface ReviewedReplayDestinationPort {
  openReviewed(
    id: string,
    revision: number,
    signal: AbortSignal,
  ): Promise<ReviewedReplayDestination>;
}
/** Resolve stored credentials in the host; never switches the active profile or runs plugin hooks. */
export class SavedReplayDestinations
  implements ReplayDestinationPort, ReviewedReplayDestinationPort
{
  constructor(
    private readonly profiles: KafkaProfileService,
    private readonly connections: KafkaConnectionPort,
  ) {}
  async openReviewed(
    id: string,
    revision: number,
    signal: AbortSignal,
  ): Promise<ReviewedReplayDestination> {
    const target = await this.open(id, revision, signal);
    let closed = false;
    let closing: Promise<void> | undefined;
    // The opening signal owns admission only; its deadline must not expire a retained review.
    const scopes = new KafkaConnectionScopes(() =>
      !closed && target.current()
        ? { connection: target.connection, generation: 0, connectionName: target.name }
        : null,
    );
    const scope = scopes.reviewedWrite(),
      readScope = scopes.recordRead(),
      registryScope = scopes.schemaRegistry();
    if (scope === null) {
      try {
        await target.close();
      } catch (cleanupCause) {
        throw Object.assign(new Error("Changed destination cleanup could not be confirmed."), {
          cleanupCause:
            cleanupCause === undefined
              ? new Error("Destination cleanup rejected without a reason.", { cause: cleanupCause })
              : cleanupCause,
        });
      }
      throw new Error("Destination profile changed while opening.");
    }
    return {
      scope,
      ...(readScope === null ? {} : { readScope }),
      ...(registryScope === null ? {} : { registryScope }),
      close(): Promise<void> {
        closed = true;
        closing ??= Promise.resolve().then(() => target.close());
        return closing;
      },
    };
  }
  async open(id: string, revision: number, signal: AbortSignal): Promise<ReplayDestination> {
    await this.profiles.list(signal);
    const current = (): boolean =>
      this.profiles
        .currentSnapshot()
        .profiles.some((p) => p.id === id && (p.revision ?? 1) === revision);
    if (!current())
      throw new Error("Destination profile changed. Reload profiles and review again.");
    const input = await this.profiles.resolveConnection(id, signal);
    signal.throwIfAborted();
    const connection = await this.connections.openConnection(input, signal);
    if (!current() || signal.aborted) {
      try {
        await connection.close();
      } catch (cleanupCause) {
        throw Object.assign(new Error("Cancelled destination cleanup could not be confirmed."), {
          cleanupCause:
            cleanupCause === undefined
              ? new Error("Destination cleanup rejected without a reason.", { cause: cleanupCause })
              : cleanupCause,
        });
      }
      throw new Error("Destination profile changed while opening.");
    }
    return { connection, name: input.name, current, close: () => connection.close() };
  }
}
