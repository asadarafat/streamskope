import type { KafkaProfileService } from "./profile-service";
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
/** Resolve stored credentials in the host; never switches the active profile or runs plugin hooks. */
export class SavedReplayDestinations implements ReplayDestinationPort {
  constructor(
    private readonly profiles: KafkaProfileService,
    private readonly connections: KafkaConnectionPort,
  ) {}
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
          cleanupCause,
        });
      }
      throw new Error("Destination profile changed while opening.");
    }
    return { connection, name: input.name, current, close: () => connection.close() };
  }
}
