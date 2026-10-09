import { ProviderCommandAdmission } from "../../../platform/providers/operation-ownership";
import type { HostCommand, HostCommandResponse } from "../contracts";

import type { KafkaCommandProtection } from "./command-protection";
import { failureResponse } from "./facade-support";

interface DispatchBindings {
  readonly correlationId: string;
  readonly internal: boolean;
  readonly protection: Pick<KafkaCommandProtection, "execute">;
  readonly unavailable: () => HostCommandResponse;
  readonly superseded: () => HostCommandResponse;
  readonly dispatch: () => Promise<HostCommandResponse>;
}

/** Owns command intent across asynchronous protection checks and connection cleanup. */
export class KafkaCommandAdmission extends ProviderCommandAdmission {
  private connectionChanges = 0;
  private authorizationIntent = 0;
  private consumptionIntent = 0;

  interruptConsumption(): void {
    this.consumptionIntent += 1;
  }

  async dispatch(command: HostCommand, bindings: DispatchBindings): Promise<HostCommandResponse> {
    if (!this.accepts(bindings.internal)) return bindings.unavailable();
    if (
      (command.command === "records.export.start" ||
        command.command === "records.analysis.start") &&
      this.connectionChanges > 0
    ) {
      const feature = command.command === "records.export.start" ? "export" : "analysis";
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "validation",
        correlationId: bindings.correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: `Wait for the connection change before starting an ${feature}.`,
        recovery: `Confirm the connected profile, then start a new ${feature} from that connection.`,
      });
    }
    const changesConnection = [
      "connection.connect",
      "profiles.connect",
      "connection.disconnect",
    ].includes(command.command);
    if (changesConnection) {
      this.authorizationIntent += 1;
      this.connectionChanges += 1;
    }
    const intent = this.authorizationIntent;
    const beginsConsumption =
      command.command === "messages.start" || command.command === "messages.continue";
    if (beginsConsumption || command.command === "messages.stop") this.interruptConsumption();
    const consumptionIntent = this.consumptionIntent;
    try {
      return await bindings.protection.execute(command, bindings.correlationId, () => {
        if (!this.accepts(bindings.internal)) return Promise.resolve(bindings.unavailable());
        return intent === this.authorizationIntent &&
          (!beginsConsumption || consumptionIntent === this.consumptionIntent)
          ? bindings.dispatch()
          : Promise.resolve(bindings.superseded());
      });
    } finally {
      if (changesConnection) this.connectionChanges -= 1;
    }
  }
}
