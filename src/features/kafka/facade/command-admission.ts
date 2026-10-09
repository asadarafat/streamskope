import { ProviderCommandAdmission } from "../../../platform/providers/operation-ownership";
import type { HostCommand, HostCommandResponse } from "../contracts";

import type { KafkaCommandProtection } from "./command-protection";
import { failureResponse } from "./facade-support";
import { RecordLocatorAdmission } from "./record-locator-admission";

export interface KafkaDispatchContext {
  readonly suppressedLocatorLoad: Promise<boolean>;
}

interface DispatchBindings {
  readonly correlationId: string;
  readonly internal: boolean;
  readonly protection: Pick<KafkaCommandProtection, "execute">;
  readonly unavailable: () => HostCommandResponse;
  readonly superseded: () => HostCommandResponse;
  readonly dispatch: (context?: KafkaDispatchContext) => Promise<HostCommandResponse>;
}

/** Owns command intent across asynchronous protection checks and connection cleanup. */
export class KafkaCommandAdmission extends ProviderCommandAdmission {
  private connectionChanges = 0;
  private authorizationIntent = 0;
  private consumptionIntent = 0;
  private readonly locators = new RecordLocatorAdmission();

  override close(): void {
    this.locators.revoke();
    super.close();
  }

  interruptConsumption(): void {
    this.consumptionIntent += 1;
  }

  async dispatch(command: HostCommand, bindings: DispatchBindings): Promise<HostCommandResponse> {
    if (!this.accepts(bindings.internal)) return bindings.unavailable();
    if (
      (command.command === "records.export.start" ||
        command.command === "records.analysis.start" ||
        command.command === "records.locator.load" ||
        command.command === "catalog.load" ||
        command.command === "catalog.put") &&
      this.connectionChanges > 0
    ) {
      if (command.command === "records.locator.load")
        this.locators.reject(command.payload.requestId);
      const feature =
        command.command === "records.export.start"
          ? "export"
          : command.command === "records.analysis.start"
            ? "analysis"
            : command.command === "catalog.load" || command.command === "catalog.put"
              ? "local topic notes operation"
              : "record reload";
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "validation",
        correlationId: bindings.correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: `Wait for the connection change before starting ${feature === "record reload" || feature === "local topic notes operation" ? "a" : "an"} ${feature}.`,
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
    if (
      changesConnection ||
      command.command === "preferences.reset" ||
      (command.command === "preferences.update" &&
        (command.payload.patch.protection !== undefined ||
          command.payload.patch.codecs !== undefined))
    )
      this.locators.revoke();
    const locatorLoad =
      command.command === "records.locator.load"
        ? this.locators.load(command.payload.requestId)
        : undefined;
    const locatorCancel =
      command.command === "records.locator.cancel"
        ? this.locators.cancel(command.payload.requestId)
        : undefined;
    if (
      (command.command === "records.locator.load" && locatorLoad === undefined) ||
      (command.command === "records.locator.cancel" && locatorCancel === undefined)
    )
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "query",
        correlationId: bindings.correlationId,
        activeStateChanged: false,
        retryable: true,
        summary: "A record reload admission or cancellation is already pending.",
        recovery: "Wait for the pending operation before retrying with a fresh request identifier.",
      });
    const intent = this.authorizationIntent;
    const beginsConsumption =
      command.command === "messages.start" || command.command === "messages.continue";
    if (beginsConsumption || command.command === "messages.stop") this.interruptConsumption();
    const consumptionIntent = this.consumptionIntent;
    let locatorCleanupConfirmed = false;
    try {
      const response = await bindings.protection.execute(
        command,
        bindings.correlationId,
        async () => {
          if (!this.accepts(bindings.internal)) return Promise.resolve(bindings.unavailable());
          if (
            intent !== this.authorizationIntent ||
            (beginsConsumption && consumptionIntent !== this.consumptionIntent)
          )
            return bindings.superseded();
          if (locatorLoad !== undefined && !locatorLoad.enter())
            return failureResponse(command, {
              code: "CANCELLED",
              stage: "query",
              correlationId: bindings.correlationId,
              activeStateChanged: false,
              retryable: true,
              summary: "The record reload was cancelled before it opened a reader.",
              recovery: "Use the current connection and settings to start a new reload when ready.",
            });
          return bindings.dispatch(
            locatorCancel === undefined
              ? undefined
              : {
                  suppressedLocatorLoad: locatorCancel.suppressed,
                },
          );
        },
      );
      locatorCleanupConfirmed =
        command.command === "records.locator.load" &&
        response.command === "records.locator.load" &&
        response.ok;
      return response;
    } finally {
      locatorLoad?.finish(locatorCleanupConfirmed);
      locatorCancel?.finish();
      if (changesConnection) this.connectionChanges -= 1;
    }
  }
}
