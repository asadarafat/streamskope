import type { HostCommandResponse, HostEvent } from "../contracts";
import type {
  KafkaConnectionTemplateService,
  KafkaConnectionTemplateSnapshot,
} from "../application";

import {
  failureResponse,
  successResponse,
  templatesChangedEvent,
  translateFacadeFailure,
  type ActivityInput,
} from "./facade-support";
import {
  templateActivityObject,
  templateOperation,
  type TemplateHostCommand,
} from "./template-activity";

export interface TemplateFacadeBindings {
  readonly available: () => boolean;
  readonly nextSequence: () => number;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly templates: KafkaConnectionTemplateService;
}

export async function executeTemplateCommand(
  command: TemplateHostCommand,
  correlationId: string,
  bindings: TemplateFacadeBindings,
): Promise<HostCommandResponse> {
  const operation = templateOperation(command);
  const object = templateActivityObject(command);
  try {
    let snapshot: KafkaConnectionTemplateSnapshot;
    switch (command.command) {
      case "templates.create":
        snapshot = await bindings.templates.create(command.payload);
        break;
      case "templates.delete":
        snapshot = await bindings.templates.delete(command.payload.catalog, command.payload.name);
        break;
      case "templates.list":
        snapshot = await bindings.templates.list();
        break;
      case "templates.select":
        snapshot = await bindings.templates.select(command.payload.catalog, command.payload.name);
        break;
      case "templates.update":
        snapshot = await bindings.templates.update(
          command.payload.catalog,
          command.payload.originalName,
          {
            name: command.payload.name,
            template: command.payload.template,
          },
        );
        break;
    }
    bindings.publish(templatesChangedEvent(snapshot, bindings.nextSequence()));
    bindings.recordActivity({
      correlationId,
      detail: `${operation} completed and the bounded template inventory was refreshed.`,
      object,
      operation,
      outcome: "succeeded",
      severity: "info",
    });
    return successResponse(command, correlationId);
  } catch (error) {
    const translated = translateFacadeFailure(
      error,
      {
        activeStateChanged: false,
        connection: undefined,
        correlationId,
      },
      bindings.available(),
    );
    bindings.publish(
      templatesChangedEvent(bindings.templates.currentSnapshot(), bindings.nextSequence()),
    );
    bindings.recordActivity({
      correlationId,
      detail: translated.detail,
      object,
      operation,
      outcome: "failed",
      severity: "error",
    });
    return failureResponse(command, translated.error);
  }
}
