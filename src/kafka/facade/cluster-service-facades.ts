import type { HostCommand, HostCommandResponse, HostEvent } from "../contracts";
import type {
  KafkaApplicationSession,
  RedpandaTransformPort,
  SchemaRegistryPort,
} from "../application";

import { AclFacadeController } from "./acl-facade";
import type { ActivityInput } from "./facade-support";
import { SchemaRegistryFacadeController } from "./schema-registry-facade";
import { TransformFacadeController } from "./transform-facade";

type ClusterServiceHostCommand = Extract<
  HostCommand,
  {
    readonly command:
      | "schemas.list"
      | "schemas.load"
      | "schemas.compatibility.check"
      | "schemas.register"
      | "schemas.delete"
      | "acls.list"
      | "acls.create"
      | "acls.delete"
      | "transforms.list"
      | "transforms.load"
      | "transforms.logs.load"
      | "transforms.delete";
  }
>;

interface ClusterServiceFacadeOptions {
  readonly nextSequence: () => number;
  readonly now: () => Date;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (activity: ActivityInput) => void;
  readonly schemaRegistry?: SchemaRegistryPort;
  readonly session: KafkaApplicationSession;
  readonly transforms?: RedpandaTransformPort;
}

export class ClusterServiceFacadeController {
  private readonly acls: AclFacadeController;
  private readonly schemaRegistry: SchemaRegistryFacadeController;
  private readonly transforms: TransformFacadeController;

  constructor(options: ClusterServiceFacadeOptions) {
    const common = {
      nextSequence: options.nextSequence,
      now: options.now,
      publish: options.publish,
      recordActivity: options.recordActivity,
      session: options.session,
    };
    this.acls = new AclFacadeController({
      ...common,
      available: (): boolean => true,
    });
    this.schemaRegistry = new SchemaRegistryFacadeController({
      ...common,
      ...(options.schemaRegistry === undefined ? {} : { port: options.schemaRegistry }),
    });
    this.transforms = new TransformFacadeController({
      ...common,
      ...(options.transforms === undefined ? {} : { port: options.transforms }),
    });
  }

  execute(command: ClusterServiceHostCommand, correlationId: string): Promise<HostCommandResponse> {
    switch (command.command) {
      case "schemas.list":
      case "schemas.load":
      case "schemas.compatibility.check":
      case "schemas.register":
      case "schemas.delete":
        return this.schemaRegistry.execute(command, correlationId);
      case "acls.list":
      case "acls.create":
      case "acls.delete":
        return this.acls.execute(command, correlationId);
      case "transforms.list":
      case "transforms.load":
      case "transforms.logs.load":
      case "transforms.delete":
        return this.transforms.execute(command, correlationId);
    }
  }

  invalidate(): void {
    this.schemaRegistry.invalidate();
    this.acls.invalidate();
    this.transforms.invalidate();
  }
}
