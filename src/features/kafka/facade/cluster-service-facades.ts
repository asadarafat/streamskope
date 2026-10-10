import { type HostCommand, type HostCommandResponse, type HostEvent } from "../contracts";
import type {
  KafkaApplicationSession,
  RedpandaTransformPort,
  SchemaRegistryPort,
} from "../application";
import type {
  SchemaRegistryReviewPort,
  SchemaRegistryPolicyPort,
} from "../application/schema-registry-types";
import { SchemaReviewOperations } from "../application/schema-review-operations";

import { SchemaPolicyFacade } from "./schema-policy-facade";
import { SchemaChangeFacade } from "./schema-change-facade";
import { AclFacadeController } from "./acl-facade";
import type { ActivityInput } from "./facade-support";
import { SchemaRegistryFacadeController } from "./schema-registry-facade";
import { TransformFacadeController } from "./transform-facade";

type ClusterServiceHostCommand = Extract<
  HostCommand,
  {
    readonly command:
      | "schemas.policy.load"
      | "schemas.policy.review"
      | "schemas.policy.apply"
      | "schemas.change.review"
      | "schemas.change.apply"
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
  readonly schemaRegistry?: SchemaRegistryPort &
    Partial<SchemaRegistryReviewPort & SchemaRegistryPolicyPort>;
  readonly session: KafkaApplicationSession;
  readonly transforms?: RedpandaTransformPort;
}

export class ClusterServiceFacadeController {
  private readonly acls: AclFacadeController;
  private readonly schemaRegistry: SchemaRegistryFacadeController;
  private readonly schemaChanges: SchemaChangeFacade;
  private readonly schemaPolicies: SchemaPolicyFacade;
  private readonly schemaOperations: SchemaReviewOperations;
  private readonly transforms: TransformFacadeController;

  constructor(options: ClusterServiceFacadeOptions) {
    const common = {
      nextSequence: options.nextSequence,
      now: options.now,
      publish: options.publish,
      recordActivity: options.recordActivity,
      session: options.session,
    };
    this.schemaOperations = new SchemaReviewOperations(() =>
      options.session.schemaRegistryReviewScope(),
    );
    this.schemaPolicies = new SchemaPolicyFacade(
      options.schemaRegistry,
      this.schemaOperations,
      options.recordActivity,
    );
    this.schemaChanges = new SchemaChangeFacade(
      options.session,
      options.schemaRegistry,
      options.recordActivity,
      this.schemaOperations,
    );
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
      case "schemas.policy.load":
      case "schemas.policy.review":
      case "schemas.policy.apply":
        return this.schemaPolicies.execute(command, correlationId);
      case "schemas.change.review":
      case "schemas.change.apply":
        return this.schemaChanges.execute(command, correlationId);
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
    throw new Error(`Unreachable cluster-service command: ${String(command satisfies never)}`);
  }

  invalidate(): void {
    this.schemaRegistry.invalidate();
    this.schemaOperations.invalidate();
    this.acls.invalidate();
    this.transforms.invalidate();
  }
}
