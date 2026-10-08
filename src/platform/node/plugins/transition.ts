import { randomUUID } from "node:crypto";

import {
  PLUGIN_TRANSITION_OPERATIONS,
  PLUGIN_TRANSITION_STAGES,
  type PluginTransition,
  type PluginTransitionOperation,
  type PluginTransitionStage,
} from "../../../plugins/contracts";
import { parsePluginId } from "../../../plugins/validation";

import { pluginProblem } from "./problem";

export const PLUGIN_TRANSITION_WAIT_MS = 10_000;
const MAXIMUM_OPERATIONS = 64;
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/u;

export interface PluginTransitionSpec {
  readonly pluginId: string;
  readonly operation: PluginTransitionOperation;
  readonly activationId?: string;
}
export interface PluginTransitionCounts {
  readonly requests: number;
  readonly connections: number;
}
export interface PluginTransitionMetadata {
  readonly activationId?: string;
  readonly commit?: PluginTransition["commit"];
  readonly counts?: () => PluginTransitionCounts;
}
export interface PluginTransitionHandle {
  assertCurrent(): void;
  phase(stage: PluginTransitionStage, metadata?: PluginTransitionMetadata): void;
  wait<T>(
    stage: PluginTransitionStage,
    run: () => Promise<T>,
    metadata?: PluginTransitionMetadata,
  ): Promise<T>;
}
export interface PluginTransitionsOptions {
  readonly changed: () => void;
  readonly waitingAfterMs?: number;
}
interface Owner {
  record: PluginTransition;
  blocking: boolean;
  phase: symbol;
  timer?: ReturnType<typeof setTimeout>;
  counts?: () => PluginTransitionCounts;
}

function initialStage(operation: PluginTransitionOperation): PluginTransitionStage {
  switch (operation) {
    case "startup":
      return "load-candidate";
    case "install":
    case "retry":
      return "verify-package";
    case "remove":
    case "renderer-recovery":
      return "wait-connections";
    case "review-install":
    case "review-retry":
    case "review-remove":
      return "review-change";
    case "review-exit":
      return "review-exit";
    case "resolve-exit":
      return "resolve-exit";
    case "shutdown":
      return "close-backend";
  }
}
function blocksDispatch(operation: PluginTransitionOperation): boolean {
  return !operation.startsWith("review-");
}
function validActivation(value: string | undefined): void {
  if (value !== undefined && !UUID.test(value))
    throw pluginProblem("The plugin transition activation identity is invalid.");
}

/** Owns mutation admission and observable waits; a watchdog never releases the real operation. */
export class PluginTransitions {
  private readonly owners = new Map<string, Owner>();
  private readonly intents = new Map<string, symbol>();
  private tail: Promise<void> = Promise.resolve();
  private ordinary = 0;
  private closing = false;
  private readonly waitingAfterMs: number;

  constructor(private readonly options: PluginTransitionsOptions) {
    this.waitingAfterMs = options.waitingAfterMs ?? PLUGIN_TRANSITION_WAIT_MS;
    if (!Number.isSafeInteger(this.waitingAfterMs) || this.waitingAfterMs <= 0)
      throw new Error("Plugin transition waiting interval must be a positive safe integer.");
  }

  async withIntent<T>(
    pluginId: string,
    work: (assertCurrent: () => void) => Promise<T>,
  ): Promise<T> {
    parsePluginId(pluginId);
    this.assertOpen();
    this.assertAvailable(pluginId);
    if (!this.intents.has(pluginId) && this.intents.size >= MAXIMUM_OPERATIONS)
      throw pluginProblem("Too many plugin acquisitions are pending. Wait for them to finish.");
    const intent = Symbol();
    this.intents.set(pluginId, intent);
    const assertCurrent = (): void => {
      this.assertOpen();
      if (this.intents.get(pluginId) !== intent)
        throw pluginProblem(
          "This plugin change was superseded by a newer install or removal.",
          "The latest plugin change takes priority. Review its current state in Preferences > Plugins.",
        );
    };
    try {
      return await work(assertCurrent);
    } finally {
      if (this.intents.get(pluginId) === intent) this.intents.delete(pluginId);
    }
  }

  async run<T>(
    spec: PluginTransitionSpec,
    work: (handle: PluginTransitionHandle) => Promise<T>,
    assertIntent?: () => void,
  ): Promise<T> {
    this.assertOpen();
    assertIntent?.();
    if (spec.operation === "shutdown")
      throw pluginProblem("Shutdown cleanup requires closed plugin admission.");
    const owner = this.admit(spec);
    this.ordinary++;
    const operation = this.tail.then(async () => {
      try {
        this.assertOpen();
        assertIntent?.();
        owner.blocking = blocksDispatch(spec.operation);
        const handle = this.handle(owner, assertIntent);
        handle.phase(initialStage(spec.operation));
        return await work(handle);
      } finally {
        this.finish(owner);
        this.ordinary--;
      }
    });
    // Publish after linking the tail so reentrant observers cannot reorder queued work.
    this.tail = operation.then(
      () => undefined,
      () => undefined,
    );
    this.publish();
    return operation;
  }

  /** Exit reviews use one coherent cohort; an earlier pending activation must settle first. */
  async reviewExit<T>(
    specs: readonly (PluginTransitionSpec & { readonly operation: "review-exit" })[],
    work: (handles: ReadonlyMap<string, PluginTransitionHandle>) => Promise<T>,
  ): Promise<T> {
    this.assertOpen();
    if (this.ordinary !== 0)
      throw pluginProblem(
        "Plugin exit review is busy while another change is pending.",
        "Wait for the current plugin change to settle, then close the application again.",
      );
    if (
      specs.length > MAXIMUM_OPERATIONS ||
      new Set(specs.map((spec) => spec.pluginId)).size !== specs.length
    )
      throw pluginProblem("Plugin exit review requires a bounded, distinct plugin cohort.");
    for (const spec of specs) {
      this.validateSpec(spec);
      if (spec.operation !== "review-exit")
        throw pluginProblem("Plugin exit review accepts only exit-review operations.");
    }
    // Construct every record before admission/publication: a bad cohort admits none of it.
    const owners = specs.map((spec) => this.createOwner(spec));
    for (const owner of owners) this.owners.set(owner.record.pluginId, owner);
    this.ordinary++;
    const operation = this.tail.then(async () => {
      try {
        this.assertOpen();
        return await work(
          new Map(owners.map((owner) => [owner.record.pluginId, this.handle(owner)])),
        );
      } finally {
        for (const owner of owners) this.finish(owner, false);
        this.ordinary--;
        this.publish();
      }
    });
    this.tail = operation.then(
      () => undefined,
      () => undefined,
    );
    this.publish();
    return operation;
  }

  isChanging(pluginId: string): boolean {
    return this.owners.get(pluginId)?.blocking === true;
  }

  snapshot(): readonly PluginTransition[] {
    return Object.freeze([...this.owners.values()].map(({ record }) => record));
  }

  refresh(pluginId: string): void {
    const owner = this.owners.get(pluginId);
    if (owner !== undefined && this.sampleCounts(owner)) this.publish();
  }

  beginClose(): void {
    this.closing = true;
    this.intents.clear();
  }

  settled(): Promise<void> {
    return this.tail;
  }

  async observeShutdown<T>(
    spec: PluginTransitionSpec & { readonly operation: "shutdown" },
    work: (handle: PluginTransitionHandle) => Promise<T>,
  ): Promise<T> {
    if (!this.closing || this.ordinary !== 0 || spec.operation !== "shutdown")
      throw pluginProblem(
        "Plugin shutdown cleanup must wait for closed admission and settled changes.",
      );
    const owner = this.admit(spec);
    owner.blocking = true;
    const handle = this.handle(owner);
    try {
      handle.phase("close-backend");
      return await work(handle);
    } finally {
      this.finish(owner);
    }
  }

  private assertOpen(): void {
    if (this.closing) throw pluginProblem("The plugin host is closing.");
  }

  private assertAvailable(pluginId: string): void {
    if (this.owners.has(pluginId))
      throw pluginProblem(
        "The plugin is busy with another change.",
        "Wait for its current operation to settle. View its progress in Preferences > Plugins.",
      );
  }

  private validateSpec(spec: PluginTransitionSpec): void {
    parsePluginId(spec.pluginId);
    if (!PLUGIN_TRANSITION_OPERATIONS.includes(spec.operation))
      throw pluginProblem("The plugin transition operation is invalid.");
    validActivation(spec.activationId);
    this.assertAvailable(spec.pluginId);
  }

  private admit(spec: PluginTransitionSpec): Owner {
    this.validateSpec(spec);
    if (this.owners.size >= MAXIMUM_OPERATIONS)
      throw pluginProblem("Too many plugin changes are pending. Wait for them to finish.");
    const owner = this.createOwner(spec);
    this.owners.set(spec.pluginId, owner);
    return owner;
  }

  private createOwner(spec: PluginTransitionSpec): Owner {
    const now = new Date().toISOString();
    const owner: Owner = {
      record: Object.freeze({
        operationId: randomUUID(),
        pluginId: spec.pluginId,
        ...(spec.activationId === undefined ? {} : { activationId: spec.activationId }),
        operation: spec.operation,
        stage: "queued",
        state: "queued",
        startedAt: now,
        stageStartedAt: now,
        outstandingRequests: 0,
        outstandingConnections: 0,
        commit: "not-started",
      }),
      blocking: false,
      phase: Symbol(),
    };
    return owner;
  }

  private assertOwned(owner: Owner): void {
    if (this.owners.get(owner.record.pluginId) !== owner)
      throw pluginProblem("This plugin transition has already settled.");
  }

  private handle(owner: Owner, assertIntent?: () => void): PluginTransitionHandle {
    return {
      assertCurrent: (): void => {
        this.assertOwned(owner);
        this.assertOpen();
        assertIntent?.();
      },
      phase: (stage, metadata): void => this.phase(owner, stage, metadata),
      wait: async <T>(
        stage: PluginTransitionStage,
        run: () => Promise<T>,
        metadata?: PluginTransitionMetadata,
      ): Promise<T> => {
        this.phase(owner, stage, metadata);
        const phase = owner.phase;
        try {
          return await run();
        } finally {
          if (owner.phase === phase) {
            this.clearTimer(owner);
            const changed = this.sampleCounts(owner);
            if (owner.record.state === "waiting") {
              owner.record = Object.freeze({ ...owner.record, state: "running" });
              this.publish();
            } else if (changed) this.publish();
          }
        }
      },
    };
  }

  private phase(
    owner: Owner,
    stage: PluginTransitionStage,
    metadata: PluginTransitionMetadata = {},
  ): void {
    this.assertOwned(owner);
    if (stage === "queued" || !PLUGIN_TRANSITION_STAGES.includes(stage))
      throw pluginProblem("The plugin transition stage is invalid.");
    validActivation(metadata.activationId);
    this.clearTimer(owner);
    owner.phase = Symbol();
    if (metadata.counts !== undefined) owner.counts = metadata.counts;
    const stageStartedAt = new Date(
      Math.max(Date.now(), Date.parse(owner.record.stageStartedAt)),
    ).toISOString();
    owner.record = Object.freeze({
      ...owner.record,
      stage,
      state: "running",
      stageStartedAt,
      ...(metadata.activationId === undefined ? {} : { activationId: metadata.activationId }),
      ...(metadata.commit === undefined ? {} : { commit: metadata.commit }),
    });
    this.sampleCounts(owner);
    const phase = owner.phase;
    owner.timer = setTimeout(() => {
      if (this.owners.get(owner.record.pluginId) !== owner || owner.phase !== phase) return;
      this.sampleCounts(owner);
      owner.record = Object.freeze({ ...owner.record, state: "waiting" });
      this.publish();
    }, this.waitingAfterMs);
    owner.timer.unref();
    this.publish();
  }

  private sampleCounts(owner: Owner): boolean {
    if (owner.counts === undefined) return false;
    try {
      const counts = owner.counts();
      if (
        !Number.isSafeInteger(counts.requests) ||
        counts.requests < 0 ||
        !Number.isSafeInteger(counts.connections) ||
        counts.connections < 0
      )
        return false;
      if (
        owner.record.outstandingRequests === counts.requests &&
        owner.record.outstandingConnections === counts.connections
      )
        return false;
      owner.record = Object.freeze({
        ...owner.record,
        outstandingRequests: counts.requests,
        outstandingConnections: counts.connections,
      });
      return true;
    } catch {
      // Diagnostic collection cannot abandon the real hook or its cleanup ownership.
      return false;
    }
  }

  private clearTimer(owner: Owner): void {
    if (owner.timer !== undefined) clearTimeout(owner.timer);
    delete owner.timer;
  }

  private finish(owner: Owner, publish = true): void {
    this.clearTimer(owner);
    if (this.owners.get(owner.record.pluginId) !== owner) return;
    this.owners.delete(owner.record.pluginId);
    if (publish) this.publish();
  }

  private publish(): void {
    try {
      this.options.changed();
    } catch {
      // Progress observers cannot release ownership or roll back a committed operation.
    }
  }
}
