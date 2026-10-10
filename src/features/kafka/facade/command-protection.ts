import type { HostCommand, HostCommandName, HostError, HostCommandResponse } from "../contracts";
import { ACL_REVIEW_COMMAND_ACCESS } from "../contracts/acl-review-commands";
import type { KafkaOperationalPreferenceService } from "../application";
import { hasRecordMasking } from "../application/record-protection";

import { failureResponse } from "./facade-support";

type Access = "local" | "remote-read" | "remote-write" | "profile";

// Exhaustive by design: adding a host command requires an explicit access decision.
export const KAFKA_COMMAND_ACCESS = {
  "relationships.capture": "remote-read",
  "relationships.cancel": "local",
  "observations.capture": "remote-read",
  "observations.history": "local",
  "observations.cancel": "local",
  "observations.clear": "local",
  "connect.list": "remote-read",
  "connect.load": "remote-read",
  "connect.validate": "remote-read",
  "connect.review": "remote-read",
  "connect.apply": "remote-write",
  "environments.capture": "remote-read",
  "environments.review": "remote-read",
  "environments.apply": "remote-write",
  "schemas.client": "remote-read",
  "records.repair.list": "local",
  "records.repair.review": "remote-read",
  "records.repair.reconcile": "remote-read",
  "records.repair.archive": "local",
  "records.replay.review": "remote-read",
  "records.replay.apply": "remote-write",
  "records.replay.cancel": "local",
  "records.analysis.start": "remote-read",
  "records.locator.load": "remote-read",
  "records.locator.cancel": "local",
  "records.analysis.status": "local",
  "records.analysis.cancel": "local",
  "records.analysis.discard": "local",
  "records.export.start": "remote-read",
  "records.export.status": "local",
  "records.export.cancel": "local",
  "records.export.discard": "local",
  "records.trace": "remote-read",
  "records.trace.cancel": "local",
  "records.decode": "remote-read",
  "schemas.inspect": "remote-read",
  "schemas.samples": "remote-read",
  "schemas.author": "remote-read",
  "schemas.policy.load": "remote-read",
  "schemas.policy.review": "remote-read",
  "schemas.policy.apply": "remote-write",
  "schemas.change.review": "remote-read",
  "schemas.change.apply": "remote-write",
  "records.batch.review": "remote-read",
  "records.batch.apply": "remote-write",
  "records.batch.cancel": "local",
  "connection.test": "remote-read",
  "connection.connect": "remote-read",
  "connection.disconnect": "local",
  "profiles.list": "local",
  "profiles.binding.get": "local",
  "profiles.create": "local",
  "profiles.update": "local",
  "profiles.test": "profile",
  "profiles.delete": "local",
  "profiles.connect": "profile",
  "recipes.list": "local",
  "recipes.create": "local",
  "recipes.update": "local",
  "recipes.delete": "local",
  "recipes.usage": "local",
  "recipes.duplicate": "local",
  "recipes.import.preview": "local",
  "recipes.export": "local",
  "recipes.legacy.preview": "local",
  "recipes.legacy.convert": "local",
  "preferences.get": "local",
  "preferences.update": "local",
  "preferences.reset": "local",
  "rules.list": "local",
  "rules.create": "local",
  "rules.update": "local",
  "rules.delete": "local",
  "rules.validate": "local",
  "rules.evaluate": "local",
  "topics.list": "remote-read",
  "topics.change.review": "remote-read",
  "topics.change.apply": "remote-write",
  "writes.review": "remote-read",
  "writes.apply": "remote-write",
  "consumerGroups.delete.review": "remote-read",
  "consumerGroups.delete.apply": "remote-write",
  "consumerGroups.reset.review": "remote-read",
  "consumerGroups.reset.apply": "remote-write",
  "consumerGroups.list": "remote-read",
  "consumerGroups.load": "remote-read",
  "schemas.list": "remote-read",
  "schemas.load": "remote-read",
  "schemas.compatibility.check": "remote-read",
  "schemas.register": "remote-write",
  "schemas.delete": "remote-write",
  ...ACL_REVIEW_COMMAND_ACCESS,
  "acls.list": "remote-read",
  "acls.create": "remote-write",
  "acls.delete": "remote-write",
  "transforms.list": "remote-read",
  "transforms.load": "remote-read",
  "transforms.logs.load": "remote-read",
  "transforms.delete": "remote-write",
  "topicConfiguration.load": "remote-read",
  "topicConfiguration.validate": "remote-read",
  "topicConfiguration.apply": "remote-write",
  "topicConfiguration.history": "local",
  "clusterDetails.load": "remote-read",
  "clusterDetails.export": "local",
  "latency.start": "remote-write",
  "latency.stop": "local",
  "latency.export": "local",
  "catalog.list": "local",
  "catalog.load": "remote-read",
  "catalog.put": "remote-read",
  "catalog.delete": "local",
  "queries.list": "local",
  "queries.put": "local",
  "queries.delete": "local",
  "messages.start": "remote-read",
  "messages.continue": "remote-read",
  "messages.stop": "local",
  "trustAcquisition.hostKey.discover": "remote-read",
  "trustAcquisition.capabilities": "local",
  "trustAcquisition.editor.open": "local",
  "trustAcquisition.editor.advance": "local",
  "trustAcquisition.editor.close": "local",
  "trustAcquisition.apply": "remote-write",
  "trustAcquisition.material.fetch": "remote-write",
  "trustAcquisition.https.fetch": "remote-write",
  "trustAcquisition.discard": "local",
  "trustAcquisition.cancel": "local",
  "plugins.list": "local",
  "plugins.catalog": "local",
  "plugins.delivery": "local",
  "plugins.network.get": "local",
  "plugins.network.update": "local",
  "plugins.network.test": "local",
  "plugins.network.cancel": "local",
  "plugins.package.inspect": "local",
  "plugins.package.change.prepare": "remote-write",
  "plugins.package.install": "remote-write",
  "plugins.package.discard": "local",
  "plugins.change.prepare": "remote-write",
  "plugins.renderer.failed": "local",
  "plugins.install": "remote-write",
  "plugins.retry": "remote-write",
  "plugins.remove": "remote-write",
  "plugins.restart": "remote-write",
  "plugins.exit.prepare": "local",
  "plugins.exit.resolve": "remote-write",
  "plugin.execute": "remote-write",
} as const satisfies Record<HostCommandName, Access>;

interface ProtectionBindings {
  readonly preferences: KafkaOperationalPreferenceService;
  readonly disconnected: () => boolean;
  readonly pendingPluginWork?: () => Promise<boolean>;
  readonly managedProfile: (
    command: Extract<HostCommand, { command: "profiles.connect" | "profiles.test" }>,
  ) => Promise<boolean>;
  readonly rejected: (command: HostCommand, error: HostError) => void;
}

export class KafkaCommandProtection {
  private changing = false;
  private remoteOperations = 0;

  constructor(private readonly bindings: ProtectionBindings) {}

  async execute(
    command: HostCommand,
    correlationId: string,
    dispatch: () => Promise<HostCommandResponse>,
  ): Promise<HostCommandResponse> {
    const access = KAFKA_COMMAND_ACCESS[command.command];
    const changesProtection =
      command.command === "preferences.reset" ||
      (command.command === "preferences.update" && command.payload.patch.protection !== undefined);
    const change =
      changesProtection ||
      (command.command === "preferences.update" && command.payload.patch.codecs !== undefined);
    if (access === "local" && !change) return dispatch();
    const reject = (summary: string, recovery: string): HostCommandResponse => {
      const error: HostError = {
        activeStateChanged: false,
        code: "AUTHORIZATION_DENIED",
        correlationId,
        summary,
        recovery,
        retryable: false,
        stage: "authorization",
      };
      this.bindings.rejected(command, error);
      return failureResponse(command, error);
    };
    const snapshot = await this.bindings.preferences.get();
    if (this.changing)
      return reject(
        "Record decoding or protection is being changed.",
        "Wait for the preference save to finish, then retry.",
      );
    if (change) {
      if (!this.bindings.disconnected() || this.remoteOperations > 0)
        return reject(
          "Disconnect before changing record decoding or protection.",
          "Finish or cancel active requests, disconnect Kafka, then save decoding or protection settings.",
        );
      this.changing = true;
      try {
        let pending: boolean;
        try {
          pending = changesProtection && ((await this.bindings.pendingPluginWork?.()) ?? false);
        } catch {
          return reject(
            "Plugin work could not be verified.",
            "Review plugin status and finish pending capture or cleanup before changing protection.",
          );
        }
        if (pending)
          return reject(
            "Finish plugin work before changing protection.",
            "Remove or finish active capture resources and pending plugin cleanup, then save decoding or protection settings.",
          );
        return await dispatch();
      } finally {
        this.changing = false;
      }
    }
    if (snapshot.store.state !== "ready")
      return reject(
        "Record protection could not be loaded.",
        "Restore preference storage, or reset preferences while disconnected and review Protection before connecting.",
      );
    if (snapshot.preferences.protection.readOnly && access === "remote-write")
      return reject(
        "Read-only mode blocks this operation.",
        "Review Preferences → Protection. Broker permissions remain authoritative; disable read-only deliberately to perform remote changes or plugin actions.",
      );
    if (
      (command.command === "records.replay.review" ||
        command.command === "records.repair.review" ||
        command.command === "records.repair.reconcile" ||
        command.command === "records.decode") &&
      hasRecordMasking(snapshot.preferences.protection)
    )
      return reject(
        "Original-byte decoding and replay are unavailable while masking is active.",
        "Search, trace and compare the shared masked projection. Original-byte operations cannot bypass disclosure settings.",
      );
    this.remoteOperations += 1;
    try {
      if (
        snapshot.preferences.protection.readOnly &&
        (command.command === "profiles.connect" || command.command === "profiles.test") &&
        (await this.bindings.managedProfile(command))
      )
        return reject(
          "Read-only mode blocks managed plugin connections.",
          "Use an existing Kafka profile without plugin lifecycle hooks, or disable read-only deliberately before resuming a managed capture.",
        );
      return await dispatch();
    } finally {
      this.remoteOperations -= 1;
    }
  }
}
