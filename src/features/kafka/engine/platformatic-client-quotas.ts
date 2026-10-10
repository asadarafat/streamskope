import { Admin, ClientQuotaMatchTypes, ResponseError, findErrorBy } from "@platformatic/kafka";

import {
  parseClientQuotaEntity,
  parseClientQuotaInput,
  parseClientQuotaSnapshot,
  sameClientQuotaEntity,
  sameClientQuotaBaseline,
  clientQuotaExpected,
  type ClientQuotaEntity,
  type ClientQuotaInput,
  type ClientQuotaSnapshot,
  type ClientQuotaOutcome,
} from "../contracts/client-quotas";

import { OwnedKafkaResources } from "./owned-kafka-resources";
import { platformaticClientOptions } from "./platformatic-options";
import type { KafkaClientInput } from "./types";

function entityReceipt(entries: unknown, entity: ClientQuotaEntity): number | undefined {
  if (!Array.isArray(entries) || entries.length !== 1) return undefined;
  const value: unknown = entries[0];
  if (
    typeof value !== "object" ||
    value === null ||
    !("errorCode" in value) ||
    !("entity" in value)
  )
    return undefined;
  if (
    !Number.isSafeInteger(value.errorCode) ||
    typeof value.errorCode !== "number" ||
    value.errorCode < -1 ||
    value.errorCode > 32767 ||
    !Array.isArray(value.entity)
  )
    return undefined;
  try {
    const actual = parseClientQuotaEntity(
      value.entity.map((item: unknown) => {
        if (
          typeof item !== "object" ||
          item === null ||
          !("entityType" in item) ||
          !("entityName" in item)
        )
          throw new Error("Incomplete quota receipt entity.");
        return { type: item.entityType, name: item.entityName };
      }),
    );
    return sameClientQuotaEntity(actual, entity) ? value.errorCode : undefined;
  } catch {
    return undefined;
  }
}
/** Strict explicit entities, actual per-entity receipts and original client cleanup. */
export class PlatformaticClientQuotas {
  private readonly resources: OwnedKafkaResources;
  constructor(
    private readonly input: KafkaClientInput,
    private readonly lifetime: AbortSignal,
  ) {
    this.resources = new OwnedKafkaResources(lifetime);
  }
  private owned<T>(run: (admin: Admin) => Promise<T>): Promise<{ value: T; cleaned: boolean }> {
    return this.resources.run(
      () => new Admin(platformaticClientOptions(this.input, "streamskope-client-quotas")),
      run,
    );
  }
  close(): Promise<void> {
    return this.resources.close();
  }
  private async inspect(admin: Admin, entity: ClientQuotaEntity): Promise<ClientQuotaSnapshot> {
    const [metadata, apis] = await Promise.all([
      admin.metadata({ topics: [], forceUpdate: true, autocreateTopics: false }),
      admin.listApis(),
    ]);
    if (!metadata.id || !apis.some((a) => a.apiKey === 48))
      throw new Error("Cluster identity or DescribeClientQuotas API is unavailable.");
    const entries = await admin.describeClientQuotas({
      strict: true,
      components: entity.map((c) =>
        c.name === null
          ? { entityType: c.type, matchType: ClientQuotaMatchTypes.DEFAULT }
          : { entityType: c.type, matchType: ClientQuotaMatchTypes.EXACT, match: c.name },
      ),
    });
    if (
      entries.length > 1 ||
      entries.some(
        (entry) =>
          !sameClientQuotaEntity(
            parseClientQuotaEntity(
              entry.entity.map((c) => ({ type: c.entityType, name: c.entityName })),
            ),
            entity,
          ),
      )
    )
      throw new Error("The strict quota response did not establish the exact selected entity.");
    return parseClientQuotaSnapshot({
      clusterId: metadata.id,
      entity,
      values: entries[0]?.values ?? [],
      alterSupported: apis.some((a) => a.apiKey === 49),
    });
  }
  async snapshot(entity: ClientQuotaEntity): Promise<ClientQuotaSnapshot> {
    const parsed = parseClientQuotaEntity(entity);
    const { value, cleaned } = await this.owned((admin) => this.inspect(admin, parsed));
    if (!cleaned) throw new Error("Original quota inspection cleanup is unresolved.");
    this.lifetime.throwIfAborted();
    return value;
  }
  async apply(input: ClientQuotaInput, baseline: ClientQuotaSnapshot): Promise<ClientQuotaOutcome> {
    input = parseClientQuotaInput(input);
    baseline = parseClientQuotaSnapshot(baseline);
    const initial: ClientQuotaOutcome = {
      input,
      state: "unsent",
      verification: "unavailable",
      observed: null,
      cleanup: "confirmed",
      detail: "Quota revalidation failed before admission. Inspect and review again.",
    };
    if (!this.resources.available)
      return {
        ...initial,
        cleanup: this.resources.cleanupUnresolved ? "unresolved" : "confirmed",
        detail:
          "The connection is revoked or original quota cleanup remains unresolved. No mutation was sent.",
      };
    const { value, cleaned } = await this.owned<ClientQuotaOutcome>(async (admin) => {
      let dispatched = false,
        result = initial;
      try {
        const fresh = await this.inspect(admin, input.entity);
        if (
          this.lifetime.aborted ||
          !sameClientQuotaBaseline(baseline, fresh) ||
          !fresh.alterSupported
        )
          return {
            ...result,
            detail:
              "Cluster, exact explicit quotas or API capabilities changed. No mutation was sent; inspect and review again.",
          };
        dispatched = true;
        const request: Parameters<Admin["alterClientQuotas"]>[0] = {
          validateOnly: false,
          entries: [
            {
              entities: input.entity.map((c) => ({ entityType: c.type, entityName: c.name })),
              ops: input.changes.map((c) =>
                c.value === null
                  ? { key: c.key, remove: true }
                  : { key: c.key, remove: false, value: c.value },
              ),
            },
          ],
        };
        let code: number | undefined;
        try {
          code = entityReceipt(await admin.alterClientQuotas(request), input.entity);
        } catch (error) {
          // The SDK throws a ResponseError for nonzero per-entity codes.
          // Inspect only its exact response; never reflect the server error text.
          const response: unknown = findErrorBy(
            error instanceof Error ? error : undefined,
            "code",
            ResponseError.code,
          )?.response;
          if (typeof response === "object" && response !== null && "entries" in response)
            code = entityReceipt(response.entries, input.entity);
          if (code === undefined || code === 0) throw error;
        }
        if (code === undefined)
          return {
            ...result,
            state: "unknown",
            detail:
              "Kafka returned no exact per-entity quota receipt. Inspect the selected entity before another attempt; no retry was made.",
          };
        if (code !== 0)
          return {
            ...result,
            state: code === -1 ? "unknown" : "rejected",
            detail:
              "Kafka returned an error for this quota entity (error code " +
              String(code) +
              "). Check cluster ALTER_CONFIGS permission and supported key/value combinations before another review." +
              (code === -1
                ? " The server reported an unknown error; its effect remains uncertain. Inspect the exact entity before another attempt."
                : ""),
          };
        result = {
          ...result,
          state: "acknowledged",
          detail:
            "Kafka acknowledged the exact quota entity. Readback and cleanup are separate evidence; untouched explicit keys are preserved.",
        };
        if (!this.lifetime.aborted) {
          try {
            const observed = await this.inspect(admin, input.entity);
            if (
              observed.clusterId === baseline.clusterId &&
              sameClientQuotaEntity(observed.entity, input.entity)
            )
              result = {
                ...result,
                observed: observed.values,
                verification:
                  JSON.stringify(observed.values) ===
                  JSON.stringify(clientQuotaExpected(baseline.values, input.changes))
                    ? "verified"
                    : "different",
              };
          } catch {
            /* Actual ACK survives unavailable readback. */
          }
        }
      } catch {
        if (result.state !== "acknowledged")
          result = {
            ...result,
            state: dispatched ? "unknown" : "unsent",
            detail: dispatched
              ? "The quota request may have reached Kafka. Inspect the exact entity before another attempt; no retry was made."
              : result.detail,
          };
      }
      return result;
    });
    return { ...value, cleanup: cleaned ? "confirmed" : "unresolved" };
  }
}
