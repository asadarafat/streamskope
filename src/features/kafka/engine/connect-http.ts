import type { ConnectInput, ConnectInventory, ConnectValidation } from "../contracts/connect";
import { connectConfig, connectName } from "../contracts/connect";
import type { KafkaClusterServiceContext } from "../application";
import type {
  ConnectPort,
  ConnectState,
  ConnectRelationships,
} from "../application/connect-service";

import type { BoundedJsonHttpPort } from "./bounded-json-http";

export class ConnectHttpError extends Error {
  constructor(readonly status: number) {
    super(`Connect HTTP ${status}.`);
  }
}
function object(v: unknown): Record<string, unknown> {
  if (!v || typeof v !== "object" || Array.isArray(v)) throw new Error("Invalid Connect response.");
  return v as Record<string, unknown>;
}
function array(v: unknown): readonly unknown[] {
  if (!Array.isArray(v) || v.length > 1000) throw new Error("Connect response exceeds limit.");
  return v;
}
function string(v: unknown, max = 512): string {
  if (typeof v !== "string" || v.length > max || !v) throw new Error("Invalid Connect field.");
  return v;
}
function state(v: unknown): string {
  const s = string(v, 80);
  return ["UNASSIGNED", "RUNNING", "PAUSED", "FAILED", "RESTARTING", "STOPPED"].includes(s)
    ? s
    : "UNKNOWN";
}
function safeConfig(config: Readonly<Record<string, string>>): Readonly<Record<string, string>> {
  return Object.fromEntries(
    Object.entries(config).map(([k, v]) => [
      k,
      [
        "name",
        "connector.class",
        "tasks.max",
        "topics",
        "errors.tolerance",
        "errors.deadletterqueue.topic.name",
        "errors.deadletterqueue.context.headers.enable",
        "key.converter",
        "value.converter",
      ].includes(k) && /^[a-zA-Z0-9._, -]{0,512}$/u.test(v)
        ? v
        : "[protected — retained unless replaced]",
    ]),
  );
}
function failureHint(trace: unknown): string {
  if (typeof trace !== "string" || !trace) return "";
  if (/Authorization|Authentication|AccessDenied/u.test(trace))
    return "Access failed. Check connector credentials and Kafka permissions in the worker logs.";
  if (/DataException|SerializationException|Schema/u.test(trace))
    return "Record conversion failed. Check converters and schemas; inspect configured DLQ records if supported.";
  return "Task failed. Inspect the worker logs securely for the cause, then review restart of failed tasks.";
}
export class ConnectHttpAdapter implements ConnectPort {
  constructor(private readonly http: BoundedJsonHttpPort) {}
  private async request(
    c: KafkaClusterServiceContext,
    signal: AbortSignal,
    method: "GET" | "POST" | "PUT" | "DELETE",
    path: string,
    body?: unknown,
  ): Promise<{ status: number; body: unknown }> {
    const authorization = await c.authorization();
    signal.throwIfAborted();
    return this.http.request({
      url: `${c.baseUrl.replace(/\/+$/u, "")}${path}`,
      method,
      signal,
      contentType: "application/json",
      ...(body === undefined ? {} : { body }),
      ...(authorization === undefined ? {} : { authorization }),
      ...(c.caPem === undefined ? {} : { caPem: c.caPem }),
    });
  }
  private ok(r: { status: number; body: unknown }): unknown {
    if (r.status < 200 || r.status >= 300) throw new ConnectHttpError(r.status);
    return r.body;
  }
  async list(c: KafkaClusterServiceContext, signal: AbortSignal): Promise<ConnectInventory> {
    const names = array(this.ok(await this.request(c, signal, "GET", "/connectors")))
      .map(connectName)
      .sort();
    const plugins = array(this.ok(await this.request(c, signal, "GET", "/connector-plugins")))
      .map((v) => string(object(v).class))
      .sort();
    return { names, plugins };
  }
  async load(
    c: KafkaClusterServiceContext,
    name: string,
    signal: AbortSignal,
  ): Promise<ConnectState | null> {
    const path = `/connectors/${encodeURIComponent(name)}`;
    const r = await this.request(c, signal, "GET", `${path}/config`);
    if (r.status === 404) return null;
    const config = connectConfig(this.ok(r));
    const s = object(this.ok(await this.request(c, signal, "GET", `${path}/status`)));
    const tasks = array(s.tasks)
      .map((v) => {
        const t = object(v);
        if (!Number.isSafeInteger(t.id) || Number(t.id) < 0) throw new Error("Invalid task id.");
        return { id: Number(t.id), state: state(t.state), failure: failureHint(t.trace) };
      })
      .sort((a, b) => a.id - b.id);
    const dlq = config["errors.deadletterqueue.topic.name"];
    return {
      config,
      detail: {
        name,
        state: state(object(s.connector).state),
        tasks,
        config: safeConfig(config),
        dlq: dlq && /^[a-zA-Z0-9._-]{1,249}$/u.test(dlq) ? dlq : null,
        observedAt: new Date().toISOString(),
      },
    };
  }
  async clusterId(c: KafkaClusterServiceContext, signal: AbortSignal): Promise<string | null> {
    const info = object(this.ok(await this.request(c, signal, "GET", "/")));
    return typeof info.kafka_cluster_id === "string" && info.kafka_cluster_id.length <= 512
      ? info.kafka_cluster_id
      : null;
  }
  async relationships(
    c: KafkaClusterServiceContext,
    name: string,
    signal: AbortSignal,
  ): Promise<ConnectRelationships> {
    const path = `/connectors/${encodeURIComponent(connectName(name))}`;
    const info = object(this.ok(await this.request(c, signal, "GET", path)));
    const config = connectConfig(info.config);
    const topic = (v: unknown): string => {
      const t = string(v, 249);
      if (!/^[A-Za-z0-9._-]+$/.test(t) || t === "." || t === "..")
        throw new Error("Invalid Connect topic.");
      return t;
    };
    const configuredTopics = config.topics
      ? [...new Set(config.topics.split(",").map((s) => topic(s.trim())))]
      : [];
    let reportedTopics: readonly string[] | null = null;
    try {
      const tracked = object(this.ok(await this.request(c, signal, "GET", `${path}/topics`)));
      reportedTopics = [...new Set(array(object(tracked[name]).topics).map(topic))];
    } catch {
      signal.throwIfAborted();
    }
    return {
      type: info.type === "source" || info.type === "sink" ? info.type : "unknown",
      reportedTopics,
      configuredTopics,
      regexSubscription: Boolean(config["topics.regex"]),
    };
  }
  async validate(
    c: KafkaClusterServiceContext,
    config: Readonly<Record<string, string>>,
    signal: AbortSignal,
  ): Promise<ConnectValidation> {
    const issues: { field: string; message: string }[] = [];
    const type = config["connector.class"];
    if (!type || !/^[A-Za-z0-9_.$]+$/u.test(type))
      return {
        issues: [{ field: "connector.class", message: "Choose an installed connector class." }],
      };
    if (config["tasks.max"] !== undefined && !/^[1-9][0-9]{0,3}$/u.test(config["tasks.max"]))
      issues.push({ field: "tasks.max", message: "Enter an integer from 1 to 9999." });
    if (config["errors.tolerance"] === "all" && !config["errors.deadletterqueue.topic.name"])
      issues.push({
        field: "errors.deadletterqueue.topic.name",
        message:
          "This client requires a DLQ topic when tolerating all errors. Confirm your sink supports a DLQ.",
      });
    const r = object(
      this.ok(
        await this.request(
          c,
          signal,
          "PUT",
          `/connector-plugins/${encodeURIComponent(type)}/config/validate`,
          config,
        ),
      ),
    );
    if (!Number.isSafeInteger(r.error_count) || Number(r.error_count) < 0)
      throw new Error("Invalid validation response.");
    for (const v of array(r.configs)) {
      const def = object(v);
      const val = object(def.value);
      const errors = array(val.errors);
      if (errors.length) {
        const name = object(def.definition).name;
        issues.push({
          field:
            typeof name === "string" && /^[A-Za-z0-9_.-]{1,200}$/u.test(name)
              ? name
              : "configuration",
          message:
            "Connect rejected this field. Check the connector's required type, supported values and worker prerequisites. Remote error text is withheld because it may contain credentials.",
        });
      }
    }
    if (Number(r.error_count) > 0 && !issues.length)
      issues.push({
        field: "configuration",
        message: "Connect reported validation errors. Inspect worker diagnostics securely.",
      });
    return { issues };
  }
  async apply(
    c: KafkaClusterServiceContext,
    input: ConnectInput,
    signal: AbortSignal,
  ): Promise<void> {
    const path = `/connectors/${encodeURIComponent(input.name)}`;
    switch (input.action) {
      case "create":
        this.ok(
          await this.request(c, signal, "POST", "/connectors", {
            name: input.name,
            config: input.config,
          }),
        );
        break;
      case "update":
        this.ok(await this.request(c, signal, "PUT", `${path}/config`, input.config));
        break;
      case "delete":
        this.ok(await this.request(c, signal, "DELETE", path));
        break;
      case "pause":
      case "resume":
        this.ok(await this.request(c, signal, "PUT", `${path}/${input.action}`));
        break;
      case "restart-failed":
        this.ok(
          await this.request(
            c,
            signal,
            "POST",
            `${path}/restart?includeTasks=true&onlyFailed=true`,
          ),
        );
        break;
    }
  }
}
