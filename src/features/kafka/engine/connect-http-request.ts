import type { KafkaClusterServiceContext } from "../application/types";
import type { ConnectMutationReceipt } from "../application/connect-service";

import type {
  BoundedJsonHttpPort,
  BoundedJsonHttpRequest,
  OwnedJsonHttpPort,
} from "./bounded-json-http";
import { boundedHttpCleanup, type OwnedHttpRequest } from "./owned-http-request";

function owned(http: BoundedJsonHttpPort): http is OwnedJsonHttpPort {
  return "open" in http && typeof http.open === "function";
}
async function input(
  context: KafkaClusterServiceContext,
  signal: AbortSignal,
  method: BoundedJsonHttpRequest["method"],
  path: string,
  body?: unknown,
): Promise<BoundedJsonHttpRequest> {
  signal = context.signal ? AbortSignal.any([signal, context.signal]) : signal;
  signal.throwIfAborted();
  const authorization = await context.authorization(signal);
  signal.throwIfAborted();
  return {
    url: `${context.baseUrl.replace(/\/+$/u, "")}${path}`,
    method,
    signal,
    contentType: "application/json",
    ...(body === undefined ? {} : { body }),
    ...(authorization === undefined ? {} : { authorization }),
    ...(context.caPem === undefined ? {} : { caPem: context.caPem }),
    ...(context.clientIdentity === undefined ? {} : { clientIdentity: context.clientIdentity }),
  };
}
/** Reads settle only after original close proof. Test-connection contexts can be read-only. */
export async function readConnectHttp(
  http: BoundedJsonHttpPort,
  context: KafkaClusterServiceContext,
  signal: AbortSignal,
  method: BoundedJsonHttpRequest["method"],
  path: string,
  body?: unknown,
): Promise<{ status: number; body: unknown }> {
  const run = async (): Promise<{ status: number; body: unknown }> => {
    const request = await input(context, signal, method, path, body);
    if (!context.requestOwner) return http.request(request);
    if (!owned(http)) throw new Error("Original HTTP request ownership is unavailable.");
    const result = await context.requestOwner.run(
      () => {
        const lease = http.open(request);
        return { lease, close: (): Promise<void> => boundedHttpCleanup(lease) };
      },
      ({ lease }) => lease.response,
    );
    if (!result.cleaned) throw new Error("Original HTTP cleanup remains unresolved.");
    return result.value;
  };
  return context.requestOwner ? context.requestOwner.runWork(run) : run();
}
/** One attempt, with actual status separate from dispatch, readback and local cleanup. */
export async function mutateConnectHttp(
  http: BoundedJsonHttpPort,
  context: KafkaClusterServiceContext,
  signal: AbortSignal,
  method: BoundedJsonHttpRequest["method"],
  path: string,
  body?: unknown,
): Promise<ConnectMutationReceipt> {
  let lease: OwnedHttpRequest | undefined;
  const refused = (
    detail = "Original Connect authority is unavailable. No action was sent; reconnect after resolving cleanup.",
  ): ConnectMutationReceipt => ({
    state: "rejected",
    dispatch: "not-sent",
    cleanup: context.requestOwner?.cleanupUnresolved ? "unresolved" : "confirmed",
    detail,
  });
  if (!context.requestOwner || !owned(http) || !context.requestOwner.available) return refused();
  try {
    return await context.requestOwner.runWork(async () => {
      const request = {
        ...(await input(context, signal, method, path, body)),
        responseMode: "status" as const,
      };
      const result = await context.requestOwner!.run(
        () => {
          lease = http.open(request);
          return { lease, close: (): Promise<void> => boundedHttpCleanup(lease!) };
        },
        async ({ lease }): Promise<Omit<ConnectMutationReceipt, "cleanup">> => {
          try {
            const response = await lease.response;
            if (response.status >= 200 && response.status < 300)
              return {
                state: "acknowledged",
                dispatch: "attempted",
                detail:
                  "Connect accepted the action. State changes are asynchronous; acknowledgement does not prove the requested state. No automatic retry was sent.",
              };
            if ([400, 401, 403, 404, 405, 409, 422].includes(response.status))
              return {
                state: "rejected",
                dispatch: "attempted",
                detail: `Connect rejected the request (HTTP ${response.status}). Check API permissions and current configuration.`,
              };
            return {
              state: "unknown",
              dispatch: "attempted",
              detail:
                "Connect did not acknowledge the action. Inspect its state before another review; no automatic retry was sent.",
            };
          } catch {
            return lease.dispatched()
              ? {
                  state: "unknown",
                  dispatch: "attempted",
                  detail:
                    "The action outcome is unknown. Inspect Connect before another review; no automatic retry was sent.",
                }
              : {
                  state: "rejected",
                  dispatch: "not-sent",
                  detail:
                    "The request was not sent. Check endpoint authentication and certificate trust before reviewing again.",
                };
          }
        },
      );
      return { ...result.value, cleanup: result.cleaned ? "confirmed" : "unresolved" };
    });
  } catch {
    return lease?.dispatched()
      ? {
          state: "unknown",
          dispatch: "attempted",
          cleanup: "unresolved",
          detail:
            "The original request did not return a complete receipt. Inspect Connect and resolve cleanup before another action.",
        }
      : refused(
          "Connect could not authorize the request before dispatch. Check endpoint credentials, reachability and certificate trust. No action was sent.",
        );
  }
}
