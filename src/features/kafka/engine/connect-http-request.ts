import type { KafkaClusterServiceContext } from "../application/types";
import type { ConnectMutationReceipt } from "../application/connect-service";

import type { BoundedJsonHttpPort, BoundedJsonHttpRequest } from "./bounded-json-http";
import { boundedHttpCleanup, type OwnedHttpRequest } from "./owned-http-request";
import { ownedServiceHttp, serviceHttpInput } from "./service-http-request";
export { readServiceHttp as readConnectHttp } from "./service-http-request";

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
  if (!context.requestOwner || !ownedServiceHttp(http) || !context.requestOwner.available)
    return refused();
  try {
    return await context.requestOwner.runWork(async () => {
      const request = {
        ...(await serviceHttpInput(context, signal, method, path, body)),
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
