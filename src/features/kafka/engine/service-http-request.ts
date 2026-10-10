import type { KafkaClusterServiceContext } from "../application/types";
import type {
  BoundedJsonHttpPort,
  BoundedJsonHttpRequest,
  OwnedJsonHttpPort,
} from "./bounded-json-http";
import { boundedHttpCleanup } from "./owned-http-request";

export function ownedServiceHttp(http: BoundedJsonHttpPort): http is OwnedJsonHttpPort {
  return "open" in http && typeof http.open === "function";
}
export async function serviceHttpInput(
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
export async function readServiceHttp(
  http: BoundedJsonHttpPort,
  context: KafkaClusterServiceContext,
  signal: AbortSignal,
  method: BoundedJsonHttpRequest["method"],
  path: string,
  body?: unknown,
): Promise<{ status: number; body: unknown }> {
  const run = async (): Promise<{ status: number; body: unknown }> => {
    const request = await serviceHttpInput(context, signal, method, path, body);
    if (!context.requestOwner) return http.request(request);
    if (!ownedServiceHttp(http)) throw new Error("Original HTTP request ownership is unavailable.");
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
