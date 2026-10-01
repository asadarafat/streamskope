import { Worker } from "node:worker_threads";

import type {
  KafkaProfileTrustDecoder,
  KafkaProfileTrustDecoderInput,
  KafkaProfileTrustDecoderResult,
} from "../application";

import {
  KafkaTrustMaterialError,
  KafkaTruststorePasswordError,
  parsePemTrustMaterial,
} from "./trust-material-shared";

export interface TrustMaterialWorkerOptions {
  readonly execArgv?: readonly string[];
  readonly script: string;
}

export type TrustMaterialWorkerReply =
  | {
      readonly ok: true;
      readonly result: KafkaProfileTrustDecoderResult;
    }
  | {
      readonly code: "TRUST_MATERIAL" | "TRUSTSTORE_PASSWORD";
      readonly ok: false;
    };

function cancellationReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new DOMException("The trust-material operation was cancelled.", "AbortError");
}

function workerFailure(reply: TrustMaterialWorkerReply): Error {
  return !reply.ok && reply.code === "TRUSTSTORE_PASSWORD"
    ? new KafkaTruststorePasswordError()
    : new KafkaTrustMaterialError();
}

export class StreamSkopeTrustMaterialDecoder implements KafkaProfileTrustDecoder {
  constructor(private readonly worker: TrustMaterialWorkerOptions) {}

  decode(
    input: KafkaProfileTrustDecoderInput,
    signal?: AbortSignal,
  ): Promise<KafkaProfileTrustDecoderResult> {
    if (input.kind === "pem") {
      return Promise.resolve().then(() => {
        signal?.throwIfAborted();
        const result = parsePemTrustMaterial(input);
        signal?.throwIfAborted();
        return result;
      });
    }
    if (signal?.aborted === true) {
      return Promise.reject(cancellationReason(signal));
    }
    return new Promise((resolve, reject) => {
      const worker = new Worker(this.worker.script, {
        ...(this.worker.execArgv === undefined ? {} : { execArgv: [...this.worker.execArgv] }),
        workerData: input,
      });
      let settled = false;

      const finish = (complete: () => void): void => {
        if (settled) {
          return;
        }
        settled = true;
        signal?.removeEventListener("abort", abort);
        worker.removeAllListeners();
        void worker.terminate();
        complete();
      };
      const abort = (): void => {
        finish(() => {
          reject(
            signal === undefined
              ? new DOMException("Cancelled.", "AbortError")
              : cancellationReason(signal),
          );
        });
      };
      worker.once("message", (value: unknown) => {
        const reply = value as TrustMaterialWorkerReply;
        finish(() => {
          if (reply.ok) {
            resolve(reply.result);
          } else {
            reject(workerFailure(reply));
          }
        });
      });
      worker.once("error", () => {
        finish(() => {
          reject(new KafkaTrustMaterialError());
        });
      });
      worker.once("exit", () => {
        finish(() => {
          reject(new KafkaTrustMaterialError());
        });
      });
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted === true) {
        abort();
      }
    });
  }
}
