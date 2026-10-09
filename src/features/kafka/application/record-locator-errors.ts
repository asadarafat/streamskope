import type { KafkaRecordLocatorReason } from "../contracts/record-locator";

export type KafkaRecordLocatorFailureReason = KafkaRecordLocatorReason;

const DETAILS: Record<KafkaRecordLocatorFailureReason, string> = {
  expired: "Kafka no longer retains this offset in the identified topic partition.",
  "resource-replaced": "The connected Kafka cluster or topic has a different identity.",
  "record-replaced": "This offset belongs to a different record history than the saved position.",
  "topic-missing": "Kafka reports that the requested topic no longer exists.",
  inaccessible:
    "Kafka denied access to the saved record. Check the current connection permissions.",
  "record-missing": "The verified offset interval contains no readable record at this position.",
  unavailable: "The current read could not establish the saved record's identity or availability.",
  cancelled: "The record reload was cancelled and its reader was closed.",
  revoked: "The connection or record settings changed. Reload using the current connection.",
};

export class KafkaRecordLocatorError extends Error {
  constructor(readonly reason: KafkaRecordLocatorFailureReason) {
    super(DETAILS[reason]);
    this.name = "KafkaRecordLocatorError";
  }
}

export class UnknownRecordLocatorRequestError extends Error {
  constructor() {
    super("This record reload request is not owned by the current host.");
    this.name = "UnknownRecordLocatorRequestError";
  }
}

export class RecordLocatorOperationError extends Error {
  constructor(readonly reason: "busy" | "reused-request" | "cleanup") {
    super(
      reason === "busy"
        ? "Finish or cancel the current record reload before starting another."
        : reason === "reused-request"
          ? "Start a new record reload with a fresh request identifier."
          : "Record reload cleanup is not confirmed. Retry Stop for the same request before continuing.",
    );
    this.name = "RecordLocatorOperationError";
  }
}
