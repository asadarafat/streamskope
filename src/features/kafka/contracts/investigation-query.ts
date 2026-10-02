import { parseKafkaSearchFilter, type KafkaSearchFilter } from "./query-search";
import { parseKafkaFetchRequest, parseKafkaTimestamp } from "./fetch-validation";
import type { KafkaFetchRequest } from "./types";
import { HostContractValidationError } from "./validation-error";
import { exactKeys, record } from "./validation-primitives";

/** Configuration only: no connection credentials, resolved secrets or message data. */
export interface KafkaInvestigationQuery {
  readonly schemaVersion: 1;
  readonly filters?: KafkaSearchFilter;
  readonly request: KafkaFetchRequest;
}

export function parseKafkaInvestigationQuery(value: unknown): KafkaInvestigationQuery {
  const query = record(value, "query");
  exactKeys(query, ["schemaVersion", "request", "filters"], "query");
  if (query.schemaVersion !== 1) {
    throw new HostContractValidationError("query.schemaVersion", "unsupported query version");
  }
  return {
    schemaVersion: 1,
    ...(query.filters === undefined
      ? {}
      : { filters: parseKafkaSearchFilter(query.filters, "query.filters") }),
    request: parseKafkaFetchRequest(query.request, "query.request"),
  };
}

/** Parse an explicit offset, never the machine's implicit local time zone. */
export function parseKafkaQueryTimestamp(value: string, label: string): number {
  const parts =
    value.length <= 40
      ? /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|[+-]\d{2}:\d{2})$/u.exec(
          value,
        )
      : null;
  if (parts === null) {
    throw new HostContractValidationError(
      label,
      "use ISO 8601 with seconds and Z or an explicit UTC offset (for example +02:00)",
    );
  }
  const year = Number(parts[1]);
  const month = Number(parts[2]);
  const day = Number(parts[3]);
  const hour = Number(parts[4]);
  const minute = Number(parts[5]);
  const second = Number(parts[6]);
  const calendar = new Date(0);
  calendar.setUTCFullYear(year, month - 1, day);
  calendar.setUTCHours(hour, minute, second, 0);
  // Date.parse normalizes invalid calendar dates; reject that silent correction.
  if (
    calendar.getUTCFullYear() !== year ||
    calendar.getUTCMonth() !== month - 1 ||
    calendar.getUTCDate() !== day ||
    calendar.getUTCHours() !== hour ||
    calendar.getUTCMinutes() !== minute ||
    calendar.getUTCSeconds() !== second
  ) {
    throw new HostContractValidationError(label, "must be a valid calendar date and time");
  }
  return parseKafkaTimestamp(Date.parse(value), label);
}
