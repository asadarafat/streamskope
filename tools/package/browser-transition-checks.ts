import { BROWSER_UPGRADE_CHECKS } from "./browser-upgrade-evidence";
import type { TransitionOptions } from "./browser-upgrade-smoke";

/** Describe only the selected native scenarios after their execution succeeds. */
export function nativeTransitionChecks(
  options: TransitionOptions,
  local: boolean,
  queryFormat: 1 | 2 | 3 | 4,
): readonly string[] {
  return !local
    ? BROWSER_UPGRADE_CHECKS
    : [
        ...BROWSER_UPGRADE_CHECKS.map((check) =>
          options.observationRecovery &&
          check === "genuine predecessor query remains unchanged until explicit view migration"
            ? "saved-view format4 reads preserve bytes before explicit update"
            : options.observationRecovery &&
                check === "incompatible view rollback refuses without changing the running host"
              ? "incompatible observation rollback refuses without changing the running host"
              : options.observationRecovery && check === "exact published predecessor"
                ? "exact immutable local-staged observation-format1 predecessor"
                : check === "exact published predecessor"
                  ? `exact previously qualified local-staged version-${String(queryFormat)} predecessor`
                  : check === "no-argument resume preserves rolled-back release"
                    ? "exact local-staged rolled-back owner restarts with preserved installation"
                    : options.repairRecovery &&
                        check ===
                          "genuine predecessor query remains unchanged until explicit view migration"
                      ? "genuine predecessor saved view remains unchanged during repair migration"
                      : options.repairRecovery &&
                          check ===
                            "incompatible view rollback refuses without changing the running host"
                        ? "incompatible repair rollback refuses without changing the running host"
                        : check,
        ),
        options.observationRecovery
          ? "published installation upgraded to actual observation-format1 predecessor with complete backup"
          : `published installation upgraded to actual local version-${String(queryFormat)} predecessor with complete backup`,
        queryFormat === 4
          ? options.observationRecovery
            ? "saved-view format4 preserved; observation migration qualified separately"
            : "saved-view format4 preserved; repair recovery migration qualified separately"
          : queryFormat === 3
            ? "view-only format3-to-format4 migration; real broker catalog writes qualified separately"
            : "locator metadata persistence only; real record reload qualified separately",
        ...(options.repairRecovery
          ? [
              `actual old repair host reads independently encrypted format${String(options.repairRecovery.from)} jobs before upgrade`,
              "new host legacy listing preserves exact encrypted bytes, inode and modification time",
              `explicit archive migrates to format${String(options.repairRecovery.to)} and retains exact private encrypted predecessor`,
              "uncertain repair remains protected across native vault restart without retry or archive",
              `old format${String(options.repairRecovery.from)} target refuses format${String(options.repairRecovery.to)} before stopping the installed owner`,
              `complete backup restoration retains changed format${String(options.repairRecovery.to)} and predecessor in recovery`,
              `actual old repair host lists original format${String(options.repairRecovery.from)} jobs after complete restore and rollback`,
            ]
          : []),
        ...(options.observationRecovery
          ? [
              "exact format1 predecessor observation reads preserve original bytes, inode and time",
              "candidate history access migrates to format2 and physically removes expired active measurements",
              "migration retains the exact private format1 predecessor",
              "desired settings survive actual native host restart without watch authority",
              "exact old inspector independently refuses format2 without a new backup path",
              "whole-backup restore preserves changed format2 and predecessor in recovery",
              "rolled-back original host reads exact restored format1 bytes",
            ]
          : []),
        ...(options.repairHistory
          ? [
              "independently encrypted interrupted repair job survives native vault restart without retry",
              "exact predecessor refuses repair journal before view migration",
              "full-backup restoration preserves the encrypted repair journal in changed-data recovery",
            ]
          : []),
      ];
}
