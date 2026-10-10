#!/usr/bin/env bash
# Local qualification and the three complete, fixed GitHub qualification lanes.
set -euo pipefail

mode=local
lane=all
scope=core
local_stages=(shared soak docs)
if [[ $# -eq 1 && "$1" == --ci ]]; then
  mode=ci
elif [[ $# -eq 3 && "$1" == --ci && "$2" == --lane && "$3" =~ ^(shared|docs|runtime)$ ]]; then
  mode=ci
  lane="$3"
elif [[ $# -eq 1 && "$1" == --full ]]; then
  scope=full
  local_stages+=(eda-live nsp-live)
elif [[ $# -eq 2 && "$1" == --live && "$2" =~ ^(eda|nsp|all)$ ]]; then
  scope="$2"
  if [[ "$scope" == all ]]; then
    scope=live
    local_stages=(eda-live nsp-live)
  else
    local_stages=("$scope-live")
  fi
elif [[ $# -ne 0 ]]; then
  echo "Usage: npm run check [-- --full | --live eda|nsp|all | --ci [--lane shared|docs|runtime]]" >&2
  exit 2
fi

patch_dependencies() {
  node tools/check/forge-patch.ts --apply
  node --import tsx tools/check/build-dependency-patches.ts --apply
}

shared() {
  patch_dependencies
  bash tools/check/workflows.sh
  npx --no-install prettier --check . --ignore-unknown
  npx --no-install eslint . --max-warnings=0
  npx --no-install tsc -b config/typescript/host.json config/typescript/renderer.json config/typescript/test.json config/typescript/packaging.json --pretty false
  npx --no-install vitest run --config config/vitest.config.ts test/architecture test/unit test/integration
  node tools/check/eda-source.mjs
  (cd vendors/streamskope/apps/capture/agent && go test -race ./...)
  node --import tsx tools/check/dependencies.ts
  node --import tsx tools/check/audit.ts
}

docs() {
  patch_dependencies
  npm run docs -- qualify
}

runtime() {
  patch_dependencies
  npx --no-install vitest run --config config/vitest.config.ts --maxWorkers=1 --outputFile=.artifacts/ci/connection-profiles-real.json test/kafka/production-connection-profiles-real.test.ts
  npx --no-install vitest run --config config/vitest.config.ts --maxWorkers=1 --outputFile=.artifacts/ci/structured-records-real.json test/kafka/structured-records-real.test.ts test/kafka/resumable-search-real.test.ts test/kafka/streaming-export-real.test.ts test/kafka/record-locators-real.test.ts test/kafka/schema-authoring-real.test.ts test/kafka/schema-evolution-real.test.ts test/kafka/schema-policy-real.test.ts
  npx --no-install vitest run --config config/vitest.config.ts --maxWorkers=1 --outputFile=.artifacts/ci/observations-real.json test/kafka/observations-real.test.ts test/kafka/observation-replication-real.test.ts test/kafka/provider-stream-stop-real.test.ts
  npx --no-install vitest run --config config/vitest.config.ts --maxWorkers=1 --outputFile=.artifacts/ci/nats-real.json test/nats/provider-real.test.ts
  STREAMSKOPE_TEST_SUITE=production-startup node tools/package/e2e.mjs web test/e2e/web-production-startup.spec.ts
  cp test-results/web/production-startup/playwright-results.json .artifacts/ci/production-startup.json
  STREAMSKOPE_TEST_SUITE=workbench node tools/package/e2e.mjs web \
    test/e2e/web-stream-monitor.spec.ts \
    test/e2e/web-observations-recovery.spec.ts \
    test/e2e/web-responsive-workbench.spec.ts \
    --grep 'keyboard-operable|investigates real lag|persistent resource hierarchy|expanded dock in both themes|main resource page primary'
  STREAMSKOPE_TEST_SUITE=nats-workspace node tools/package/e2e.mjs web test/e2e/web-nats-workspace.spec.ts
  STREAMSKOPE_TEST_SUITE=structured-records node tools/package/e2e.mjs web test/e2e/web-structured-events.spec.ts test/e2e/web-streaming-export.spec.ts test/e2e/web-record-analysis.spec.ts test/e2e/web-investigation-views.spec.ts test/e2e/web-record-bookmarks.spec.ts test/e2e/web-portable-views.spec.ts test/e2e/web-topic-notes.spec.ts test/e2e/web-schema-authoring.spec.ts test/e2e/web-schema-evolution.spec.ts test/e2e/web-schema-policy.spec.ts
  STREAMSKOPE_TEST_SUITE=plugin-lifecycle node tools/package/e2e.mjs web test/e2e/web-plugin-installation.spec.ts
}

ci_lane() {
  local selected="$1" started status
  node tools/check/ci-evidence.ts prepare "$selected"
  started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # A subshell preserves fail-fast behavior while its parent records a failed lane.
  set +e
  (set -e; "$selected")
  status=$?
  set -e
  node tools/check/ci-evidence.ts record "$selected" "$started" "$status"
  return "$status"
}

if [[ "$mode" == ci ]]; then
  if [[ "$lane" == all ]]; then
    for selected in shared docs runtime; do ci_lane "$selected"; done
  else
    ci_lane "$lane"
  fi
else
  qualification=$(node --import tsx tools/check/qualification.ts begin "$scope")
  finish_local() {
    local status=$? evidence_status
    trap - EXIT INT TERM
    set +e
    node --import tsx tools/check/qualification.ts finish "$qualification" "$status"
    evidence_status=$?
    if [[ $status -eq 0 ]]; then status=$evidence_status; fi
    exit "$status"
  }
  trap finish_local EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  # Explicit live runs require every selected target before either harness can mutate it.
  # Full qualification preserves each harness's existing configured-or-skipped behavior.
  if [[ "$scope" == eda || "$scope" == live ]]; then
    if [[ -z "${STREAMSKOPE_EDA_API_URL:-}" || -z "${STREAMSKOPE_EDA_API_USERNAME:-}" || -z "${STREAMSKOPE_EDA_API_PASSWORD:-}" ]]; then
      echo "Live EDA requires STREAMSKOPE_EDA_API_URL, STREAMSKOPE_EDA_API_USERNAME and STREAMSKOPE_EDA_API_PASSWORD together." >&2
      exit 1
    fi
    if [[ -n "${STREAMSKOPE_EDA_API_CA:-}" && ( ! -f "$STREAMSKOPE_EDA_API_CA" || ! -r "$STREAMSKOPE_EDA_API_CA" || ! -s "$STREAMSKOPE_EDA_API_CA" ) ]]; then
      echo "Live EDA requires STREAMSKOPE_EDA_API_CA to name a readable, nonempty certificate file when provided." >&2
      exit 1
    fi
  fi
  if [[ "$scope" == nsp || "$scope" == live ]]; then
    if [[ -z "${STREAMSKOPE_NSP_CONFIG:-}" || ! -f "$STREAMSKOPE_NSP_CONFIG" || ! -r "$STREAMSKOPE_NSP_CONFIG" || ! -s "$STREAMSKOPE_NSP_CONFIG" ]]; then
      echo "Live NSP requires STREAMSKOPE_NSP_CONFIG to name a readable, nonempty configuration file." >&2
      exit 1
    fi
  fi
  if [[ "$scope" == eda || "$scope" == nsp || "$scope" == live ]]; then
    patch_dependencies
  fi
  for selected in "${local_stages[@]}"; do
    node --import tsx tools/check/qualification.ts stage "$qualification" "$selected"
    case "$selected" in
      shared|docs) "$selected" ;;
      soak) node --import tsx test/performance/stream-pipeline-replay.ts --seconds=60 --rate=1000 --bytes=256 --mixed --clone ;;
      eda-live|nsp-live) node --import tsx "tools/check/$selected.ts" ;;
    esac
    node --import tsx tools/check/qualification.ts complete "$qualification" "$selected"
  done
fi
