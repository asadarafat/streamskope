#!/usr/bin/env bash
# Local qualification and the three complete, fixed GitHub qualification lanes.
set -euo pipefail

mode=local
lane=all
if [[ $# -eq 1 && "$1" == --ci ]]; then
  mode=ci
elif [[ $# -eq 3 && "$1" == --ci && "$2" == --lane && "$3" =~ ^(shared|docs|runtime)$ ]]; then
  mode=ci
  lane="$3"
elif [[ $# -ne 0 ]]; then
  echo "Usage: npm run check [-- --ci [--lane shared|docs|runtime]]" >&2
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
  shared
  node --import tsx test/performance/stream-pipeline-replay.ts --seconds=60 --rate=1000 --bytes=256 --mixed --clone
  docs
  node --import tsx tools/check/eda-live.ts
  node --import tsx tools/check/nsp-live.ts
fi
