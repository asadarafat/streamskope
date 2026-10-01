#!/usr/bin/env bash
# Shared checks; local runs also qualify the one-minute soak and live EDA.
set -euo pipefail

mode=local
if [[ $# -eq 1 && "$1" == --ci ]]; then
  mode=ci
elif [[ $# -ne 0 ]]; then
  echo "Usage: npm run check [-- --ci]" >&2
  exit 2
fi

bash tools/check/workflows.sh
npx --no-install prettier --check . --ignore-unknown
npx --no-install eslint . --max-warnings=0
npx --no-install tsc -b config/typescript/host.json config/typescript/renderer.json config/typescript/test.json config/typescript/packaging.json --pretty false
npx --no-install vitest run --config config/vitest.config.ts test/architecture test/unit test/integration
node tools/check/eda-source.mjs
(cd vendors/streamskope/apps/capture/agent && go test -race ./...)
node --import tsx tools/check/dependencies.ts
npm audit --package-lock-only --audit-level=high
if [[ "$mode" == local ]]; then
  node --import tsx test/performance/stream-pipeline-replay.ts --seconds=60 --rate=1000 --bytes=256 --mixed --clone
fi
npm run docs -- qualify
if [[ "$mode" == local ]]; then
  node --import tsx tools/check/eda-live.ts
  node --import tsx tools/check/nsp-live.ts
fi
