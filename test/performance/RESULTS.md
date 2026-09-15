# Sustained-stream performance evidence — 2026-09-14

## Scope and method

Baseline: commit `63b66e4`; candidate: working-tree changes accompanying this report.
Linux ARM64 OrbStack VM, eight logical Apple-host CPUs, 28,089 MiB assigned RAM.
The guest did not expose a CPU model. The shared VM also ran containers and some
verification jobs: these are diagnostic measurements, not controlled hardware
certification. Electron 43.2.0 bundles Node 24.18.0, Chromium 150.0.7871.129 and V8
15.0.1240245-electron.0. CLI replay uses Node 24.12.0. React 19.2.8, MUI 9.2.0,
Platformatic Kafka 2.8.0 and TypeScript 5.9.3 remain unchanged.

Replay uses the real application facade, live-rule runtime and renderer reducer.
It starts **after binary Kafka decoding**, with a paced pull-stream fixture.
`--clone` measures a JSON byte count plus structured cloning, not Electron IPC.
It does not measure React paint, user interaction or committed consumer-group lag.
Offered-but-not-generated records, delivered records, host display drops and
rolling-window evictions are separate. All generated records traverse rule
evaluation before display buffering. Histograms and memory telemetry are bounded.

## Repeated comparison

Three 10-second runs per revision: 10,000 offered records/s, 1,024-byte payloads,
six partitions, 25-ms presentation cadence, cloning enabled, no applicable rules.
Numbers are medians; CPU is percent of **one core**, including replay machinery.

| Measurement                      |      Before |       After | Change |
| -------------------------------- | ----------: | ----------: | -----: |
| CPU                              |      46.40% |      21.41% | −53.9% |
| Reducer p95                      |    2.543 ms |    1.008 ms | −60.4% |
| Sampled peak JS heap             |  139.11 MiB |   83.51 MiB | −40.0% |
| Sampled peak RSS                 |  330.00 MiB |  262.41 MiB | −20.5% |
| Generated records/s              |       9,921 |       9,972 |  +0.5% |
| Generated payload/key throughput | 9.735 MiB/s | 9.785 MiB/s |  +0.5% |
| Delivered records per run        |      64,332 |      69,948 |  +8.7% |
| Host display drops per run       |      35,549 |      29,958 | −15.7% |
| Ingestion-to-reducer p95         |  104.895 ms |  105.727 ms |  +0.8% |

This is **not lossless 10k/s display throughput**. The generator was rate-limited,
and the fixed presentation cadence still loses records from the live view. A
lower reducer time did not improve that queueing latency. No Rust comparison ran.

## Exploration, including regressions

Single five-second runs are noisy and are not acceptance certification.
All retained count/byte and queue checks passed, including slow presentation.

| Workload                                  | CPU before → after | Reducer p95 before → after | Host view drops before → after |
| ----------------------------------------- | ------------------ | -------------------------- | ------------------------------ |
| 1k/s, 256 B                               | 11.85% → 5.92%     | 1.592 → 0.326 ms           | 0 → 0                          |
| 10k/s, 256 B                              | 40.30% → 26.32%    | 1.498 → 3.553 ms           | 15,525 → 19,540                |
| 50k/s, 256 B                              | 94.04% → 55.79%    | 2.113 → 1.588 ms           | 151,262 → 220,434              |
| 1k/s, mixed 256 B / 1 KiB / 64 KiB        | 75.35% → 36.87%    | 25.503 → 13.287 ms         | 0 → 309                        |
| 1k/s, 64 KiB                              | 109.67% → 54.87%   | 60.959 → 7.791 ms          | 3,487 → 2,759                  |
| 10k/s, 100-ms bursts, 200-ms presentation | 32.59% → 19.96%    | 1.846 → 5.399 ms           | 43,267 → 44,288                |

At offered 50k/s, actual generation was 33,570 → 47,432/s; more generated records
also means more display drops. These results do not establish a sustainable 50k/s
application. Mixed-payload p95 delivery regressed from 448 to 894 ms in the short
run; the slow-presentation run also regressed. Do not hide these behind the median
improvements. Concurrent VM activity and cadence sensitivity require isolated
longer repeats before selecting new production cadence defaults.

An additional 10-second eight-active-rule run kept required JSON evaluation:
CPU 99.20% → 105.23%, reducer p95 22.255 → 7.179 ms, generated 9,902 → 9,889/s.
Rule evaluation remains CPU-bound; the empty-rule optimization cannot help it.

## Fifteen-minute soak

`--seconds=900 --rate=1000 --bytes=256 --mixed --clone` completed with all bounded
queue/history assertions passing. This exercised the optimized replay pipeline,
not the subsequent Electron acknowledgement transport. Generation averaged
999.96/s; 4,388 records were dropped from the host display queue. Retention ended
at 1,000 records / 22,514,984 accounted bytes. Sampled peak RSS was 357.3 MiB,
heap 128.8 MiB, external memory 75.4 MiB. CPU consumed 216.2 seconds (~24% of one
core). GC: 2,559 events / 12.59 seconds. Event-loop delay p95/p99: 17.55/35.45 ms.
Reducer p95/p99: 7.139/16.415 ms. Ingestion-to-reducer p95/p99: 419.839/716.799 ms.

The **p95 interaction <100 ms objective is unverified**; delivery latency for this
mixed workload exceeds 100 ms. Bounded retention is not proof of no leaks in all
workflows, and ten thousand retained records were not enabled: the intended live
window stays at 1,000 while higher input counts exercise eviction.

## Actual Electron measurements

Real AIO Kafka engine runs (10 seconds, six partitions, 1 KiB records):

| Offered | Fetched/s before → after | Producer + consumer CPU before → after | Backlog after drain |
| ------- | ------------------------ | -------------------------------------- | ------------------- |
| 1k/s    | 999.58 → 998.65          | 43.96% → 39.96%                        | 0 → 0               |
| 10k/s   | 9,988.60 → 9,986.07      | 73.81% → 75.78%                        | 0 → 0               |

All acknowledged records were decoded in contiguous per-partition offset order.
The 10k candidate decoded 99,999 records / 9.752 MiB/s, with backlog 5 when offers
ended and 0 after draining. Peak acknowledged-minus-decoded backlog was 316.
Fetch limits did not demonstrate a throughput improvement at these capped rates;
their rationale is limiting requested buffering. CPU includes the generator and
producer, unlike the separate Electron process measurements below.

Production package build and launch passed. Fresh-profile banner startup in the
diagnostic launch was 2.90 seconds, including Playwright process launch. This is
not a controlled OS-cache-cold/warm comparison. Idle last sample:

| Process        |       RSS | Linux PSS |
| -------------- | --------: | --------: |
| Main / Browser | 255.2 MiB | 157.9 MiB |
| Renderer / Tab | 148.7 MiB |  95.1 MiB |
| GPU            | 135.6 MiB |  62.3 MiB |
| Utility        |  78.8 MiB |  28.7 MiB |

RSS sum ~618 MiB double-counts shared pages; PSS sum ~344 MiB apportions them and
is not equivalent to macOS footprint. Main JS heap used 17.6 MiB / external 7.9
MiB; renderer CDP heap used 11.0 MiB. Last idle CPU sample was 0% for each process,
not a long-run zero-CPU claim. Thirty main-thread idle timer probes had p95
overshoot ~2.56 ms. Raw reports retain per-process samples and CDP task metrics.

The initial bare-Xvfb run did **not** actually minimize the window (`isMinimized=false`, visibility
`visible`); that phase cannot verify minimized behavior. Production profile
creation was unavailable without a Linux OS credential service, so connected
process-tree profiling reports unavailable and exits 1. Real Electron fixture
tests connected successfully through the protected test host; those are
correctness tests, not production throughput measurements.

### Completion run: real streaming and window-manager integration

An isolated D-Bus session, temporary GNOME keyring and Openbox resolved the
production credential-service and actual-window-minimization blockers without
changing application security. The stale shared Registry fixture used incompatible
internal SASL settings; a separate `streamskope-perf-verify` fixture on ports
29093/25000/28081, generated from current source, passed all 13 Kafka tests.
The original fixture was not restarted or replaced.

Presentation pauses at 32 events, 500 records **or** 4 MiB and resumes at or below 16 events,
250 records **and** 2 MiB. Kafka ingestion/rule evaluation continue through the
existing bounded display buffer; display evictions are still reported. Hard
overflow/timeout still fails explicitly. The hook is internal to the desktop
host, not a new renderer capability. Explicit stop/completion still drains;
drain overflow is an explicit delivery failure, not a promise of lossless display.

The resulting packaged run exited 0, offered 48,489 records at 1k/s with 256-byte
payloads on one disposable partition, and verified visible progress before and
after minimizing/restoring. Startup to banner was 1,134 ms, not OS-cache-cold
startup. Producer work ran outside Electron. This is a short diagnostic, not the
15-minute replay soak or a new sustained-throughput certification.

| Phase               | Peak process-tree PSS | Filter interaction p95 / p99 | Main timer overshoot p95 |
| ------------------- | --------------------: | ---------------------------: | -----------------------: |
| Idle                |               354 MiB |                Not exercised |                  2.78 ms |
| Connected           |               392 MiB |                Not exercised |                  2.80 ms |
| Streaming           |               599 MiB |              69.4 / 196.2 ms |                  3.32 ms |
| Minimized streaming |               626 MiB |                Not exercised |                  3.67 ms |
| Restored streaming  |               664 MiB |             202.1 / 306.5 ms |                 11.55 ms |

Electron-reported process-tree CPU sample medians were 21.02%, 20.86%, and 20.74%
for the three streaming phases; raw per-process cumulative CPU seconds are also
retained. These are Electron's reported percentages, not the replay's explicitly
one-core CPU calculation. Idle/connected medians were zero over five samples,
not evidence of zero long-term resource use. PSS apportions shared Linux pages;
it is not native macOS footprint. Memory growth across this short run is **not**
proof of a plateau or absence of leaks.

Forty probes per visible streaming phase measured synthetic filter clicks to two
animation frames with a confirmed state change. This excludes physical input and
receive-to-visible latency. **The <100 ms p95 interaction objective was not met
after restore.** Native UI latency, longer production-process soak, renderer GC
attribution and full cold/warm startup remain follow-up work. Openbox confirmed
`isMinimized=true`, but CDP reported document visibility `visible`; do not infer
hidden-page rendering savings from this environment. No Rust parity is claimed.

Raw evidence: `production-streaming-watermarks.json`,
`production-connected-minimized.json`, `watermarks-red.log`,
`watermarks-green.log`, and `kafka-isolated-green.log` in the ignored evidence
directory. Rerun using `STREAMSKOPE_TEST_FIXTURE_NAME=streamskope-perf-verify`
and `npm run performance:electron-processes -- --stream` after package verification.
A headless Linux runner needs a real credential service and window manager;
plain Xvfb alone cannot reproduce those two acceptance conditions.

## Implemented changes and limits

- Skip payload parsing only when no enabled rule applies. CPU profiling located
  repeated parsing/errors even on ordinary text with an empty rule catalog.
- Merge only the incoming sorted batch with ordered retained state; measure new
  and evicted bytes rather than re-encoding every retained payload. Keep exact
  BigInt offset ordering and immutable snapshots. No speculative worker pool.
- Request 4 MiB aggregate / 1 MiB per-partition Kafka fetches and an object-mode
  high-water mark of 200. The installed client's Readable backpressure delays
  subsequent fetches; the current fetch, oversized first Kafka record batches,
  decompression expansion and multiple brokers still need separate accounting.
- Electron IPC now has one acknowledged event in flight, including that event in
  limits of 64 events, 1,000 records and 8 MiB serialized bytes. Queue saturation
  or a 30-second missing acknowledgement stops consumption and reports recovery
  explicitly. Sender validation and teardown are tested. Acknowledgement means
  the preload listener returned, **not** that React painted. Serialized-byte
  budgeting adds a JSON pass; no zero-copy claim is made.
- Report normal window eviction separately from overload drops. Neither changes
  Kafka commits, transaction isolation, export delivery or active-rule evaluation.

Recommended current limits: preserve 1,000 records / 64 MiB for the renderer,
1,000 / 16 MiB for the facade, 200 / approximately 1 MiB per display batch, and the
new IPC limits above. These bound live display, not a reliable stream archive.
Use 1k/s small records as an initial diagnostic workload, not a published SLA.
For future isolated regression gates, provisional replay budgets are <15% of one
core, <200 MiB RSS and <5 ms reducer p95 at that workload; only byte/count bounds
are currently hard benchmark assertions. Do not make noisy wall-time gates part
of CI without stable runners. The real UI objective remains p95 <100 ms.

Remaining work: deeper active-stream renderer/GC and interaction profiling;
native-platform and hidden-document measurements; payload decoding before truncation;
active-rule CPU isolation; exhaustive cache/request/fetch expansion bounds;
cadence tuning without concealing display loss. Electron's browser/GPU baseline
and structured-clone overhead remain. No native component is justified yet:
first isolate active-rule cost, then compare a bounded worker against serialization
and worker-heap overhead. There was no package-size change or new dependency.

## Reproduction

Run `npm run performance:stream-replay --` with the options above. Add `--rules`
for eight applicable rules; `--partitions=6` is the default. Baseline and candidate
must use identical scripts, Node, options and fixture semantics. Preserve raw JSON
outside version control. To inspect allocation CPU samples:

```sh
node --cpu-prof --cpu-prof-dir=/tmp --import tsx test/performance/stream-pipeline-replay.ts --seconds=15 --rate=10000 --bytes=1024 --clone
npm run performance:kafka-ingestion -- --seconds=10 --rate=1000 --bytes=1024
npm run performance:kafka-ingestion -- --seconds=10 --rate=10000 --bytes=1024
npm run package:verify
npm run performance:electron-processes -- --connect
```

The real-broker benchmark creates/deletes its own UUID-named six-partition topic.
It checks contiguous exact offsets per partition and drains for up to ten seconds.
Producer requests are sequential and bounded to 200 records / 1 MiB. Reported CPU
includes both producer and engine in one Node process, not the broker or renderer.
It separates acknowledged production, decoded records and outstanding backlog;
it is not a consumer-group committed-lag measurement or an end-to-end UI benchmark.

## Verification — 2026-09-14

These results qualify the candidate measured in this report, not subsequent
revisions. Commands ran in a Linux-native dependency mirror with the source Git
directory/work tree for Git-sensitive checks. Final runs exited 0:

| Check                                                                      | Result                                                                |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `npm run test:unit`                                                        | 1,256 tests / 153 files                                               |
| `npm run test:integration`                                                 | 181 tests / 31 files                                                  |
| `npm run test:architecture`                                                | 9 tests                                                               |
| Electron host workflows                                                    | 9 tests                                                               |
| Web monitor/responsive workflows                                           | 7 tests; artifact scan passed                                         |
| Focused delivery/facade/host regression                                    | 39 tests                                                              |
| `npm run package:verify`                                                   | Build, inspection, one packaged launch and artifact scan passed       |
| `STREAMSKOPE_TEST_FIXTURE_NAME=streamskope-perf-verify npm run test:kafka` | 13 tests / 9 files                                                    |
| Background-work, message-retention, consumer-soak and stream-tuning gates  | Passed; fixed-workload soak is separate from the 15-minute replay     |
| `npm run lint`, `npm run typecheck`, `npm run format:check`                | Passed                                                                |
| `npm run dependencies:verify`                                              | 0 vulnerabilities; initial inventory: 36 direct / 494 locked packages |
| Strict OpenSpec validation                                                 | Passed                                                                |

The initial Kafka run exited 1 (12 passed, one failed): the shared Registry lacked
`test-value`. The separate fixture resolved this without restarting the original.
The production `--stream` profiler exited 0; its latency and memory limitations
are reported above and do not establish a performance SLA.

Regression coverage includes exact offsets beyond Number precision, Unicode byte
retention, immutability, rule evaluation, fetch limits, delivery ordering,
foreign/duplicate acknowledgements, queue saturation, timeout and stop/recovery.
Normal window eviction and overload drops retain separate counters.

At this measurement date, full web E2E, native macOS/Windows profiling, a long
production-process soak and physical-input/receive-to-visible percentiles were
unverified. Raw JSON and CPU profiles remain under the ignored
`.artifacts/performance/sustained-stream/` directory. Cleanup removed the disposable
fixture and four temporary benchmark topics, preserving existing Kafka data and profiles.
