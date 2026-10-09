# Validation and measured performance experiments

## Fail-first validation

Resolve the library's untracked lockfile from the existing offline cache, then run:

```bash
cargo generate-lockfile --offline
./scripts/full_validation.sh
python3 -m unittest discover -s scripts/tests -v
```

`full_validation.sh` runs format, library/all-target builds, strict Clippy and
rustdoc, isolated TLS e2e, separate default/test-helper/all-feature test lanes,
release default/all-feature lanes, and both copy guards. No retries or substring
skips are used. Each lane records test inventory and requires positive executed
counts. The complete commands, stdout/stderr, exit/capture statuses and counts
are preserved under a unique `logs/validation_*/` directory. The first failed
command or capture blocks completion. An EPERM or other environmental failure is
not a pass; diagnose separately and retain the original run.

Historical focused pointer/streaming selectors have been removed: the canonical
workspace lanes cover those targets, without stale names or duplicate filter
claims. Inventory files show exactly which targets/features were present.

For troubleshooting only, `./scripts/full_validation.sh --focus FILTER`
uses the same retained inventory/executed-count checks with all features.
Focused success is explicitly **not full validation**.

Harness regression tests (no real Cargo/project execution):

```bash
python3 -m unittest discover -s scripts/tests -v
bash scripts/tests/full_validation_test.sh
```

Optional coverage (requires already-available tooling):

```bash
./scripts/full_validation.sh --plan docs/QA_REMEDIATION_PLAN.md
./scripts/check_critical_coverage.sh docs/QA_REMEDIATION_PLAN.md
./scripts/analyze_coverage_gaps.sh docs/QA_REMEDIATION_PLAN.md
```

A missing plan path fails rather than silently disabling requested gates. The
standalone coverage helpers retain historical default labels; supply an explicit
plan path. Coverage percentage does not replace assertion migration evidence.

## A/B experiments

The old five-run, last-throughput-token benchmark summary has been replaced by
an explicit paired runner:

```bash
./scripts/bench_transport_contract.sh --config /absolute/experiment.json \
  --output /absolute/new-evidence-directory
```

The runner uses Python's standard library. It never installs dependencies,
implicitly prepares worktrees, retries failures, or overwrites existing
experiment directories. Prebuild each arm with the same installed toolchain and
runtime dependency lock; apply the same harness to both source snapshots.
Commands run on the host, not in a security sandbox. Output belongs outside
source. The runner records config/source/lock/runner hashes, logs and statuses,
independent paired samples, per-session/pooled bootstrap CIs, and blocked runs.
Metric-gate success still requires human correctness, workload and evidence
review; it is not final remediation acceptance.

Config shape (replace all paths and environment metadata with real identities):

```json
{
  "mode": "pilot",
  "experiment": "E0-AA-noise",
  "pairs": 10,
  "sessions": 2,
  "seed": 8102026,
  "environment": {"host": "record real CPU/OS/toolchain/power/profile"},
  "arms": {
    "A": {"cwd": "/absolute/safe-baseline"},
    "B": {"cwd": "/absolute/candidate"}
  },
  "workloads": [{
    "id": "tell_delivered",
    "argv": ["python3", "scripts/sample_transport.py", "--workload", "tell_delivered", "--minimum-seconds", "0"],
    "timeout_seconds": 120,
    "hypothesis": "Establish pilot noise only, not an optimization claim",
    "correctness_oracle": "Exactly one executed test and positive completed deliveries",
    "metrics": {
      "throughput": {"unit": "messages/s", "direction": "higher", "goal": "nonregression", "max_regression": 0.03}
    }
  }]
}
```

`mode` is `pilot` or `acceptance`; pilot never accepts an optimization. Metric
`goal` is `nonregression`, `improve` (predeclare `min_improvement >= 0.05`), or
`reduce` (lower-is-better deterministic counters must improve every pair).
Regression caps default to 3%, with at most 5% for metrics explicitly marked
`"category": "memory"`. Runtime locks must match unless a dependency experiment explicitly
sets `require_same_lock` false and documents both graphs.

A command must emit **exactly one** structured stdout record:

```text
AB_METRICS {"workload":"tell_delivered","completed":10000,"errors":0,"drops":0,"metrics":{"throughput":250000}}
```

Nonzero exits, timeout, missing/ambiguous metrics, nonfinite values, errors/drops,
zero completions, changed sources/config, and mismatched paired counts block.

`sample_transport.py` adapts exact existing release/test-helper TLS benchmarks
for delivered tells, single-flight asks, and inflight-512 asks. It verifies
list-mode selection and exactly one executed passing test before accepting a
throughput record. It does **not** provide per-message latency percentiles,
allocation, CPU or memory data. Native short fixtures are only noise pilots
when `--minimum-seconds 0` is explicitly selected. The default duration gate is
10 seconds; extend and validate the common fixture for real acceptance instead
of turning off that gate. Existing Criterion iteration samples are not
independent process samples.

### Long concurrent-ask samples

`sample_concurrent.py --inflight 64` runs a common four-worker TLS fixture with
five-second warmup, ten-second measurement, full nonce/payload checks and
per-operation p50/p95/p99. Supported depths are 1/8/64/512. These are closed-loop
saturation latencies. The normal integration smoke test exercises each depth.

`sample_offered.py --rate 1000` schedules that same ask path at a constant
rate. Latency starts at the scheduled instant, so a late issue keeps the
delay. The issuer spins any wait of 20 ms or less and never sends early.
An offer still unissued after the lateness bound is a drop and the
adapter rejects it. The offer count must equal rate times the measured
window. This is not a saturation-throughput result and has no accepted A/A.
The frozen 1,000/s A/A missed its latency gates. Later 100/s clock probes
are not a replacement baseline. The integration smoke checks a 200/s, 50 ms window.

For these experiments, declare `"completion_policy": "fixed_duration"`, a
`"duration_metric": "duration_ns"`, and `"minimum_duration": 10000000000`;
include duration_ns with unit ns in the metric definitions. Positive completed
counts may differ across arms because that is the throughput observation.
Default `fixed_count` experiments still require matching paired completion
counts. Source/lock equality, raw evidence and correctness checks apply to both.

Every optimization must satisfy the full methodology and controls in
[QA_REMEDIATION_PLAN.md](../docs/QA_REMEDIATION_PLAN.md), not just whichever
metrics one command happens to output. Hardware noise, missing required metrics
or unresolved tests block acceptance. See
[PERFORMANCE_REPORT.md](../docs/PERFORMANCE_REPORT.md) for current evidence and
limitations.

## Other helpers and prerequisites

- `check_no_rkyv_from_bytes.sh`: runtime deserialization copy guard.
- `check_forbidden_copy_patterns.sh`: selected copy-pattern guard.
- `coverage.sh`: LCOV via existing `cargo-llvm-cov`.
- `capture_baseline.sh` / `compare_allocations.sh`: historical helpers; not a
  substitute for paired, path-specific evidence.

Use installed Cargo/Rust, Python 3, Bash, `rg`, and cached dependencies. Missing
coverage/profiling/Miri tools are reported as unavailable; installation or network
fetches require separate consent. Raw generated reports/logs must not be committed.
