# Validation Scripts

This directory contains repository validation helpers. The current scripts are centered on test execution, copy-guard checks, and optional coverage analysis.

## Main scripts

| Script | Purpose |
| ------ | ------- |
| `full_validation.sh` | Runs the main validation flow: isolated TLS e2e, workspace tests, copy guards, pointer tests, and streaming tests. |
| `check_no_rkyv_from_bytes.sh` | Fails if forbidden `rkyv::from_bytes` usage is present. |
| `check_forbidden_copy_patterns.sh` | Fails on selected copy-pattern regressions. |
| `coverage.sh` | Builds `reports/coverage.lcov` with `cargo llvm-cov`. |
| `check_critical_coverage.sh [plan_path]` | Ensures `CRITICAL_PATH`-annotated lines are covered. |
| `analyze_coverage_gaps.sh [plan_path]` | Writes a timestamped Markdown report of uncovered lines. |
| `capture_baseline.sh` / `compare_allocations.sh` | Historical helpers for baseline and allocation comparisons. |

## Running the full suite

```bash
./scripts/full_validation.sh
```

The script runs default and all-features workspace test lanes once each, followed by both copy guards and optional coverage gates. It uses the existing local lockfile and offline dependencies. Resolve/fetch dependencies separately before running it. No broad retries or substring skips are used; socket failures block validation rather than becoming clean passes.

Native test output is not piped because of macOS socket/output constraints. Command identities and exit statuses are logged under `logs/`; preserve the terminal transcript when diagnosing failures. This is not a complete test-output log.

For troubleshooting only, `./scripts/full_validation.sh --focus FILTER` verifies a nonzero test selection and runs it with all features. Focused success is not full validation.

Harness regression tests (no real Cargo execution):

```bash
bash scripts/tests/full_validation_test.sh
```

## Coverage scripts

```bash
./scripts/check_critical_coverage.sh path/to/plan.md
./scripts/analyze_coverage_gaps.sh path/to/plan.md
```

Notes:

- Both scripts rebuild coverage unless `SKIP_COVERAGE_REBUILD=1` is set.
- Both scripts have an internal default plan path of `sprints/LEGACY_FUNCTION_CLEANUP/sprint_3.md`.
- That legacy path is not present in this repository, so pass an explicit plan path when you want the plan label in output to point at a real file.

## Prerequisites

- `cargo`
- `python3`
- `rg`
- `cargo-llvm-cov` for coverage workflows

Install coverage support with:

```bash
cargo install cargo-llvm-cov
```
