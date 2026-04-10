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

The script currently:

1. Runs `cargo test --test ask_reply_end_to_end -j 1 -- --test-threads=1` first.
2. Runs the broader workspace test suite with retries.
3. Runs `check_no_rkyv_from_bytes.sh`.
4. Runs `check_forbidden_copy_patterns.sh`.
5. Runs focused pointer-identity tests.
6. Runs focused streaming tests from `tests/streaming_tests.rs`.
7. Optionally runs coverage gates if a plan path is supplied.

It also creates `baselines/`, `reports/`, and `logs/` if they do not exist, and writes a log to `logs/validation_<timestamp>.txt`.

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
