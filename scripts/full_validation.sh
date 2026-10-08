#!/bin/bash
# Authoritative validation: one attempt per lane; a failure stays a failure.
set -euo pipefail
PLAN_PATH=""
FOCUS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--plan) [[ $# -ge 2 ]] || { echo 'Missing plan path' >&2; exit 2; }; PLAN_PATH="$2"; shift 2 ;;
        --focus) [[ $# -ge 2 && -n "$2" ]] || { echo 'Missing test filter' >&2; exit 2; }; FOCUS="$2"; shift 2 ;;
        *) [[ -z "$PLAN_PATH" ]] || { echo "Unexpected argument: $1" >&2; exit 2; }; PLAN_PATH="$1"; shift ;;
    esac
done
cd "$(dirname "${BASH_SOURCE[0]}")/.."
[[ -f Cargo.toml ]] || { echo 'Cargo.toml missing' >&2; exit 1; }
[[ -z "$PLAN_PATH" || -f "$PLAN_PATH" ]] || { echo "Plan missing: $PLAN_PATH" >&2; exit 1; }
mkdir -p logs reports
LOG_FILE="logs/validation_$(date +%Y-%m-%d_%H-%M-%S)_$$.txt"
run() {
    printf '%s command:' "$(date -u +%FT%TZ)" | tee -a "$LOG_FILE"
    printf ' %q' "$@" | tee -a "$LOG_FILE"
    printf '\n' | tee -a "$LOG_FILE"
    # Run natively: piping socket-test output can cause macOS EPERM failures.
    local status=0
    "$@" || status=$?
    echo "exit=$status" | tee -a "$LOG_FILE"
    [[ "$status" -eq 0 ]] || { echo 'FAILED: no automatic retry; preserve native output for diagnosis.' | tee -a "$LOG_FILE"; exit "$status"; }
}
if [[ -n "$FOCUS" ]]; then
    # Listing must succeed and select at least one real test before execution.
    inventory=$(cargo test --offline --locked --workspace --all-features "$FOCUS" -- --list)
    if ! grep -Eq ': test$' <<< "$inventory"; then
        echo "No tests selected by: $FOCUS" >&2
        exit 1
    fi
    run cargo test --offline --locked --workspace --all-features "$FOCUS" -- --test-threads=1
    echo "FOCUSED PASS (not full validation): $LOG_FILE"
    exit 0
fi
# Test default and feature-enabled behavior independently. No substring skips.
run cargo test --offline --locked --workspace -- --test-threads=1
run cargo test --offline --locked --workspace --all-features -- --test-threads=1
run ./scripts/check_no_rkyv_from_bytes.sh
run ./scripts/check_forbidden_copy_patterns.sh
if [[ -n "$PLAN_PATH" ]]; then
    run ./scripts/check_critical_coverage.sh "$PLAN_PATH"
    run ./scripts/analyze_coverage_gaps.sh "$PLAN_PATH"
fi
echo "VALIDATION PASSED: $LOG_FILE" | tee -a "$LOG_FILE"
