#!/usr/bin/env bash
# Fail-first, offline validation. Every command and its output is retained.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PLAN_PATH=""
FOCUS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--plan)
            [[ $# -ge 2 && -n "$2" ]] || { echo "Missing plan path" >&2; exit 2; }
            PLAN_PATH="$2"; shift 2 ;;
        --focus)
            [[ $# -ge 2 && -n "$2" ]] || { echo "Missing test filter" >&2; exit 2; }
            FOCUS="$2"; shift 2 ;;
        *)
            [[ -z "$PLAN_PATH" ]] || { echo "Unexpected argument: $1" >&2; exit 2; }
            PLAN_PATH="$1"; shift ;;
    esac
done
[[ -f Cargo.toml ]] || { echo "Cargo.toml missing" >&2; exit 2; }
[[ -z "$PLAN_PATH" || -f "$PLAN_PATH" ]] || { echo "Plan does not exist: $PLAN_PATH" >&2; exit 2; }
mkdir -p logs
LOG_DIR=$(mktemp -d "logs/validation_$(date +%Y%m%d_%H%M%S)_XXXXXX")
LOG_FILE="$LOG_DIR/summary.log"
STEP=0
LAST_OUTPUT=""
# Never fetch dependencies indirectly from an optional coverage helper either.
export CARGO_NET_OFFLINE=true

run() {
    STEP=$((STEP + 1))
    LAST_OUTPUT="$LOG_DIR/step_${STEP}.log"
    printf 'START step=%s time=%s command=' "$STEP" "$(date -u +%FT%TZ)" | tee -a "$LOG_FILE"
    printf '%q ' "$@" | tee -a "$LOG_FILE"
    printf '\n' | tee -a "$LOG_FILE"
    local statuses
    if "$@" 2>&1 | tee "$LAST_OUTPUT"; then
        statuses=("${PIPESTATUS[@]}")
    else
        statuses=("${PIPESTATUS[@]}")
    fi
    printf 'END step=%s command_status=%s capture_status=%s time=%s\n' \
        "$STEP" "${statuses[0]}" "${statuses[1]}" "$(date -u +%FT%TZ)" | tee -a "$LOG_FILE"
    [[ ${statuses[0]} -eq 0 ]] || exit "${statuses[0]}"
    [[ ${statuses[1]} -eq 0 ]] || exit "${statuses[1]}"
}

# Each lane gets an inventory and a positive executed-count proof. No substring
# skips, stale named filters, retries, quiet output or unrecorded recovery runs.
test_lane() {
    local selected executed
    run cargo test --offline --locked "$@" -- --list
    selected=$(awk '/: test$/ {n++} END {print n+0}' "$LAST_OUTPUT")
    [[ "$selected" -gt 0 ]] || { echo "ERROR: zero selected tests" | tee -a "$LOG_FILE"; exit 1; }
    run cargo test --offline --locked "$@" -- --test-threads=1
    executed=$(awk '/^test result: ok\./ {n += $4} END {print n+0}' "$LAST_OUTPUT")
    # Only passed tests count; ignored tests are not executed acceptance checks.
    [[ "$executed" -gt 0 ]] || { echo "ERROR: zero executed tests" | tee -a "$LOG_FILE"; exit 1; }
    printf 'TEST_COUNTS selected=%s executed=%s\n' "$selected" "$executed" | tee -a "$LOG_FILE"
}

if [[ -n "$FOCUS" ]]; then
    test_lane --workspace --all-features "$FOCUS"
    printf 'FOCUSED PASS (not full validation): %s\n' "$LOG_DIR" | tee -a "$LOG_FILE"
    exit 0
fi

run cargo fmt --all -- --check
run cargo build --offline --locked --lib --no-default-features
run cargo build --offline --locked --all-targets
run cargo build --offline --locked --all-targets --all-features
run cargo clippy --offline --locked --all-targets --all-features -- -D warnings
run cargo rustdoc --offline --locked --all-features -- -D warnings
# Keep the historically socket-sensitive target isolated as well; it is not
# skipped from any feature lane. Capture failures/environmental errors block.
test_lane --test ask_reply_end_to_end -j 1
test_lane --workspace
test_lane --workspace --features test-helpers
test_lane --workspace --all-features
test_lane --release --workspace
test_lane --release --workspace --all-features
run ./scripts/check_no_rkyv_from_bytes.sh
run ./scripts/check_forbidden_copy_patterns.sh
if [[ -n "$PLAN_PATH" ]]; then
    run ./scripts/check_critical_coverage.sh "$PLAN_PATH"
    run ./scripts/analyze_coverage_gaps.sh "$PLAN_PATH"
fi
printf 'VALIDATION COMPLETE: %s\n' "$LOG_DIR" | tee -a "$LOG_FILE"
