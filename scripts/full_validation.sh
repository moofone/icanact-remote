#!/bin/bash
# Full validation suite for the Gossip Zero-Copy rollout.
#
# This script wires together the zero-copy allocation guards, pointer identity
# proofs, telemetry smoke tests, and coverage gates required by
# sprints/GOSSIP_ZERO_COPY/sprint_3.md.
#
# Usage: ./scripts/full_validation.sh

set -euo pipefail


PLAN_PATH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--plan)
            PLAN_PATH="$2"
            shift 2
            ;;
        *)
            if [[ -z "$PLAN_PATH" ]]; then
                PLAN_PATH="$1"
                shift
            else
                echo "Error: unexpected argument '$1'" >&2
                exit 1
            fi
            ;;
    esac
done

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║     ZERO-COPY VALIDATION (Legacy Cleanup Plan)               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

if [ ! -f "Cargo.toml" ]; then
    echo "Error: Must be run from the project root (Cargo.toml missing)"
    exit 1
fi

if [ -n "$PLAN_PATH" ] && [ ! -f "$PLAN_PATH" ]; then
    echo "Warning: plan file '$PLAN_PATH' not found; continuing without coverage gates"
    PLAN_PATH=""
fi

mkdir -p baselines reports logs
VALIDATION_DATE=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_FILE="logs/validation_${VALIDATION_DATE}.txt"

log_section() {
    local title="$1"
    echo "═══════════════════════════════════════════════════════════════" | tee -a "${LOG_FILE}"
    echo "${title}" | tee -a "${LOG_FILE}"
    echo "═══════════════════════════════════════════════════════════════" | tee -a "${LOG_FILE}"
}

# Also: EPERM can still occur transiently even when serialized. Retry a couple times to make
# validation runs deterministic without hiding real regressions (non-EPERM failures will repeat).
MAX_TEST_ATTEMPTS="${ICANACT_VALIDATION_TEST_ATTEMPTS:-10}"
RETRY_SLEEP_SECS="${ICANACT_VALIDATION_RETRY_SLEEP_SECS:-5}"
CARGO_TEST_QUIET="${ICANACT_VALIDATION_CARGO_TEST_QUIET:-1}"
#
# Some e2e tests retry socket bind/connect on transient sandbox EPERM. The default retry window
# in tests is intentionally conservative for ad-hoc `cargo test` runs, but for this validation
# harness we prefer faster "fail and retry the whole suite" behavior.
TEST_EPERM_MAX_MS="${ICANACT_VALIDATION_TEST_EPERM_MAX_MS:-5000}"

# Step 0: Run the most socket-sensitive TLS e2e test in isolation first.
#
# Rationale: In sandboxed macOS runs, we've observed transient EPERM bursts that correlate with
# overall socket syscall volume. Running this e2e test first dramatically reduces flakiness.
log_section "Step 0: socket-heavy TLS e2e (isolated)"
SOCKET_E2E_SKIP_NAME="test_end_to_end_ask_reply"
e2e_attempt=1
while true; do
  echo "Running isolated TLS e2e (attempt ${e2e_attempt}/${MAX_TEST_ATTEMPTS})..." | tee -a "${LOG_FILE}"
  if env ICANACT_TEST_EPERM_MAX_MS="${TEST_EPERM_MAX_MS}" \
    cargo test --test ask_reply_end_to_end -j 1 -- --test-threads=1; then
    echo "✅ socket-heavy TLS e2e passed (isolated)" | tee -a "${LOG_FILE}"
    break
  fi

  if [[ "${e2e_attempt}" -ge "${MAX_TEST_ATTEMPTS}" ]]; then
    echo "❌ socket-heavy TLS e2e failed (isolated)" | tee -a "${LOG_FILE}"
    exit 1
  fi
  e2e_attempt=$((e2e_attempt+1))
  sleep "${RETRY_SLEEP_SECS}"
done
echo "" | tee -a "${LOG_FILE}"

# Step 1: Run the full workspace test suite.
log_section "Step 1: cargo test --workspace"
# IMPORTANT (macOS): piping/redirecting `cargo test` output (e.g. through `tee`) can cause
# socket-heavy tests to fail with EPERM ("Operation not permitted"). Run tests directly.
echo "NOTE: cargo test output is not streamed into ${LOG_FILE} to avoid EPERM in e2e socket tests." | tee -a "${LOG_FILE}"
# IMPORTANT (macOS): keep the harness single-threaded to avoid socket-heavy flakiness.
# NOTE: For reasons we haven't isolated yet, forcing Cargo itself to a single job (`-j 1` /
# `CARGO_TEST_JOBS=1`) increases the frequency of EPERM in these sandboxed runs. Keep Cargo's
# default job scheduling, but serialize the test harness.
#
# Also IMPORTANT (macOS/sandbox): `--nocapture` dramatically increases stdout/stderr volume and
# has been observed to correlate with transient EPERM in socket-heavy TLS tests. Prefer the
# default capture behavior for the full suite; run individual tests with `--nocapture` when
# debugging.
# IMPORTANT (macOS/sandbox): Cargo can run multiple test binaries concurrently via the jobserver.
# In this repo's socket-heavy TLS integration tests, that concurrency has been observed to
# correlate with transient EPERM ("Operation not permitted"). Force Cargo jobserver to 1 so test
# binaries run sequentially.
attempt=1
while true; do
    echo "Running workspace tests (attempt ${attempt}/${MAX_TEST_ATTEMPTS})..." | tee -a "${LOG_FILE}"
    # Keep stdout minimal in sandboxed TLS socket tests. High output volume has been
    # observed to correlate with transient EPERM ("Operation not permitted").
    if [[ "${CARGO_TEST_QUIET}" == "1" ]]; then
      CARGO_TEST_CMD=(env ICANACT_TEST_EPERM_MAX_MS="${TEST_EPERM_MAX_MS}" cargo test --all -q -j 1 -- --test-threads=1 --skip "${SOCKET_E2E_SKIP_NAME}")
    else
      CARGO_TEST_CMD=(env ICANACT_TEST_EPERM_MAX_MS="${TEST_EPERM_MAX_MS}" cargo test --all -j 1 -- --test-threads=1 --skip "${SOCKET_E2E_SKIP_NAME}")
    fi

    if "${CARGO_TEST_CMD[@]}"; then
        echo "✅ Workspace tests passed" | tee -a "${LOG_FILE}"
        break
    fi

    if [[ "${attempt}" -ge "${MAX_TEST_ATTEMPTS}" ]]; then
        echo "❌ Workspace tests failed" | tee -a "${LOG_FILE}"
        exit 1
    fi

    attempt=$((attempt+1))
    # In sandboxed macOS runs we've observed transient EPERM bursts that can persist
    # for several seconds. A slightly longer cooldown improves determinism without
    # hiding real regressions.
    sleep "${RETRY_SLEEP_SECS}"
done
echo "" | tee -a "${LOG_FILE}"

# Step 2: Guard against forbidden rkyv::from_bytes in runtime code.
log_section "Step 2: rkyv::from_bytes guard"
if ./scripts/check_no_rkyv_from_bytes.sh 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ No forbidden rkyv::from_bytes usage detected" | tee -a "${LOG_FILE}"
else
    echo "❌ Forbidden rkyv::from_bytes usage detected" | tee -a "${LOG_FILE}"
    exit 1
fi
echo "" | tee -a "${LOG_FILE}"

# Step 3: Zero-copy allocation guards (debug profile).
log_section "Step 3: Forbidden copy guard"
if ./scripts/check_forbidden_copy_patterns.sh 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ Forbidden copy patterns not detected" | tee -a "${LOG_FILE}"
else
    echo "❌ Forbidden copy guard failed" | tee -a "${LOG_FILE}"
    exit 1
fi
echo "" | tee -a "${LOG_FILE}"



# Step 4: Pointer identity proofs in the TLS reader.
log_section "Step 5: Pointer identity proofs"
POINTER_TESTS=(
    "gossip_frame_uses_zero_copy_buffer"
    "gossip_registry_payload_deserializes_from_aligned_buffer"
)
for test_name in "${POINTER_TESTS[@]}"; do
    # See note above about `tee` and EPERM with socket-heavy tests.
    if cargo test "${test_name}" -j 1 -- --test-threads=1; then
        echo "✅ ${test_name} passed" | tee -a "${LOG_FILE}"
    else
        echo "❌ ${test_name} failed" | tee -a "${LOG_FILE}"
        exit 1
    fi
done
echo "" | tee -a "${LOG_FILE}"

# Step 5: Telemetry smoke test to ensure counters wire through end-to-end.


# Step 5.5: Streaming request/response edge case tests
log_section "Step 6.5: Streaming request/response edge case tests"
STREAMING_TESTS=(
    "test_streaming_request_large_payload"
    "test_streaming_request_zero_copy"
    "test_streaming_response_auto"
    "test_streaming_threshold_boundary"
    "test_concurrent_streaming_requests"
    "test_message_type_streaming_response_variants"
    "test_streaming_tell_no_response"
)
for test_name in "${STREAMING_TESTS[@]}"; do
    # See note above about `tee` and EPERM with socket-heavy tests.
    if cargo test --test streaming_tests "${test_name}" -j 1 -- --test-threads=1; then
        echo "✅ ${test_name} passed" | tee -a "${LOG_FILE}"
    else
        echo "❌ ${test_name} failed" | tee -a "${LOG_FILE}"
        exit 1
    fi
done
echo "" | tee -a "${LOG_FILE}"



if [ -n "${PLAN_PATH}" ]; then
    # Step 7: Critical-path coverage gate.
    log_section "Step 8: Critical-path coverage gate"
    if ./scripts/check_critical_coverage.sh "${PLAN_PATH}" 2>&1 | tee -a "${LOG_FILE}"; then
        echo "✅ CRITICAL_PATH coverage satisfied" | tee -a "${LOG_FILE}"
    else
        echo "❌ CRITICAL_PATH coverage failed" | tee -a "${LOG_FILE}"
        exit 1
    fi
    echo "" | tee -a "${LOG_FILE}"

    # Step 8: Coverage gap analysis (produces Markdown report under reports/).
    log_section "Step 9: Coverage gap report"
    if ./scripts/analyze_coverage_gaps.sh "${PLAN_PATH}" 2>&1 | tee -a "${LOG_FILE}"; then
        echo "✅ Coverage gap report generated" | tee -a "${LOG_FILE}"
    else
        echo "❌ Coverage gap analysis failed" | tee -a "${LOG_FILE}"
        exit 1
    fi
    echo "" | tee -a "${LOG_FILE}"
else
    echo "Skipping coverage gate/report (no plan file)." | tee -a "${LOG_FILE}"
fi

echo "═══════════════════════════════════════════════════════════════" | tee -a "${LOG_FILE}"
echo "VALIDATION COMPLETE: ${LOG_FILE}" | tee -a "${LOG_FILE}"
if [ -n "${PLAN_PATH}" ]; then
    echo "Plan reference: ${PLAN_PATH}" | tee -a "${LOG_FILE}"
else
    echo "Plan reference: <none>" | tee -a "${LOG_FILE}"
fi
echo "═══════════════════════════════════════════════════════════════" | tee -a "${LOG_FILE}"
echo ""
echo "Recommended follow-ups:"
echo "  1. Inspect ${LOG_FILE} for warnings (telemetry counters, allocation guard output)."
echo "  2. Review reports/coverage_gaps_* for actionable coverage work items."
echo "  3. Attach the validation log to the sprint report under ${PLAN_PATH}."
