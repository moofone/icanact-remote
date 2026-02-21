#!/bin/bash
# Run optimized tests and compare with baseline - Phase 3 of BYTEMUCK Optimization Sprint
#
# This script runs optimized tests and generates a comparison report.
# This should be run AFTER implementation is complete.
#
# Usage: ./scripts/compare_allocations.sh [baseline_file]

set -e

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║     ALLOCATION COMPARISON REPORT                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo "Error: Must be run from project root (where Cargo.toml is located)"
    exit 1
fi

# Find the most recent baseline file if not specified
if [ -z "$1" ]; then
    BASELINE_FILE=$(ls -t baselines/baseline_allocations_*.txt 2>/dev/null | head -1)
    if [ -z "$BASELINE_FILE" ]; then
        echo "Error: No baseline file found in baselines/ directory"
        echo "       Run ./scripts/capture_baseline.sh first"
        exit 1
    fi
    echo "Using baseline file: ${BASELINE_FILE}"
else
    BASELINE_FILE="$1"
    if [ ! -f "$BASELINE_FILE" ]; then
        echo "Error: Baseline file not found: ${BASELINE_FILE}"
        exit 1
    fi
fi

# Create output directory
mkdir -p reports

# Get current date for filename
DATE=$(date +"%Y-%m-%d_%H-%M-%S")
COMPARISON_REPORT="reports/comparison_${DATE}.txt"

echo "Running optimized tests..."
echo "Comparison report will be saved to: ${COMPARISON_REPORT}"
echo ""

# Extract baseline metrics
echo "═══════════════════════════════════════════════════════════════" | tee "${COMPARISON_REPORT}"
echo "BASELINE METRICS (from ${BASELINE_FILE})" | tee -a "${COMPARISON_REPORT}"
echo "═══════════════════════════════════════════════════════════════" | tee -a "${COMPARISON_REPORT}"
grep "Result:" "${BASELINE_FILE}" | tee -a "${COMPARISON_REPORT}"
echo "" | tee -a "${COMPARISON_REPORT}"

# Run optimized tests
echo "═══════════════════════════════════════════════════════════════" | tee -a "${COMPARISON_REPORT}"
echo "OPTIMIZED METRICS (current implementation)" | tee -a "${COMPARISON_REPORT}"
echo "═══════════════════════════════════════════════════════════════" | tee -a "${COMPARISON_REPORT}"

# Check if optimized tests exist first
if cargo test --test bytemuck_optimized_tests -- --list 2>/dev/null | grep -q "optimized"; then
    cargo test --test bytemuck_optimized_tests -- --nocapture | tee -a "${COMPARISON_REPORT}"
else
    echo "Note: Optimized tests not found. Run baseline tests for comparison." | tee -a "${COMPARISON_REPORT}"
    echo "" | tee -a "${COMPARISON_REPORT}"
    echo "Running baseline tests again for comparison..." | tee -a "${COMPARISON_REPORT}"
    cargo test --test bytemuck_allocation_tests baseline -- --ignored --nocapture | tee -a "${COMPARISON_REPORT}"
fi

echo "" | tee -a "${COMPARISON_REPORT}"
echo "═══════════════════════════════════════════════════════════════" | tee -a "${COMPARISON_REPORT}"
echo "COMPARISON COMPLETE: ${COMPARISON_REPORT}" | tee -a "${COMPARISON_REPORT}"
echo "═══════════════════════════════════════════════════════════════" | tee -a "${COMPARISON_REPORT}"
echo ""
echo "Next steps:"
echo "  1. Review the comparison report in ${COMPARISON_REPORT}"
echo "  2. Verify allocation reductions match expectations"
echo "  3. Check for any performance regressions"
