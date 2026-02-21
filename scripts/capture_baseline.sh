#!/bin/bash
# Capture baseline allocation metrics - Phase 1 of BYTEMUCK Optimization Sprint
#
# This script runs all baseline tests and saves the output to a file.
# NO CODE CHANGES SHOULD BE MADE UNTIL BASELINE IS CAPTURED.
#
# Usage: ./scripts/capture_baseline.sh

set -e

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║     CAPTURING BASELINE ALLOCATION METRICS                       ║"
echo "║     DO NOT MODIFY CODE UNTIL BASELINE IS CAPTURED              ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo "Error: Must be run from project root (where Cargo.toml is located)"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p baselines

# Get current date for filename
DATE=$(date +"%Y-%m-%d_%H-%M-%S")
OUTPUT_FILE="baselines/baseline_allocations_${DATE}.txt"

echo "Running baseline tests..."
echo "Output will be saved to: ${OUTPUT_FILE}"
echo ""

# Run the tests with output capture (stdout + stderr so allocation metrics are preserved)
cargo test --test bytemuck_allocation_tests baseline -- --ignored --nocapture 2>&1 | tee "${OUTPUT_FILE}"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  BASELINE CAPTURED: ${OUTPUT_FILE}"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. Review the baseline metrics in ${OUTPUT_FILE}"
echo "  2. Proceed to implementation phase (see spec/BYTEMUCK.md)"
echo "  3. Run comparison tests after implementation"
