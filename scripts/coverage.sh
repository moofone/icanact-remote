#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov is required (install with 'cargo install cargo-llvm-cov')" >&2
  exit 1
fi

mkdir -p reports
cargo llvm-cov --workspace --lcov --output-path reports/coverage.lcov
echo "Coverage report written to reports/coverage.lcov"
