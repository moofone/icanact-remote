#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLAN_PATH="${1:-sprints/LEGACY_FUNCTION_CLEANUP/sprint_3.md}"

mkdir -p "${ROOT_DIR}/reports"
if [[ "${SKIP_COVERAGE_REBUILD:-0}" != "1" ]]; then
  "${ROOT_DIR}/scripts/coverage.sh"
fi

COVERAGE_FILE="${ROOT_DIR}/reports/coverage.lcov"
if [[ ! -f "${COVERAGE_FILE}" ]]; then
  echo "Coverage report not found at ${COVERAGE_FILE}." >&2
  echo "Re-run without SKIP_COVERAGE_REBUILD=1 to regenerate coverage." >&2
  exit 1
fi

REPORT_PATH="${ROOT_DIR}/reports/coverage_gaps_$(date +%Y-%m-%d_%H-%M-%S).md"

python3 - <<'PY' "${ROOT_DIR}" "${COVERAGE_FILE}" "${PLAN_PATH}" "${REPORT_PATH}"
import pathlib
from datetime import datetime
import sys

root = pathlib.Path(sys.argv[1])
coverage_path = pathlib.Path(sys.argv[2])
plan_path = sys.argv[3]
report_path = pathlib.Path(sys.argv[4])

coverage = {}
current_file = None
with coverage_path.open("r", encoding="utf-8") as handle:
    for raw in handle:
        raw = raw.strip()
        if raw.startswith("SF:"):
            current_file = pathlib.Path(raw[3:])
        elif raw.startswith("DA:") and current_file:
            line_part, hits_part = raw[3:].split(",", 1)
            coverage.setdefault(current_file, {})[int(line_part)] = int(hits_part)

gaps = []
for file_path, lines in coverage.items():
    zero_lines = sorted(line for line, hits in lines.items() if hits == 0)
    if zero_lines:
        rel = file_path
        if rel.is_absolute():
            try:
                rel = rel.relative_to(root)
            except ValueError:
                rel = file_path
        gaps.append((rel, len(zero_lines), zero_lines[:20]))

gaps.sort(key=lambda item: item[1], reverse=True)

with report_path.open("w", encoding="utf-8") as handle:
    handle.write(f"# Coverage Gap Report\n\n")
    handle.write(f"- Plan: `{plan_path}`\n")
    handle.write(f"- Generated: {datetime.utcnow().isoformat(timespec='seconds')}Z\n")
    handle.write(f"- Source: `{coverage_path}`\n\n")
    if not gaps:
        handle.write("All recorded lines are covered 🎉\n")
    else:
        handle.write("| File | Uncovered Lines | Sample (first 20) |\n")
        handle.write("| --- | ---: | --- |\n")
        for rel, count, sample in gaps:
            sample_str = ", ".join(str(line) for line in sample)
            handle.write(f"| `{rel}` | {count} | `{sample_str}` |\n")

if gaps:
    print(
        f"⚠️  {len(gaps)} files have uncovered lines. "
        f"Detailed report written to {report_path}"
    )
else:
    print(f"✅ All recorded lines covered; report written to {report_path}")
PY
