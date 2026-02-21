#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLAN_PATH="${1:-sprints/LEGACY_FUNCTION_CLEANUP/sprint_3.md}"

if [[ "${SKIP_COVERAGE_REBUILD:-0}" != "1" ]]; then
  "${ROOT_DIR}/scripts/coverage.sh"
fi

COVERAGE_FILE="${ROOT_DIR}/reports/coverage.lcov"
if [[ ! -f "${COVERAGE_FILE}" ]]; then
  echo "Coverage report not found at ${COVERAGE_FILE}." >&2
  echo "Re-run without SKIP_COVERAGE_REBUILD=1 to regenerate coverage." >&2
  exit 1
fi

python3 - <<'PY' "${ROOT_DIR}" "${COVERAGE_FILE}" "${PLAN_PATH}"
import pathlib
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
coverage_path = pathlib.Path(sys.argv[2])
plan_path = sys.argv[3]

try:
    rg_proc = subprocess.run(
        [
            "rg",
            "--line-number",
            "--no-heading",
            "CRITICAL_PATH",
            str(root / "src"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
except subprocess.CalledProcessError as err:
    if err.returncode == 1:
        print("No CRITICAL_PATH markers were found under src/")
        sys.exit(0)
    raise

critical_lines = []
for line in rg_proc.stdout.strip().splitlines():
    if not line.strip():
        continue
    rel_path, line_no, *_ = line.split(":", 2)
    critical_lines.append((root / rel_path, int(line_no)))

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

missing = []
for file_path, line_no in critical_lines:
    lookup_keys = [
        file_path,
        file_path.resolve(),
        pathlib.Path(str(file_path.resolve())),
    ]
    line_hits = None
    found_line = line_no
    for key in lookup_keys:
        if key in coverage:
            hits_map = coverage[key]
            line_hits = hits_map.get(line_no)
            if line_hits is None:
                for delta in range(1, 26):
                    candidate = line_no + delta
                    if candidate in hits_map:
                        line_hits = hits_map[candidate]
                        found_line = candidate
                        break
            break
    if line_hits is None:
        missing.append((file_path.relative_to(root), line_no, "no coverage data"))
    elif line_hits == 0:
        missing.append(
            (file_path.relative_to(root), found_line, "executed 0 times (closest line)")
        )

if missing:
    print(f"❌ CRITICAL_PATH coverage gate failed for plan {plan_path}:")
    for rel_path, line_no, reason in missing:
        print(f"  - {rel_path}:{line_no} -> {reason}")
    sys.exit(1)

print(
    f"✅ CRITICAL_PATH coverage gate passed for plan {plan_path} "
    f"({len(critical_lines)} annotated locations checked)"
)
PY
