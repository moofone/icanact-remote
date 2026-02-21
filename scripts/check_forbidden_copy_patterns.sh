#!/bin/bash
set -euo pipefail

CRITICAL_FILES=(
  "src/connection_pool.rs"
  "src/peer_discovery.rs"
)

patterns=("Bytes::copy_from_slice" "extend_from_slice")

found=0
for file in "${CRITICAL_FILES[@]}"; do
  if [ ! -f "$file" ]; then
    continue
  fi
  for pattern in "${patterns[@]}"; do
    if rg --with-filename --line-number --fixed-strings "$pattern" "$file" | grep -v "ALLOW_COPY" >/tmp/forbidden_copy_matches.$$; then
      if [ $found -eq 0 ]; then
        echo "Forbidden copy patterns detected:" >&2
      fi
      cat /tmp/forbidden_copy_matches.$$ >&2
      echo "  -> Pattern '$pattern' found in $file" >&2
      found=1
    fi
  done
  rm -f /tmp/forbidden_copy_matches.$$
done

if [ $found -ne 0 ]; then
  echo "ERROR: Forbidden copy patterns present in critical files. See matches above." >&2
  exit 1
fi

echo "OK: no forbidden copy patterns detected in critical files."
