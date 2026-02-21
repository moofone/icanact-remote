#!/bin/bash
set -euo pipefail

if rg -n "rkyv::from_bytes" src | grep -v "ALLOW_RKYV_FROM_BYTES"; then
  echo "ERROR: rkyv::from_bytes found in src/ (runtime code). Use valid zero-copy deserialization or add ALLOW_RKYV_FROM_BYTES to exempt safe usages."
  exit 1
fi

echo "OK: no rkyv::from_bytes usage found in src/"
