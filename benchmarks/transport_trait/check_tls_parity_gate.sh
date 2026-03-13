#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

BASELINE_DELIVERED_FILE="${BASELINE_DELIVERED_FILE:-$ROOT_DIR/benchmarks/transport_trait/raw/baseline_pre_trait/remote_routed_tell_delivered_baseline.md}"
BASELINE_ASK_FILE="${BASELINE_ASK_FILE:-$ROOT_DIR/benchmarks/transport_trait/raw/baseline_pre_trait/remote_routed_ask_baseline.md}"

POST_DELIVERED_FILE="${POST_DELIVERED_FILE:-$ROOT_DIR/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_tell_delivered_tls_aggregate6.md}"
POST_ASK_FILE="${POST_ASK_FILE:-$ROOT_DIR/benchmarks/transport_trait/raw/post_trait_tls/remote_routed_ask_tls_aggregate6.md}"

REGRESSION_THRESHOLD_PCT="${REGRESSION_THRESHOLD_PCT:-1}"
POST_MIN_TRIALS="${POST_MIN_TRIALS:-5}"
BASELINE_MIN_TRIALS="${BASELINE_MIN_TRIALS:-3}"
OUT_FILE="${OUT_FILE:-}"

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "missing required file: $path" >&2
    exit 1
  fi
}

require_file "$BASELINE_DELIVERED_FILE"
require_file "$BASELINE_ASK_FILE"
require_file "$POST_DELIVERED_FILE"
require_file "$POST_ASK_FILE"

extract_metric_values() {
  local file="$1"
  local metric_key="$2"
  local grep_pat="$3"

  grep -E "$grep_pat" "$file" \
    | grep "RESULT framework=icanact_remote" \
    | awk -v key="$metric_key" '
      {
        for (i = 1; i <= NF; i++) {
          split($i, kv, "=")
          if (kv[1] == key) {
            print kv[2]
          }
        }
      }
    '
}

stats_from_stdin() {
  local tmp
  tmp="$(mktemp)"
  cat > "$tmp"

  local n
  n="$(grep -c '.' "$tmp" || true)"
  if [[ "$n" -eq 0 ]]; then
    rm -f "$tmp"
    echo "0.00 0.00 0"
    return
  fi

  local mean
  mean="$(awk '{sum += $1; n += 1} END { if (n == 0) print "0.00"; else printf "%.2f", sum / n }' "$tmp")"

  local std
  std="$(awk '{vals[++n]=$1; sum+=$1} END { if (n == 0) { print "0.00"; exit } mean=sum/n; for (i=1;i<=n;i++) { d=vals[i]-mean; ss += d*d } printf "%.6f", sqrt(ss/n) }' "$tmp")"

  local cv
  cv="$(awk -v m="$mean" -v s="$std" 'BEGIN { if (m == 0) print "0.00"; else printf "%.2f", (s / m) * 100 }')"

  local median
  median="$(sort -g "$tmp" | awk -v n="$n" '
    { vals[NR] = $1 }
    END {
      if (n == 0) {
        print "0.00"
      } else if (n % 2 == 1) {
        printf "%.2f", vals[(n + 1) / 2]
      } else {
        printf "%.2f", (vals[n / 2] + vals[n / 2 + 1]) / 2
      }
    }
  ')"

  rm -f "$tmp"
  echo "$median $cv $n"
}

collect_stats() {
  local file="$1"
  local metric_key="$2"
  local grep_pat="$3"

  local values
  values="$(extract_metric_values "$file" "$metric_key" "$grep_pat" || true)"
  if [[ -z "$values" ]]; then
    echo "0.00 0.00 0"
    return
  fi
  printf "%s\n" "$values" | stats_from_stdin
}

compute_delta_pct() {
  local baseline="$1"
  local post="$2"
  awk -v b="$baseline" -v p="$post" 'BEGIN { if (b == 0) print "0.00"; else printf "%.2f", ((p - b) / b) * 100 }'
}

regression_gate() {
  local delta_pct="$1"
  local threshold="$2"
  awk -v d="$delta_pct" -v t="$threshold" 'BEGIN { if (d < -t) print "FAIL"; else print "PASS" }'
}

# Tell send-loop completion throughput from delivery-verified runs (sender-side send metric)
read -r B_TELL_SEND_MED B_TELL_SEND_CV B_TELL_SEND_N <<< "$(collect_stats "$BASELINE_DELIVERED_FILE" "send_msgs_per_sec" "^- icanact routed trial [0-9]+ sender:")"
read -r P_TELL_SEND_MED P_TELL_SEND_CV P_TELL_SEND_N <<< "$(collect_stats "$POST_DELIVERED_FILE" "send_msgs_per_sec" "^- icanact routed trial [0-9]+ sender:")"

# Tell delivery-verified throughput (sender-side e2e metric from delivered run)
read -r B_TELL_DV_MED B_TELL_DV_CV B_TELL_DV_N <<< "$(collect_stats "$BASELINE_DELIVERED_FILE" "e2e_msgs_per_sec" "^- icanact routed trial [0-9]+ sender:")"
read -r P_TELL_DV_MED P_TELL_DV_CV P_TELL_DV_N <<< "$(collect_stats "$POST_DELIVERED_FILE" "e2e_msgs_per_sec" "^- icanact routed trial [0-9]+ sender:")"

# Ask throughput (timeout path) from sender-side ask send throughput
read -r B_ASK_MED B_ASK_CV B_ASK_N <<< "$(collect_stats "$BASELINE_ASK_FILE" "send_msgs_per_sec" "^- icanact routed trial [0-9]+ sender: .*wait_mode=timeout")"
read -r P_ASK_MED P_ASK_CV P_ASK_N <<< "$(collect_stats "$POST_ASK_FILE" "send_msgs_per_sec" "^- icanact routed trial [0-9]+ sender: .*wait_mode=timeout")"

OVERALL_FAIL=0

metric_row() {
  local label="$1"
  local baseline_med="$2"
  local baseline_cv="$3"
  local baseline_n="$4"
  local post_med="$5"
  local post_cv="$6"
  local post_n="$7"

  local delta
  local gate
  local trials_gate="PASS"

  delta="$(compute_delta_pct "$baseline_med" "$post_med")"
  gate="$(regression_gate "$delta" "$REGRESSION_THRESHOLD_PCT")"

  if (( baseline_n < BASELINE_MIN_TRIALS )) || (( post_n < POST_MIN_TRIALS )); then
    trials_gate="FAIL"
    gate="FAIL"
  fi

  if [[ "$gate" == "FAIL" ]]; then
    OVERALL_FAIL=1
  fi

  LAST_ROW="$(printf "| %s | %.2f | %.2f%% (%d) | %.2f | %.2f%% (%d) | %s%% | %s | %s |" \
    "$label" "$baseline_med" "$baseline_cv" "$baseline_n" "$post_med" "$post_cv" "$post_n" "$delta" "$trials_gate" "$gate")"
}

RUN_TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

REPORT_HEADER="$(cat <<MARKDOWN
# TLS Parity Gate Check

- Run timestamp (UTC): $RUN_TS
- Regression threshold: max ${REGRESSION_THRESHOLD_PCT}% slowdown (improvements are allowed)
- Post-trait minimum trials per metric: $POST_MIN_TRIALS
- Baseline minimum trials per metric: $BASELINE_MIN_TRIALS
- Delivery-verified tell metric source: sender-side e2e_msgs_per_sec from delivery-verified run

| Critical metric (routed) | Baseline median | Baseline CV (n) | Post median | Post CV (n) | Delta (post vs baseline) | Trial gate | Regression gate |
|---|---:|---:|---:|---:|---:|---|---|
MARKDOWN
)"

metric_row "Tell send-loop completion throughput (msg/s, delivered run sender send)" "$B_TELL_SEND_MED" "$B_TELL_SEND_CV" "$B_TELL_SEND_N" "$P_TELL_SEND_MED" "$P_TELL_SEND_CV" "$P_TELL_SEND_N"
ROW1="$LAST_ROW"
metric_row "Tell delivery-verified throughput (msg/s, sender e2e)" "$B_TELL_DV_MED" "$B_TELL_DV_CV" "$B_TELL_DV_N" "$P_TELL_DV_MED" "$P_TELL_DV_CV" "$P_TELL_DV_N"
ROW2="$LAST_ROW"
metric_row "Ask throughput (req/s, inflight=32, timeout path)" "$B_ASK_MED" "$B_ASK_CV" "$B_ASK_N" "$P_ASK_MED" "$P_ASK_CV" "$P_ASK_N"
ROW3="$LAST_ROW"

REPORT="$REPORT_HEADER
$ROW1
$ROW2
$ROW3"

if [[ -n "$OUT_FILE" ]]; then
  printf "%s\n" "$REPORT" > "$OUT_FILE"
fi

printf "%s\n" "$REPORT"

if [[ "$OVERALL_FAIL" -ne 0 ]]; then
  exit 2
fi
