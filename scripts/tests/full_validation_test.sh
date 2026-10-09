#!/bin/bash
# Isolated fake-Cargo harness; never builds or runs project code.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/project/scripts" "$tmp/bin"
cp "$root/scripts/full_validation.sh" "$tmp/project/scripts/"
touch "$tmp/project/Cargo.toml"
for guard in check_no_rkyv_from_bytes check_forbidden_copy_patterns; do
    printf '#!/bin/bash\nexit 0\n' > "$tmp/project/scripts/$guard.sh"
    chmod +x "$tmp/project/scripts/$guard.sh"
done
cat > "$tmp/bin/cargo" <<'CARGO'
#!/bin/bash
echo "$*" >> "$CALLS"
case "$SCENARIO" in
    missing) exit 127 ;;
    compile) exit 101 ;;
    assertion) if [[ " $* " == *' test '* && " $* " != *' --list '* ]]; then exit 1; fi ;;
    flaky) [[ $(wc -l < "$CALLS") -gt 1 ]] || exit 1 ;;
esac
if [[ " $* " == *' --list '* ]]; then
    [[ "$SCENARIO" == zero ]] || echo 'fixture::test: test'
elif [[ " $* " == *' test '* ]]; then
    echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;'
fi
CARGO
chmod +x "$tmp/bin/cargo"
export PATH="$tmp/bin:$PATH" CALLS="$tmp/calls"
for scenario in missing compile assertion flaky zero success; do
    export SCENARIO="$scenario"
    : > "$CALLS"
    status=0
    if [[ "$scenario" == zero ]]; then
        bash "$tmp/project/scripts/full_validation.sh" --focus absent > "$tmp/output" 2>&1 || status=$?
    else
        bash "$tmp/project/scripts/full_validation.sh" > "$tmp/output" 2>&1 || status=$?
    fi
    if [[ "$scenario" == success ]]; then
        [[ "$status" == 0 && $(wc -l < "$CALLS") == 18 ]]
        grep -q -- '--all-features' "$CALLS"
        grep -q -- 'test-helpers' "$CALLS"
        grep -q -- '--release' "$CALLS"
        grep -q 'TEST_COUNTS selected=1 executed=1' "$tmp/output"
        grep -q 'VALIDATION COMPLETE' "$tmp/output"
    else
        expected_calls=1
        [[ "$scenario" != assertion ]] || expected_calls=8
        [[ "$status" != 0 && $(wc -l < "$CALLS") == "$expected_calls" ]]
        ! grep -q 'VALIDATION COMPLETE' "$tmp/output"
    fi
done
export SCENARIO=success
: > "$CALLS"
bash "$tmp/project/scripts/full_validation.sh" --focus fixture > "$tmp/output" 2>&1
[[ $(wc -l < "$CALLS") == 2 ]]
grep -q 'FOCUSED PASS (not full validation)' "$tmp/output"
echo 'PASS: validation harness failure/selection/lane tests'
