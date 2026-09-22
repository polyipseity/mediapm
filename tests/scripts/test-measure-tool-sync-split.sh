#!/bin/sh
# Self-test for scripts/measure-tool-sync-split.sh offline parser mode.
#
# Tests the --ticks parser against committed JSONL fixtures that exercise:
#   - Known-good case (basic.jsonl): res/fch/pro/pro tail, status-based activity
#   - D2/D3/D4 defects (defects-d2-d4.jsonl): phase in label only, count/total suffix, indeterminate bar
#   - D5 defect (warm-interleaved.jsonl): interleaved workflow ticks filtered by group isolation
#
# POSIX sh (driven by the mediapm-tests crate via cargo test-all; also runnable standalone).
set -eu

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
script="$repo_root/scripts/measure-tool-sync-split.sh"
fixtures="$repo_root/tests/scripts/fixtures/measure-tool-sync"
fail_count=0

fail() {
    echo "test-measure-tool-sync-split: FAIL: $1" >&2
    if [ -n "${2:-}" ]; then
        echo "  expected: $2" >&2
        echo "  got:      $3" >&2
    fi
    fail_count=$((fail_count + 1))
}

check_contains() {
    output="$1"
    pattern="$2"
    label="$3"
    case "$output" in
        *"$pattern"*) ;;
        *) fail "$label: output does not contain '$pattern'" "$pattern" "(missing)" ;;
    esac
}

check_not_contains() {
    output="$1"
    pattern="$2"
    label="$3"
    case "$output" in
        *"$pattern"*) fail "$label: output unexpectedly contains '$pattern'" "(no $pattern)" "$pattern" ;;
        *) ;;
    esac
}

# --- Test 1: basic.jsonl ---
echo "--- test: basic.jsonl ---" >&2
if output=$(bash "$script" --ticks "$fixtures/basic.jsonl" 2>&1); then
    check_contains "$output" "total_tool_sync:" "basic"
    check_contains "$output" "R_split: fetch=" "basic"
    check_contains "$output" "per_tool" "basic"
    check_contains "$output" "yt-dlp:" "basic"
    # basic.jsonl has res (indeterminate), fch (0/3, 2/3, 3/3), pro (0/3, 2/3, 3/3)
    # fch active ticks: 1→2 (0.5s), tail at tick3 (1.0s) = 1.5s fetch
    # pro active ticks: 3→4 (1.0s), tail at tick4 (0s) = 1.0s proc
    check_contains "$output" "fetch=1.5s" "basic: fetch delta"
    check_contains "$output" "process=2.0s" "basic: proc delta"
else
    fail "basic.jsonl: script exited non-zero"
fi

# --- Test 2: defects-d2-d4.jsonl ---
echo "--- test: defects-d2-d4.jsonl ---" >&2
if output=$(bash "$script" --ticks "$fixtures/defects-d2-d4.jsonl" 2>&1); then
    # D2: [res] found in label (prefix is empty) → parser must extract from label
    # D3: [fch] found with "0/3" / "3/3" suffix → regex must match before suffix
    # D4: [res] bar with total=0 and status=Active → must be treated as active
    check_contains "$output" "total_tool_sync:" "defects"
    check_contains "$output" "yt-dlp:" "defects"
    check_not_contains "$output" "no tool-sync ticks" "defects: must find tool-sync ticks despite D2/D4"
else
    fail "defects-d2-d4.jsonl: script exited non-zero"
fi

# --- Test 3: warm-interleaved.jsonl ---
echo "--- test: warm-interleaved.jsonl ---" >&2
if output=$(bash "$script" --ticks "$fixtures/warm-interleaved.jsonl" 2>&1); then
    # D5: workflow ticks (with [wf]) must NOT appear in tool-sync group
    # Tool-sync group: ticks 1-3 (have res/fch/pro bars), workflow only: tick 0, tick 4
    # Group isolation filters to ticks with >=1 active res/fch/pro bar
    check_contains "$output" "total_tool_sync:" "warm-interleaved"
    check_contains "$output" "yt-dlp:" "warm-interleaved"
    # The [wf] workflow ticks must not contribute to tool-sync time
    check_not_contains "$output" "workflow:" "warm-interleaved: workflow bars must not appear in per_tool"
else
    fail "warm-interleaved.jsonl: script exited non-zero"
fi

# --- Test 4: unknown arg ---
echo "--- test: unknown arg ---" >&2
if bash "$script" --bogus 2>/dev/null; then
    fail "unknown arg: should exit non-zero"
fi

# --- Test 5: --ticks with missing file ---
echo "--- test: --ticks missing file ---" >&2
if bash "$script" --ticks /nonexistent/file.jsonl 2>/dev/null; then
    fail "missing file: should exit non-zero"
fi

# --- Test 6: --help ---
echo "--- test: --help ---" >&2
if ! bash "$script" --help >/dev/null 2>&1; then
    fail "--help: should exit zero"
fi

# --- Summary ---
if [ "$fail_count" -gt 0 ]; then
    echo "test-measure-tool-sync-split: $fail_count assertion(s) FAILED" >&2
    exit 1
fi
echo "test-measure-tool-sync-split: all assertions passed" >&2
exit 0
