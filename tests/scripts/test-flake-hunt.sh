#!/usr/bin/env bash
# Self-test for scripts/flake-hunt.sh.
#
# Proves the harness reports a seeded failure with exit 1 and a clean run with
# exit 0. The exit-0 case is the load-bearing one: a harness that always
# exited 0 would silently invalidate every measurement taken with it.
#
# The seeded tests in tests/scripts/mod.rs are gated on
# MEDIAPM_FLAKE_HUNT_SEEDED, so the ordinary gate stays green while this
# self-test drives one of them to a deterministic failure. Only the failing
# invocation sets the variable; the clean invocation must not see it.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
harness="${repo_root}/scripts/flake-hunt.sh"

out_dir="$(mktemp -d)"
trap 'rm -rf "$out_dir"' EXIT

fail_count=0
note() {
    echo "FAIL: $1" >&2
    fail_count=$((fail_count + 1))
}

# --- seeded failure: must be named, and must exit non-zero ---
failing_out="${out_dir}/failing.log"
if MEDIAPM_FLAKE_HUNT_SEEDED=1 "${harness}" --runs 2 --test seeded_failing_test \
    >"${failing_out}" 2>&1; then
    note "expected non-zero exit for a seeded failing test"
fi
# nextest names a test "<binary> <test path>", so the FLAKE line carries the
# binary as a prefix: match the test name as a suffix, not a whole line.
if ! grep -qE '^FLAKE: .*seeded_failing_test$' "${failing_out}"; then
    note "expected a FLAKE line naming seeded_failing_test"
    cat "${failing_out}" >&2
fi
if ! grep -q '^run 2: FAIL' "${failing_out}"; then
    note "expected a per-run FAIL line for the seeded failure"
fi

# --- clean run: must exit zero and report no flakes ---
clean_out="${out_dir}/clean.log"
if ! "${harness}" --runs 2 --test seeded_passing_test >"${clean_out}" 2>&1; then
    note "expected zero exit for a seeded passing test"
    cat "${clean_out}" >&2
fi
if grep -q 'FLAKE:' "${clean_out}"; then
    note "unexpected FLAKE line in a clean run"
    cat "${clean_out}" >&2
fi
if ! grep -q '^run 1: PASS' "${clean_out}" || ! grep -q '^run 2: PASS' "${clean_out}"; then
    note "expected a per-run PASS line for each of the 2 clean runs"
fi

if [ "$fail_count" -ne 0 ]; then
    echo "test-flake-hunt: ${fail_count} failure(s)" >&2
    exit 1
fi
echo "test-flake-hunt: OK"
