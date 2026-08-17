#!/bin/sh
# Sandbox-free self-test for scripts/run-all-tests.sh.
#
# The runner's argument validation exits before any cargo invocation, so
# this self-test never runs the real test suite (execution-safe). It
# syntax-checks the runner, exercises --help / unknown-arg handling, and
# asserts the static validation gates exist.
set -eu

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
runner="$repo_root/scripts/run-all-tests.sh"

fail() {
    echo "test-run-all-tests.sh: FAIL: $1" >&2
    exit 1
}

# 1. Syntax check (POSIX sh, matching the runner's shebang).
if ! sh -n "$runner"; then
    fail "runner has a syntax error"
fi

# 2. --help exits 0 and documents usage (before any cargo invocation).
help_out="$(sh "$runner" --help 2>&1)" || fail "--help should exit 0"
case "$help_out" in
    *"usage: run-all-tests.sh"*) ;;
    *) fail "--help missing usage line" ;;
esac

# 3. Unknown arguments exit non-zero with a stderr diagnostic.
if sh "$runner" --bogus >/dev/null 2>&1; then
    fail "--bogus should exit non-zero"
fi
# The assignment lives inside an `if` condition so its non-zero status is
# exempt from `set -e`; the stderr text is what matters here.
if bogus_err="$(sh "$runner" --bogus 2>&1 >/dev/null)"; then
    fail "--bogus should exit non-zero"
fi
case "$bogus_err" in
    *"unknown argument"*) ;;
    *) fail "--bogus missing stderr diagnostic" ;;
esac

# 4. Static gates: the runner must invoke the canonical commands.
runner_text="$(cat "$runner")"
for needle in 'cargo --locked nextest run' 'cargo --locked test --doc --workspace' 'clean-mediapm-temp' 'tempfile::tempdir' '.prefix'; do
    case "$runner_text" in
        *"$needle"*) ;;
        *) fail "runner missing static gate: $needle" ;;
    esac
done

# 5. --large must enable the large-tests Cargo feature (not an env var).
case "$runner_text" in
    *'--features large-tests'*) ;;
    *) fail "runner missing --features large-tests under --large" ;;
esac
case "$runner_text" in
    *'MEDIAPM_RUN_LARGE_TESTS'*) fail "runner must not reference MEDIAPM_RUN_LARGE_TESTS" ;;
esac

echo "test-run-all-tests.sh: OK"
