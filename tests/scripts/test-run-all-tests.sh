#!/bin/sh
# Self-test for scripts/run-all-tests.sh.
#
# The runner's argument validation exits before any cargo invocation, so
# this self-test never runs the real test suite (execution-safe). It
# syntax-checks the runner, exercises --help / unknown-arg handling, and
# asserts the static validation gates exist. The temp-dir gate is exercised
# for real against a stub `cargo` on PATH, which lets the runner reach its
# gates without running the workspace suite.
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

# 6. Temp-dir gate: a sweep that fails must fail the runner. A stub `cargo`
#    on PATH lets the real runner reach its gates without running the
#    workspace suite.
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT INT TERM
mkdir -p "$tmpdir/bin"
printf '#!/bin/sh\nexit 0\n' >"$tmpdir/bin/cargo"
chmod +x "$tmpdir/bin/cargo"

# A missing temp root: the janitor cannot scan it and exits non-zero. The
# runner must fail and name the cause rather than read the empty sweep as
# clean. The assignment lives inside an `if` condition so its non-zero
# status is exempt from `set -e`.
missing_root="$tmpdir/missing-root"
if gate_out="$(cd "$repo_root" && PATH="$tmpdir/bin:$PATH" TMPDIR="$missing_root" sh "$runner" 2>&1)"; then
    fail "runner passed its temp-dir gate with a missing temp root ($missing_root)"
fi
case "$gate_out" in
    *"error: mediapm temp-dir sweep failed: no such directory: $missing_root"*) ;;
    *) fail "missing-root gate failure did not name the cause: $gate_out" ;;
esac

# The other route stays: a sweep that exits 0 and reports leftovers still
# fails, with its own message so the two failures read differently.
leftover_root="$tmpdir/leftover-root"
mkdir -p "$leftover_root/mediapm-artifact-fake"
if leftover_out="$(cd "$repo_root" && PATH="$tmpdir/bin:$PATH" TMPDIR="$leftover_root" sh "$runner" 2>&1)"; then
    fail "runner passed its temp-dir gate with leftover dirs behind ($leftover_root)"
fi
case "$leftover_out" in
    *"error: test suite left mediapm temp dirs behind"*) ;;
    *) fail "leftover gate failure missing its message: $leftover_out" ;;
esac

# A root that exists and holds nothing: the sweep scans it, finds no
# leftovers, and the runner passes. The two failure routes above cannot tell
# whether this case still works, so the runner layer asserts it too. The
# assignment lives inside an `if` condition so `set -e` does not abort the
# self-test before the diagnostic is reported.
empty_root="$tmpdir/empty-root"
mkdir -p "$empty_root"
if ! empty_out="$(cd "$repo_root" && PATH="$tmpdir/bin:$PATH" TMPDIR="$empty_root" sh "$runner" 2>&1)"; then
    fail "runner failed its temp-dir gate on an existing empty temp root ($empty_root): $empty_out"
fi

echo "test-run-all-tests.sh: OK"
