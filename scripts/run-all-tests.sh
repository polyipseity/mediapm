#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
usage: run-all-tests.sh [--help] [--large]

Runs the full workspace validation suite:
  - cargo --locked test-all (nextest, all targets and features)
  - cargo --locked test --doc --workspace
  - janitor dry-run gate (leftover mediapm temp dirs fail the suite)
  - unprefixed-tempdir invariant gate

Options:
  --large   opt in to network/external-tool-heavy tests by exporting
             MEDIAPM_RUN_LARGE_TESTS=1 (required by online regression tests)
EOF
}

# Argument validation happens before any `cd` or cargo invocation so the
# `--help`/unknown-arg paths are execution-safe (exercised by the runner
# self-test in tests/scripts/test-run-all-tests.sh).
case "${1:-}" in
    '')
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    --large)
        export MEDIAPM_RUN_LARGE_TESTS=1
        ;;
    *)
        echo "error: unknown argument: $1" >&2
        usage >&2
        exit 1
        ;;
esac

cd "$(git rev-parse --show-toplevel)"

cargo --locked test-all
cargo --locked test --doc --workspace

# Regression gate: the test suite must not leave mediapm-owned temp dirs
# behind. Non-destructive (dry-run only); fails loudly if any leftover
# artifact/cache/runtime dir appears.
# Caveat: concurrent local mediapm processes could trip it spuriously.
# Note: janitor sandbox self-tests now live in the root `tests` crate
# (`cargo --locked test-pkg mediapm-tests`, covered by `cargo test-all`).
if scripts/clean-mediapm-temp.sh --dry-run | grep -q 'would remove'; then
    echo "error: test suite left mediapm temp dirs behind" >&2
    exit 1
fi

# Invariant gate: no bare unprefixed tempdir creation outside the role
# helpers (scanning `src` plus the root `tests` crate). The only allowed
# sites are the prefix-using helpers in `src/mediapm-utils/src/temp.rs`
# (artifact_dir/cache_dir); every other tempdir must go through them.
if grep -rnE 'tempfile::tempdir\(|\.prefix\(' src tests --include='*.rs' \
    | grep -v '^src/mediapm-utils/src/temp.rs:' >/dev/null; then
    echo "error: unprefixed tempdir/prefix use outside src/mediapm-utils/src/temp.rs" >&2
    exit 1
fi
