#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
usage: run-all-tests.sh [--large] [--help]

Runs the full workspace validation suite:
  - cargo nextest run --workspace --all-targets (default features)
  - cargo --locked test --doc --workspace
  - janitor dry-run gate (leftover mediapm temp dirs fail the suite)
  - unprefixed-tempdir invariant gate

Options:
  --large   enable the `large-tests` Cargo feature (runs network/external-tool
            heavy tests such as mediapm-cas streaming_large). The online demo
            YouTube-download regression is gated separately by the 3-level
            mechanism, not by this flag.
EOF
}

# Argument validation happens before any `cd` or cargo invocation so the
# `--help`/unknown-arg paths are execution-safe (exercised by the runner
# self-test in tests/scripts/test-run-all-tests.sh).
LARGE=0
case "${1:-}" in
    '')
        ;;
    --large)
        LARGE=1
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        echo "error: unknown argument: $1" >&2
        usage >&2
        exit 1
        ;;
esac

cd "$(git rev-parse --show-toplevel)"

# Default run uses default features (all features except the opt-in
# `large-tests` feature). `--large` adds `--features large-tests`. We invoke
# nextest directly rather than the `test-all` alias so we control the feature
# set; `test-all` stays `--all-features` for other consumers (e.g. pre-push).
if [ "$LARGE" -eq 1 ]; then
    cargo --locked nextest run --workspace --all-targets --features large-tests
else
    cargo --locked nextest run --workspace --all-targets
fi
cargo --locked test --doc --workspace

# Regression gate: the test suite must not leave mediapm-owned temp dirs
# behind. Non-destructive (dry-run only); fails loudly if any leftover
# artifact/cache/runtime dir appears.
# Caveat: concurrent local mediapm processes could trip it spuriously.
# Note: janitor sandbox self-tests now live in the root `tests` crate
# (`cargo --locked test-pkg mediapm-tests`, covered by `cargo test-all`).
# The janitor's own exit status is read before its output: a temp root that
# cannot be scanned prints nothing that reads as a leftover, so a text-only
# gate would take the failure for a clean sweep.
dry_run_out="$(scripts/clean-mediapm-temp.sh --dry-run 2>&1)" || {
    echo "error: mediapm temp-dir sweep failed: $dry_run_out" >&2
    exit 1
}
# The capture above also swallows the janitor's own diagnostics on the
# success path, which is where they are least expected. Anything that is not
# one of the janitor's output-contract lines goes back out on stderr, so a
# warning it grows later cannot vanish into this variable.
printf '%s\n' "$dry_run_out" |
    grep -vE '^(would remove: |removed: |would remove [0-9]+ mediapm temp director\(ies\)$|removed [0-9]+ mediapm temp director\(ies\)$|no mediapm temp directories found$)' >&2 ||
    true # check-suppress:suppression_doc: grep exits 1 when every line is a known-good one, which is the passing case.
if printf '%s\n' "$dry_run_out" | grep -q 'would remove'; then
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
