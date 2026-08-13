#!/bin/sh
set -eu

cd "$(git rev-parse --show-toplevel)"

cargo --locked test-all
cargo --locked test --doc --workspace

# Janitor sandbox self-tests: POSIX sh always; PowerShell twin when pwsh is
# available (not CI-covered, Linux-only CI has no pwsh).
scripts/test-clean-mediapm-temp.sh
if command -v pwsh >/dev/null 2>&1; then
    scripts/test-clean-mediapm-temp.ps1
fi

# Regression gate: the test suite must not leave mediapm-owned temp dirs
# behind. Non-destructive (dry-run only); fails loudly if any leftover
# artifact/cache/runtime dir appears.
# Caveat: concurrent local mediapm processes could trip it spuriously.
if scripts/clean-mediapm-temp.sh --dry-run | grep -q 'would remove'; then
    echo "error: test suite left mediapm temp dirs behind" >&2
    exit 1
fi
