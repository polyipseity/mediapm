#!/usr/bin/env pwsh
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

cargo --locked test-all
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

cargo --locked test --doc --workspace
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Regression gate: the test suite must not leave mediapm-owned temp dirs
# behind. Non-destructive (dry-run only); fails loudly if any leftover
# artifact/cache/runtime dir appears. Parity with run-all-tests.sh; not
# CI-covered (Linux-only CI has no pwsh).
# Note: janitor sandbox self-tests now live in the root `tests` crate
# (`cargo --locked test-pkg mediapm-tests`, covered by `cargo test-all`).
$dryRun = @(& "$PSScriptRoot/clean-mediapm-temp.ps1" --dry-run 2>&1)
if (@($dryRun | Where-Object { $_ -like 'would remove:*' }).Count -gt 0) {
    [Console]::Error.WriteLine('error: test suite left mediapm temp dirs behind')
    exit 1
}
