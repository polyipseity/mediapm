#!/usr/bin/env pwsh
param([string]$Arg = '')

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# `pwsh -File script.ps1 --token` leaves dash-prefixed tokens unbound in
# $args (they do not bind to the positional param); fold the first one in
# so `--help`/unknown-arg handling works for that invocation shape too.
if ($args.Count -gt 0 -and $Arg -eq '') { $Arg = $args[0] }

function Show-Usage {
    @'
usage: run-all-tests.ps1 [--help] [--large]

Runs the full workspace validation suite:
  - cargo --locked test-all (nextest, all targets and features)
  - cargo --locked test --doc --workspace
  - janitor dry-run gate (leftover mediapm temp dirs fail the suite)
  - unprefixed-tempdir invariant gate

Options:
  --large   opt in to network/external-tool-heavy tests by exporting
             MEDIAPM_RUN_LARGE_TESTS=1 (required by online regression tests)
'@
}

# Argument validation happens before any `Set-Location` or cargo
# invocation so the `--help`/unknown-arg paths are execution-safe
# (exercised by the runner self-test in tests/scripts/test-run-all-tests.ps1).
switch ($Arg) {
    '' { }
    { $_ -in @('-h', '--help') } {
        Show-Usage
        exit 0
    }
    '--large' {
        $env:MEDIAPM_RUN_LARGE_TESTS = '1'
    }
    default {
        [Console]::Error.WriteLine("error: unknown argument: $Arg")
        Show-Usage | ForEach-Object { [Console]::Error.WriteLine($_) }
        exit 1
    }
}

Set-Location (git rev-parse --show-toplevel)

cargo --locked test-all
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

cargo --locked test --doc --workspace
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Regression gate: the test suite must not leave mediapm-owned temp dirs
# behind. Non-destructive (dry-run only); fails loudly if any leftover
# artifact/cache/runtime dir appears. Parity with run-all-tests.sh; the
# ps1 runner self-test is CI-covered via the Windows workspace-tests job.
# Note: janitor sandbox self-tests now live in the root `tests` crate
# (`cargo --locked test-pkg mediapm-tests`, covered by `cargo test-all`).
$dryRun = @(& "$PSScriptRoot/clean-mediapm-temp.ps1" --dry-run 2>&1)
if (@($dryRun | Where-Object { $_ -like 'would remove:*' }).Count -gt 0) {
    [Console]::Error.WriteLine('error: test suite left mediapm temp dirs behind')
    exit 1
}

# Invariant gate: no bare unprefixed tempdir creation outside the role
# helpers (scanning `src` plus the root `tests` crate). The only allowed
# sites are the prefix-using helpers in `src/mediapm-utils/src/temp.rs`
# (artifact_dir/cache_dir); every other tempdir must go through them.
# Parity with run-all-tests.sh.
$violations = Get-ChildItem -Recurse -File -Include '*.rs' -Path src, tests |
    Where-Object { $_.FullName -notlike '*mediapm-utils*src*temp.rs' } |
    Select-String -Pattern 'tempfile::tempdir\(|\.prefix\('
if ($violations) {
    [Console]::Error.WriteLine('error: unprefixed tempdir/prefix use outside src/mediapm-utils/src/temp.rs')
    exit 1
}
