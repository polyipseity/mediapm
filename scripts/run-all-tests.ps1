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
usage: run-all-tests.ps1 [--large] [--help]

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
'@
}

# Argument validation happens before any `Set-Location` or cargo
# invocation so the `--help`/unknown-arg paths are execution-safe
# (exercised by the runner self-test in tests/scripts/test-run-all-tests.ps1).
$LARGE = $false
switch ($Arg) {
    '' { }
    '--large' { $LARGE = $true }
    { $_ -in @('-h', '--help') } {
        Show-Usage
        exit 0
    }
    default {
        [Console]::Error.WriteLine("error: unknown argument: $Arg")
        Show-Usage | ForEach-Object { [Console]::Error.WriteLine($_) }
        exit 1
    }
}

Set-Location (git rev-parse --show-toplevel)

# Default run uses default features (all features except the opt-in
# `large-tests` feature). `--large` adds `--features large-tests`. We invoke
# nextest directly rather than the `test-all` alias so we control the feature
# set; `test-all` stays `--all-features` for other consumers (e.g. pre-push).
if ($LARGE) {
    cargo --locked nextest run --workspace --all-targets --features large-tests
} else {
    cargo --locked nextest run --workspace --all-targets
}
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
