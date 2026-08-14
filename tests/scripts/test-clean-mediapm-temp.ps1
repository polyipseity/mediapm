#!/usr/bin/env pwsh
# Sandbox self-test for scripts/clean-mediapm-temp.ps1.
#
# Two parts:
#   1. Runtime: under a sandboxed temp root ($env:TMP/$env:TMPDIR/$env:TEMP),
#      the janitor removes exactly the three mediapm-* prefixed dirs in
#      dry-run and real-run and never touches non-mediapm control dirs.
#   2. Static: the janitor source must contain no migration-era workspace
#      globs (cli-add-hierarchy / examples/artifacts / stale stamped) - the
#      janitor scope is the temp-root three prefixes ONLY.
#
# Runs only when pwsh is available (the `mediapm-tests` crate probes and
# skips). CI-covered via the Windows workspace-tests job. Behavioral twin of
# test-clean-mediapm-temp.sh.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$janitor = Join-Path $repoRoot 'scripts/clean-mediapm-temp.ps1'

function Fail([string]$Message) {
    [Console]::Error.WriteLine("test-clean-mediapm-temp.ps1: FAIL: $Message")
    exit 1
}

# --- Runtime part: sandboxed temp root with fake mediapm-* dirs and controls.
$sandbox = Join-Path ([System.IO.Path]::GetTempPath()) ("mediapm-test-" + [System.Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $sandbox | Out-Null

$savedTmp = $env:TMP
$savedTmpDir = $env:TMPDIR
$savedTemp = $env:TEMP
try {
    $env:TMP = $sandbox
    $env:TMPDIR = $sandbox
    $env:TEMP = $sandbox

    New-Item -ItemType Directory -Path (Join-Path $sandbox 'mediapm-artifact-fake1') | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $sandbox 'mediapm-cache-fake2') | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $sandbox 'mediapm-runtime-abcdef1234567890') | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $sandbox 'cli-add-hierarchy-123-456') | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $sandbox 'unrelated-dir') | Out-Null

    # Dry run: reports all three prefixed dirs, never the controls.
    $dryOut = @(& $janitor --dry-run 2>&1)
    $dryCount = @($dryOut | Where-Object { $_ -like 'would remove:*' }).Count
    if ($dryCount -ne 3) { Fail "dry-run reported $dryCount removals, expected 3" }
    foreach ($name in @('mediapm-artifact-fake1', 'mediapm-cache-fake2', 'mediapm-runtime-abcdef1234567890')) {
        $path = Join-Path $sandbox $name
        if (-not ($dryOut -contains "would remove: $path")) { Fail "dry-run missing $name" }
    }
    foreach ($name in @('cli-add-hierarchy-123-456', 'unrelated-dir')) {
        $path = Join-Path $sandbox $name
        if ($dryOut -contains "would remove: $path") { Fail "dry-run reported a control dir ($name)" }
    }

    # Real run: removes exactly the three prefixed dirs, leaves controls.
    $realOut = @(& $janitor 2>&1)
    $realCount = @($realOut | Where-Object { $_ -like 'removed:*' }).Count
    if ($realCount -ne 3) { Fail "real run reported $realCount removals, expected 3" }
    foreach ($name in @('mediapm-artifact-fake1', 'mediapm-cache-fake2', 'mediapm-runtime-abcdef1234567890')) {
        if (Test-Path -LiteralPath (Join-Path $sandbox $name)) { Fail "$name not removed" }
    }
    foreach ($name in @('cli-add-hierarchy-123-456', 'unrelated-dir')) {
        if (-not (Test-Path -LiteralPath (Join-Path $sandbox $name))) { Fail "control $name dir removed" }
    }
} finally {
    $env:TMP = $savedTmp
    $env:TMPDIR = $savedTmpDir
    $env:TEMP = $savedTemp
    Remove-Item -LiteralPath $sandbox -Recurse -Force -ErrorAction SilentlyContinue
}

# --- Static part: migration-era workspace globs must be gone.
$janitorText = Get-Content -LiteralPath $janitor -Raw
if ($janitorText -match 'cli-add-hierarchy|examples/artifacts|stale stamped') {
    Fail 'janitor still references migration-era workspace globs (cli-add-hierarchy/examples/artifacts/stale stamped)'
}

Write-Output 'test-clean-mediapm-temp.ps1: OK'
