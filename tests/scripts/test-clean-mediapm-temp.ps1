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
#   3. Temp root preconditions: a temp root that does not exist, or that
#      exists but cannot be enumerated, must fail and name that path; an
#      existing empty one must report a clean sweep.
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

# Runs the janitor in a child pwsh with the temp-root env vars pointed at
# $Root, so the janitor's own `exit` does not end this self-test. Returns the
# child exit code and its merged output.
function Invoke-JanitorAtRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [string[]]$JanitorArgs = @()
    )

    $savedTmp = $env:TMP
    $savedTmpDir = $env:TMPDIR
    $savedTemp = $env:TEMP
    try {
        $env:TMP = $Root
        $env:TMPDIR = $Root
        $env:TEMP = $Root
        $lines = @(& pwsh -NoProfile -File $janitor @JanitorArgs 2>&1)
        return [pscustomobject]@{
            ExitCode = $LASTEXITCODE
            Output   = ($lines -join "`n")
        }
    } finally {
        $env:TMP = $savedTmp
        $env:TMPDIR = $savedTmpDir
        $env:TEMP = $savedTemp
    }
}

# --- Preconditions: a missing temp root must fail, an existing empty one passes.
$missingRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("mediapm-clean-mediapm-temp-missing-" + [System.Guid]::NewGuid().ToString('N'))
$missing = Invoke-JanitorAtRoot -Root $missingRoot -JanitorArgs @('--dry-run')
if ($missing.ExitCode -eq 0) { Fail "janitor exited 0 for a missing temp root ($missingRoot)" }
if (-not $missing.Output.Contains("no such directory: $missingRoot")) {
    Fail "missing-root diagnostic did not name ${missingRoot}: $($missing.Output)"
}

$emptyRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("mediapm-clean-mediapm-temp-empty-" + [System.Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $emptyRoot | Out-Null
try {
    $empty = Invoke-JanitorAtRoot -Root $emptyRoot -JanitorArgs @('--dry-run')
    if ($empty.ExitCode -ne 0) { Fail "janitor exited $($empty.ExitCode) for an empty temp root ($emptyRoot): $($empty.Output)" }
    if (-not $empty.Output.Contains('no mediapm temp directories found')) {
        Fail "empty temp root did not report a clean sweep: $($empty.Output)"
    }
} finally {
    Remove-Item -LiteralPath $emptyRoot -Recurse -Force -ErrorAction SilentlyContinue
}

# A temp root that exists but cannot be enumerated is the same hazard as a
# missing one: Get-ChildItem throws UnauthorizedAccessException, which is
# terminating under $ErrorActionPreference = 'Stop', so this twin already
# fails closed where the sh twin used to report a clean run it never
# performed. Assert the exit code only: the diagnostic is .NET's, not the
# janitor's. SetUnixFileMode is .NET 7+ and Unix-only, and root ignores the
# permission bits, so the case is skipped on Windows and as root.
if (-not $IsWindows) {
    $deniedRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("mediapm-clean-mediapm-temp-denied-" + [System.Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $deniedRoot | Out-Null
    [System.IO.File]::SetUnixFileMode($deniedRoot, [System.IO.UnixFileMode]::None)
    try {
        if ((& id -u) -eq 0) {
            Write-Output 'test-clean-mediapm-temp.ps1: skipped denied-root case (running as root)'
        } else {
            $denied = Invoke-JanitorAtRoot -Root $deniedRoot -JanitorArgs @('--dry-run')
            if ($denied.ExitCode -eq 0) { Fail "janitor exited 0 for a temp root it cannot enumerate ($deniedRoot)" }
        }
    } finally {
        # Restore the mode so Remove-Item can reach inside the dir.
        [System.IO.File]::SetUnixFileMode($deniedRoot, ([System.IO.UnixFileMode]::UserRead -bor [System.IO.UnixFileMode]::UserWrite -bor [System.IO.UnixFileMode]::UserExecute))
        Remove-Item -LiteralPath $deniedRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}

# --- Static part: migration-era workspace globs must be gone.
$janitorText = Get-Content -LiteralPath $janitor -Raw
if ($janitorText -match 'cli-add-hierarchy|examples/artifacts|stale stamped') {
    Fail 'janitor still references migration-era workspace globs (cli-add-hierarchy/examples/artifacts/stale stamped)'
}

Write-Output 'test-clean-mediapm-temp.ps1: OK'
