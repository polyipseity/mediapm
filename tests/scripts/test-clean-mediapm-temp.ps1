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
#   4. Refusal: a removal that cannot happen must fail the janitor at its
#      preflight, before it reports any removal, leaving every seeded path.
#   5. Per-path failure: a directory the sweep cannot remove is named on
#      stderr, still fails the run, and does not stop the rest of the sweep.
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
# child exit code, its merged output, and the temp root that child resolves
# under the same override.
#
# `ResolvedTempRoot` is the evidence the missing-root case needs: the janitor
# reads its root from `[System.IO.Path]::GetTempPath()`, so a child that
# resolved a different root than the one it was given sweeps that other root
# and reports a clean result. Asking the child itself is the only report of
# what it saw; the self-test's own view of the environment is what was set, not
# what the child read.
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
        $resolvedTempRoot = (@(& pwsh -NoProfile -Command '[System.IO.Path]::GetTempPath()') -join "`n").Trim()
        # The exit status is the case under test, so it is read from a variable
        # that cannot carry a stale value: `$LASTEXITCODE` changes only when a
        # native command reports one, so a read that followed no report would
        # answer with whatever an earlier command left behind. Cleared here, a
        # `$null` afterwards says the child never reported.
        $global:LASTEXITCODE = $null
        $lines = @(& pwsh -NoProfile -File $janitor @JanitorArgs 2>&1)
        return [pscustomobject]@{
            ExitCode         = $LASTEXITCODE
            Output           = ($lines -join "`n")
            ResolvedTempRoot = $resolvedTempRoot
        }
    } finally {
        $env:TMP = $savedTmp
        $env:TMPDIR = $savedTmpDir
        $env:TEMP = $savedTemp
    }
}

# Runs the janitor in a child pwsh whose session shadows `Remove-Item` with a
# function of that name: the janitor resolves the name through its caller's
# scope, so every removal it performs goes through the shadow.
#
# Shadowing is how this twin provokes a removal that fails. A directory the
# current user cannot delete needs an open handle (Windows-only), an ACL, or a
# file mode the janitor's own read-only clearing repairs (Unix), so none of them
# is portable across the hosts this suite runs on. The sh twin covers a real `rm`
# that refuses; this twin covers the janitor's verdict for a removal that failed.
# An empty `-Marker` fails every removal (the refusal preflight); a marker fails
# only the paths containing it (one stuck tree beside a removable one).
function Invoke-JanitorAtRootWithShim {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$Marker
    )

    $wrapper = Join-Path $Root 'shim-janitor.ps1'
    $shim = @"
[CmdletBinding()]
param(
    [Parameter(Mandatory = `$true)][string]`$Janitor,
    [Parameter(Mandatory = `$true)][AllowEmptyString()][string]`$Marker
)
function Remove-Item {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = `$true)][string]`$LiteralPath,
        [switch]`$Recurse,
        [switch]`$Force
    )
    if (`$Marker -eq '' -or `$LiteralPath -like "*`$Marker*") {
        throw "simulated removal failure: `$LiteralPath"
    }
    Microsoft.PowerShell.Management\Remove-Item -LiteralPath `$LiteralPath -Recurse -Force
}
& `$Janitor
exit `$LASTEXITCODE
"@
    Set-Content -LiteralPath $wrapper -Value $shim -Encoding utf8NoBOM

    $savedTmp = $env:TMP
    $savedTmpDir = $env:TMPDIR
    $savedTemp = $env:TEMP
    try {
        $env:TMP = $Root
        $env:TMPDIR = $Root
        $env:TEMP = $Root
        # Cleared for the reason given in Invoke-JanitorAtRoot: the wrapper's own
        # exit status is what these cases assert.
        $global:LASTEXITCODE = $null
        $lines = @(& pwsh -NoProfile -File $wrapper -Janitor $janitor -Marker $Marker 2>&1)
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
# The diagnostic is asserted first because it is the one assertion that names the
# root the child actually used: a child that resolved a different temp root
# sweeps it and reports a clean result, and the message below then shows that
# output instead of only the status the run happened to return.
if (-not $missing.Output.Contains("no such directory: $missingRoot")) {
    Fail "missing-root diagnostic did not name ${missingRoot}: $($missing.Output); the child resolved its temp root as $($missing.ResolvedTempRoot)"
}
if ($null -eq $missing.ExitCode) {
    Fail "the janitor reported no exit status for a missing temp root ($missingRoot): $($missing.Output)"
}
if ($missing.ExitCode -eq 0) {
    Fail "janitor exited 0 for a missing temp root ($missingRoot): $($missing.Output)"
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

# --- Refusal contract: a host whose removal cannot remove a directory it just
# created must fail the janitor before any removal is reported, and leave every
# seeded path where it was.
$refuseRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("mediapm-clean-mediapm-temp-refuse-" + [System.Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path (Join-Path $refuseRoot 'mediapm-artifact-refuse1') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $refuseRoot 'mediapm-cache-refuse2') -Force | Out-Null
try {
    $refusal = Invoke-JanitorAtRootWithShim -Root $refuseRoot -Marker ''
    if ($refusal.ExitCode -eq 0) { Fail "janitor exited 0 when no removal can happen ($refuseRoot)" }
    if ($refusal.Output -like '*removed:*') { Fail "janitor reported a removal it did not perform: $($refusal.Output)" }
    if (-not $refusal.Output.Contains("cannot remove directories under $refuseRoot")) {
        Fail "refusal did not name ${refuseRoot}: $($refusal.Output)"
    }
    foreach ($name in @('mediapm-artifact-refuse1', 'mediapm-cache-refuse2')) {
        if (-not (Test-Path -LiteralPath (Join-Path $refuseRoot $name))) { Fail "a refused sweep removed $name" }
    }
} finally {
    Remove-Item -LiteralPath $refuseRoot -Recurse -Force -ErrorAction SilentlyContinue
}

# --- Per-path failure: the preflight passes, then one seeded directory cannot be
# removed. It must be named on stderr, must still fail the run, and must not stop
# the sweep from removing the rest.
$failRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("mediapm-clean-mediapm-temp-fail-" + [System.Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path (Join-Path $failRoot 'mediapm-artifact-sealed') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $failRoot 'mediapm-cache-ok') -Force | Out-Null
try {
    $sealedPath = Join-Path $failRoot 'mediapm-artifact-sealed'
    $okPath = Join-Path $failRoot 'mediapm-cache-ok'
    $partial = Invoke-JanitorAtRootWithShim -Root $failRoot -Marker 'mediapm-artifact-sealed'
    if ($partial.ExitCode -eq 0) { Fail "janitor exited 0 for a directory it cannot remove ($sealedPath)" }
    if (-not $partial.Output.Contains("failed to remove: $sealedPath")) {
        Fail "per-path failure did not name the sealed dir: $($partial.Output)"
    }
    if (-not $partial.Output.Contains("removed: $okPath")) {
        Fail "a per-path failure stopped the sweep before the removable dir: $($partial.Output)"
    }
    if (-not $partial.Output.Contains('1 director(ies) could not be removed')) {
        Fail "per-path failure missing the summary count: $($partial.Output)"
    }
    if (-not (Test-Path -LiteralPath $sealedPath)) { Fail 'the sealed dir vanished, so the case proves nothing' }
    if (Test-Path -LiteralPath $okPath) { Fail 'the removable dir survived next to a sealed one' }
} finally {
    Remove-Item -LiteralPath $failRoot -Recurse -Force -ErrorAction SilentlyContinue
}

# --- Static part: migration-era workspace globs must be gone.
$janitorText = Get-Content -LiteralPath $janitor -Raw
if ($janitorText -match 'cli-add-hierarchy|examples/artifacts|stale stamped') {
    Fail 'janitor still references migration-era workspace globs (cli-add-hierarchy/examples/artifacts/stale stamped)'
}

Write-Output 'test-clean-mediapm-temp.ps1: OK'
