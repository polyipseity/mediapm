Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$runner = Join-Path $repoRoot 'scripts/run-all-tests.ps1'

function Fail($message) {
    [Console]::Error.WriteLine("test-run-all-tests.ps1: FAIL: $message")
    exit 1
}

# 1. Parse check: the runner must be syntactically valid.
try {
    $null = [scriptblock]::Create((Get-Content -LiteralPath $runner -Raw))
} catch {
    Fail "runner has a syntax error: $($_.Exception.Message)"
}

# 2. --help exits 0 and documents usage (before any cargo invocation).
$helpOut = & pwsh -NoProfile -File $runner --help 2>&1
if ($LASTEXITCODE -ne 0) { Fail '--help should exit 0' }
if (-not ($helpOut -match 'usage: run-all-tests.ps1')) { Fail '--help missing usage line' }

# 3. Unknown arguments exit non-zero with a stderr diagnostic.
$bogusOut = & pwsh -NoProfile -File $runner --bogus 2>&1
if ($LASTEXITCODE -eq 0) { Fail '--bogus should exit non-zero' }
if (-not ($bogusOut -match 'unknown argument')) { Fail '--bogus missing stderr diagnostic' }

# 4. Static gates: the runner must invoke the canonical commands.
$runnerText = Get-Content -LiteralPath $runner -Raw
foreach ($needle in @('cargo --locked nextest run', 'cargo --locked test --doc --workspace', 'clean-mediapm-temp', 'tempfile::tempdir', '.prefix')) {
    if (-not $runnerText.Contains($needle)) { Fail "runner missing static gate: $needle" }
}

# 5. --large must enable the large-tests Cargo feature (not an env var).
if (-not $runnerText.Contains('--features large-tests')) { Fail 'runner missing --features large-tests under --large' }

# 6. Temp-dir gate: a sweep that fails must fail the runner. A stub `cargo`
#    first on PATH lets the real runner reach its gates without running the
#    workspace suite. The stub dir carries the managed `mediapm-` prefix, so
#    a leak is reclaimed by the janitor rather than stranded.
$stubBin = Join-Path ([System.IO.Path]::GetTempPath()) ('mediapm-run-all-tests-stub-' + [System.Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $stubBin | Out-Null
Set-Content -LiteralPath (Join-Path $stubBin 'cargo') -Value "#!/bin/sh`nexit 0`n"
Set-Content -LiteralPath (Join-Path $stubBin 'cargo.cmd') -Value @('echo off', 'exit /b 0')
if ($env:OS -ne 'Windows_NT') { & chmod +x (Join-Path $stubBin 'cargo') }

$savedPath = $env:PATH
$savedTmp = $env:TMP
$savedTmpDir = $env:TMPDIR
$savedTemp = $env:TEMP
function Invoke-RunnerAtRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root
    )

    $env:TMP = $Root
    $env:TMPDIR = $Root
    $env:TEMP = $Root
    $lines = @(& pwsh -NoProfile -File $runner 2>&1)
    return [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        Output   = ($lines -join "`n")
    }
}

Push-Location $repoRoot
try {
    $env:PATH = "$stubBin$([System.IO.Path]::PathSeparator)$savedPath"

    # A missing temp root: the janitor cannot enumerate it and exits
    # non-zero. The runner must fail and name the cause rather than read the
    # empty sweep as clean.
    $missingRoot = Join-Path $stubBin 'missing-root'
    $missing = Invoke-RunnerAtRoot -Root $missingRoot
    if ($missing.ExitCode -eq 0) { Fail "runner passed its temp-dir gate with a missing temp root ($missingRoot)" }
    # .NET's own GetTempPath() diagnostic may precede the cause, so the
    # runner's message and the cause it names are checked separately.
    if (-not $missing.Output.Contains('error: mediapm temp-dir sweep failed:')) {
        Fail "missing-root gate failure missing the sweep message: $($missing.Output)"
    }
    if (-not $missing.Output.Contains("no such directory: $missingRoot")) {
        Fail "missing-root gate failure did not name the cause: $($missing.Output)"
    }

    # The other route stays: a sweep that exits 0 and reports leftovers
    # still fails, with its own message so the two failures read
    # differently.
    $leftoverRoot = Join-Path $stubBin 'leftover-root'
    New-Item -ItemType Directory -Path (Join-Path $leftoverRoot 'mediapm-artifact-fake') -Force | Out-Null
    $leftover = Invoke-RunnerAtRoot -Root $leftoverRoot
    if ($leftover.ExitCode -eq 0) { Fail "runner passed its temp-dir gate with leftover dirs behind ($leftoverRoot)" }
    if (-not $leftover.Output.Contains('error: test suite left mediapm temp dirs behind')) {
        Fail "leftover gate failure missing its message: $($leftover.Output)"
    }

    # A root that exists and holds nothing: the sweep scans it, finds no
    # leftovers, and the runner passes. The two failure routes above cannot
    # tell whether this case still works, so the runner layer asserts it too.
    $emptyRoot = Join-Path $stubBin 'empty-root'
    New-Item -ItemType Directory -Path $emptyRoot -Force | Out-Null
    $empty = Invoke-RunnerAtRoot -Root $emptyRoot
    if ($empty.ExitCode -ne 0) {
        Fail "runner failed its temp-dir gate on an existing empty temp root ($emptyRoot): $($empty.Output)"
    }
} finally {
    Pop-Location
    $env:PATH = $savedPath
    $env:TMP = $savedTmp
    $env:TMPDIR = $savedTmpDir
    $env:TEMP = $savedTemp
    Remove-Item -LiteralPath $stubBin -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Output 'test-run-all-tests.ps1: OK'
