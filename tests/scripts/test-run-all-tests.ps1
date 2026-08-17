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
if ($runnerText.Contains('MEDIAPM_RUN_LARGE_TESTS')) { Fail 'runner must not reference MEDIAPM_RUN_LARGE_TESTS' }

Write-Output 'test-run-all-tests.ps1: OK'
