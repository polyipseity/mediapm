#!/usr/bin/env pwsh
# Remove mediapm-owned temp directories under the OS temp dir.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$tempRoot = ([System.IO.Path]::GetTempPath()).TrimEnd(
    [System.IO.Path]::DirectorySeparatorChar,
    [System.IO.Path]::AltDirectorySeparatorChar
)
$dryRun = $false
$removed = 0
$failed = 0

foreach ($arg in $args) {
    switch ($arg) {
        '--dry-run' { $dryRun = $true }
        { $_ -eq '-h' -or $_ -eq '--help' } {
            Write-Output "usage: $PSCommandPath [--dry-run]"
            Write-Output 'removes: mediapm-*'
            Write-Output '         under the OS temp dir.'
            exit 0
        }
        default {
            [Console]::Error.WriteLine("unknown argument: $arg")
            exit 1
        }
    }
}

# A temp root that does not exist cannot be enumerated, and an unscanned sweep
# prints the same "nothing found" line a real clean sweep prints. Fail instead
# of letting a caller read the silent case as a clean one. GetTempPath() honors
# TMPDIR/TMP/TEMP, so the root can go missing on this platform too.
if (-not (Test-Path -LiteralPath $tempRoot -PathType Container)) {
    [Console]::Error.WriteLine("no such directory: $tempRoot")
    exit 1
}

function Clear-ReadOnlyAttributes {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $stack = [System.Collections.Generic.Stack[string]]::new()
    $stack.Push($Path)
    while ($stack.Count -gt 0) {
        $current = $stack.Pop()
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if ($item.Attributes -band [System.IO.FileAttributes]::ReadOnly) {
                $item.Attributes = $item.Attributes -band -bnot [System.IO.FileAttributes]::ReadOnly
            }
            if ($item.PSIsContainer) {
                foreach ($child in Get-ChildItem -LiteralPath $current -Force) {
                    $stack.Push($child.FullName)
                }
            }
        }
    }
}

function Remove-JunkDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        # The preflight removes its probe through this same function, but a
        # probe is not a swept directory: it must add no `removed:` line and no
        # count to the summary a caller reads.
        [switch]$Quiet
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    if ($dryRun) {
        if (-not $Quiet) {
            Write-Output "would remove: $Path"
        }
    } else {
        # Clear read-only attributes first: stale artifact trees can contain
        # read-only dirs/files (mirrors clear_readonly_bits_recursively in
        # src/mediapm-utils/src/temp.rs), which make Remove-Item fail to
        # unlink children. The paths are about to be deleted, so this is safe.
        Clear-ReadOnlyAttributes -Path $Path
        # Retry loop mirrors remove_dir_all_with_retry in
        # src/mediapm-utils/src/temp.rs (6 attempts, 40 ms backoff): Windows
        # Remove-Item -Recurse -Force can transiently fail with
        # ERROR_SHARING_VIOLATION while a lingering process holds a handle.
        # A path that survives the retries is reported and the sweep moves on —
        # one stuck tree must not hide the verdict for the others.
        $attempt = 0
        while ($attempt -lt 6) {
            try {
                Remove-Item -LiteralPath $Path -Recurse -Force -ErrorAction Stop
                break
            } catch {
                Start-Sleep -Milliseconds 40
            }
            $attempt++
        }
        if (Test-Path -LiteralPath $Path) {
            if (-not $Quiet) {
                [Console]::Error.WriteLine("failed to remove: $Path")
                $script:failed++
            }
            return
        }
        if (-not $Quiet) {
            Write-Output "removed: $Path"
        }
    }
    if (-not $Quiet) {
        $script:removed++
    }
}

# A sweep must not report what it did not do. Before touching any path, remove a
# directory the sweep itself created, through the same call the sweep uses. A
# host that shims removal (a safe-delete wrapper whose trash step is denied, for
# one) fails here instead of printing `removed:` for trees it left in place.
if (-not $dryRun) {
    $probeDir = Join-Path $tempRoot ('mediapm-probe-' + [System.Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $probeDir | Out-Null
    Remove-JunkDirectory -Path $probeDir -Quiet
    if (Test-Path -LiteralPath $probeDir) {
        [Console]::Error.WriteLine("cannot remove directories under ${tempRoot}: the probe ${probeDir} survived")
        exit 1
    }
}

# Single mediapm-* glob at depth 1 under the OS temp dir.
foreach ($dir in Get-ChildItem -LiteralPath $tempRoot -Directory -Force) {
    if ($dir.Name -like 'mediapm-*') {
        Remove-JunkDirectory -Path $dir.FullName
    }
}

if ($removed -eq 0) {
    Write-Output 'no mediapm temp directories found'
} else {
    if ($dryRun) {
        Write-Output "would remove $removed mediapm temp director(ies)"
    } else {
        Write-Output "removed $removed mediapm temp director(ies)"
    }
}

if ($failed -gt 0) {
    [Console]::Error.WriteLine("$failed director(ies) could not be removed")
    exit 1
}
