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

foreach ($arg in $args) {
    switch ($arg) {
        '--dry-run' { $dryRun = $true }
        { $_ -eq '-h' -or $_ -eq '--help' } {
            Write-Output "usage: $PSCommandPath [--dry-run]"
            Write-Output 'removes: mediapm-artifact-* mediapm-cache-* mediapm-runtime-*'
            Write-Output '         under the OS temp dir.'
            exit 0
        }
        default {
            [Console]::Error.WriteLine("unknown argument: $arg")
            exit 1
        }
    }
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
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    if ($dryRun) {
        Write-Output "would remove: $Path"
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
        # After retries the error propagates — fail fast, matching the
        # bash twin's `set -e` abort.
        $attempt = 0
        $lastError = $null
        while ($attempt -lt 6) {
            try {
                Remove-Item -LiteralPath $Path -Recurse -Force -ErrorAction Stop
                $lastError = $null
                break
            } catch {
                $lastError = $_
                Start-Sleep -Milliseconds 40
            }
            $attempt++
        }
        if ($null -ne $lastError) {
            throw $lastError
        }
        Write-Output "removed: $Path"
    }
    $script:removed++
}

# Three temp role prefixes at depth 1 under the OS temp dir.
foreach ($dir in Get-ChildItem -LiteralPath $tempRoot -Directory -Force) {
    if ($dir.Name -like 'mediapm-artifact-*' -or
        $dir.Name -like 'mediapm-cache-*' -or
        $dir.Name -like 'mediapm-runtime-*') {
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
