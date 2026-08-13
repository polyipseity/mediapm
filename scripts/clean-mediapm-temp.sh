#!/usr/bin/env bash
# Remove mediapm-owned temp directories under $TMPDIR and stale stamped
# example-artifact folders in the workspace.
set -euo pipefail

tmp_root="${TMPDIR:-/tmp}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dry_run=0
removed=0

for arg in "$@"; do
    case "$arg" in
        --dry-run) dry_run=1 ;;
        -h | --help)
            echo "usage: $0 [--dry-run]"
            echo "removes: mediapm-artifact-* mediapm-cache-* mediapm-runtime-*"
            echo "         under \$TMPDIR, plus stale cli-add-hierarchy-*"
            echo "         folders under src/mediapm/examples/artifacts/"
            exit 0
            ;;
        *)
            echo "unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

remove_if_exists() {
    local path="$1"
    if [[ ! -e "$path" ]]; then
        return
    fi
    if [[ "$dry_run" -eq 1 ]]; then
        echo "would remove: $path"
    else
        # Clear read-only bits first: stale artifact trees can contain
        # read-only dirs/files (mirrors clear_readonly_bits_recursively in
        # src/mediapm-utils/src/temp.rs), which make rm fail to unlink
        # children. The paths are about to be deleted, so this is safe.
        chmod -R u+w "$path" 2>/dev/null || true
        rm -rf "$path"
        echo "removed: $path"
    fi
    removed=$((removed + 1))
}

while IFS= read -r -d '' dir; do
    remove_if_exists "$dir"
done < <(find "$tmp_root" -maxdepth 1 -type d \( \
    -name 'mediapm-artifact-*' -o \
    -name 'mediapm-cache-*' -o \
    -name 'mediapm-runtime-*' \
    \) -print0 2>/dev/null)

# Stale stamped artifact folders from examples that predate the canonical
# artifact root (git-ignored via src/mediapm/examples/.gitignore). The
# canonical cli-add-hierarchy folder is preserved: the glob requires a
# trailing "-<pid>-<nanos>" stamp, so it never matches the bare name.
while IFS= read -r -d '' dir; do
    remove_if_exists "$dir"
done < <(find "$repo_root/src/mediapm/examples/artifacts" -maxdepth 1 -type d \
    -name 'cli-add-hierarchy-*' -print0 2>/dev/null)

if [[ "$removed" -eq 0 ]]; then
    echo "no mediapm temp directories or stale artifact folders found"
else
    if [[ "$dry_run" -eq 1 ]]; then
        echo "would remove $removed mediapm temp director(ies)/stale folder(s)"
    else
        echo "removed $removed mediapm temp director(ies)/stale folder(s)"
    fi
fi
