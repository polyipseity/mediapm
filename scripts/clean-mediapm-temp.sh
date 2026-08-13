#!/usr/bin/env bash
# Remove mediapm-owned temp directories under $TMPDIR.
set -euo pipefail

tmp_root="${TMPDIR:-/tmp}"
dry_run=0
removed=0

for arg in "$@"; do
    case "$arg" in
        --dry-run) dry_run=1 ;;
        -h | --help)
            echo "usage: $0 [--dry-run]"
            echo "removes: mediapm-artifact-* mediapm-cache-* mediapm-runtime-*"
            echo "         under \$TMPDIR."
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
        chmod -R u+w "$path" 2>/dev/null || true # check-suppress:suppression_doc: chmod may fail on already-deleted/unreadable trees; rm -rf below reports the real failure.
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

if [[ "$removed" -eq 0 ]]; then
    echo "no mediapm temp directories found"
else
    if [[ "$dry_run" -eq 1 ]]; then
        echo "would remove $removed mediapm temp director(ies)"
    else
        echo "removed $removed mediapm temp director(ies)"
    fi
fi
