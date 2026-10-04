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
            echo "removes: mediapm-*"
            echo "         under \$TMPDIR."
            exit 0
            ;;
        *)
            echo "unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

# A temp root that does not exist, or that cannot be enumerated, produces a
# sweep indistinguishable from a clean one. Fail instead.
if [[ ! -d "$tmp_root" || ! -r "$tmp_root" || ! -x "$tmp_root" ]]; then
    echo "no such directory: $tmp_root" >&2
    exit 1
fi

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

# `find` runs into a file rather than a process substitution: a process
# substitution is not a compound command, so its exit status reaches neither
# the loop nor `set -e`, and a `find` that dies part way through reports a
# clean sweep it never performed. The whole root is listed before anything is
# removed, so a failed scan leaves every entry in place instead of sweeping
# the part it happened to see. The listing is named `tmp.*`, which the
# `mediapm-*` glob below cannot match, so the scan never sees its own file.
# The EXIT trap covers every exit after this point, including the one `set -e`
# takes when `rm -rf` refuses a tree part way down.
listing="$(mktemp)"
trap 'rm -f "$listing"' EXIT

if ! find "$tmp_root" -maxdepth 1 -type d -name 'mediapm-*' -print0 >"$listing"; then
    exit 1
fi

while IFS= read -r -d '' dir; do
    remove_if_exists "$dir"
done <"$listing"

if [[ "$removed" -eq 0 ]]; then
    echo "no mediapm temp directories found"
else
    if [[ "$dry_run" -eq 1 ]]; then
        echo "would remove $removed mediapm temp director(ies)"
    else
        echo "removed $removed mediapm temp director(ies)"
    fi
fi
