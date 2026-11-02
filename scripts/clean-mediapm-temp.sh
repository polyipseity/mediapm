#!/usr/bin/env bash
# Remove mediapm-owned temp directories under $TMPDIR.
set -euo pipefail

tmp_root="${TMPDIR:-/tmp}"
dry_run=0
removed=0
failed=0

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

# A sweep must not report what it did not do. Before touching any path, remove a
# directory the sweep itself created, through the same call the sweep uses. A
# host that shims `rm` (a safe-delete wrapper whose trash step is denied, for
# one) fails here instead of printing `removed:` for trees it left in place.
if [[ "$dry_run" -eq 0 ]]; then
    probe_dir="$(mktemp -d)"
    rm -rf "$probe_dir" || true # check-suppress:suppression_doc: the existence check below is the verdict; rm's exit status is one of two ways to fail it.
    if [[ -e "$probe_dir" ]]; then
        printf 'cannot remove directories under %s: the probe %s survived\n' "$tmp_root" "$probe_dir" >&2
        exit 1
    fi
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
        chmod -R u+w "$path" 2>/dev/null || true # check-suppress:suppression_doc: chmod may fail on already-deleted/unreadable trees; the removal check below reports the real failure.
        # A failing `rm` must not abort the sweep through `set -e`: a sweep has
        # to name every path it left in place, so the existence check below is
        # the verdict and `rm`'s exit status is only one of the two ways to
        # fail it.
        rm -rf "$path" || true # check-suppress:suppression_doc: the existence check below is the verdict; rm's exit status is one of two ways to fail it.
        if [[ -e "$path" ]]; then
            printf 'failed to remove: %s\n' "$path" >&2
            failed=$((failed + 1))
            return
        fi
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
# The trap covers every exit after this point. A failing unlink of the listing
# itself must not change the sweep's verdict: the listing is one file under the
# temp root the sweep reports on, and an environment that shims `rm` (some hosts
# ship a safe-delete wrapper that exits non-zero) would otherwise let its
# cleanup decide the exit status. The sweep's own removals are a different
# matter: the preflight above refuses a shimmed `rm` before the sweep starts,
# and remove_if_exists names every path it could not remove.
listing="$(mktemp)"
# check-suppress:suppression_doc: the listing is a single temp file and a failed unlink must not mask the sweep's exit status
trap 'rm -f "$listing" || true' EXIT

if ! find "$tmp_root" -maxdepth 1 -type d -name 'mediapm-*' -print0 >"$listing"; then
    exit 1
fi

while IFS= read -r -d '' dir; do
    remove_if_exists "$dir"
done <"$listing"

# A swept path has exactly two outcomes: removed, or named as failed. So the
# "found nothing" line belongs to a glob that matched nothing; a run that
# matched a path it could not remove prints its count line (`removed 0 ...`)
# and exits 1 through the failure summary below, instead of claiming it found
# no mediapm temp directory while naming one it left in place.
if [[ "$removed" -eq 0 && "$failed" -eq 0 ]]; then
    echo "no mediapm temp directories found"
else
    if [[ "$dry_run" -eq 1 ]]; then
        echo "would remove $removed mediapm temp director(ies)"
    else
        echo "removed $removed mediapm temp director(ies)"
    fi
fi

if [[ "$failed" -gt 0 ]]; then
    printf '%d director(ies) could not be removed\n' "$failed" >&2
    exit 1
fi
