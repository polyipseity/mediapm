#!/bin/sh
# Sandbox self-test for scripts/clean-mediapm-temp.sh.
#
# Two parts:
#   1. Runtime: under a sandboxed $TMPDIR, the janitor removes exactly the
#      three mediapm-* prefixed dirs in dry-run and real-run and never
#      touches non-mediapm control dirs.
#   2. Static: the janitor source must contain no migration-era workspace
#      globs (cli-add-hierarchy / examples/artifacts / stale stamped): the
#      janitor scope is the temp-root three prefixes ONLY.
#   3. Temp root preconditions: a $TMPDIR that does not exist, or that exists
#      but cannot be enumerated, must fail and name that path; an existing
#      empty one must report a clean sweep.
#
# POSIX sh (driven by the `mediapm-tests` crate via `cargo test-all`; also
# runnable standalone).
set -eu

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
janitor="$repo_root/scripts/clean-mediapm-temp.sh"

fail() {
    echo "test-clean-mediapm-temp: FAIL: $1" >&2
    exit 1
}

# --- Runtime part: sandboxed $TMPDIR with fake mediapm-* dirs and controls.
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT INT TERM

mkdir -p "$tmpdir/mediapm-artifact-fake1"
mkdir -p "$tmpdir/mediapm-cache-fake2"
mkdir -p "$tmpdir/mediapm-runtime-abcdef1234567890"
mkdir -p "$tmpdir/cli-add-hierarchy-123-456"
mkdir -p "$tmpdir/unrelated-dir"

# Dry run: reports all three prefixed dirs, never the controls.
dry_out="$(TMPDIR="$tmpdir" "$janitor" --dry-run)"
dry_count="$(printf '%s\n' "$dry_out" | grep -c 'would remove:')" || true
if [ "$dry_count" -ne 3 ]; then
    fail "dry-run reported $dry_count removals, expected 3"
fi
for name in mediapm-artifact-fake1 mediapm-cache-fake2 mediapm-runtime-abcdef1234567890; do
    case "$dry_out" in
        *"would remove: $tmpdir/$name"*) ;;
        *) fail "dry-run missing $name" ;;
    esac
done
case "$dry_out" in
    *"cli-add-hierarchy-123-456"* | *"unrelated-dir"*) fail "dry-run reported a control dir" ;;
    *) ;;
esac

# Real run: removes exactly the three prefixed dirs, leaves controls.
real_out="$(TMPDIR="$tmpdir" "$janitor")"
real_count="$(printf '%s\n' "$real_out" | grep -c 'removed:')" || true
if [ "$real_count" -ne 3 ]; then
    fail "real run reported $real_count removals, expected 3"
fi
test ! -e "$tmpdir/mediapm-artifact-fake1" || fail "artifact dir not removed"
test ! -e "$tmpdir/mediapm-cache-fake2" || fail "cache dir not removed"
test ! -e "$tmpdir/mediapm-runtime-abcdef1234567890" || fail "runtime dir not removed"
test -d "$tmpdir/cli-add-hierarchy-123-456" || fail "control cli-add-hierarchy-* dir removed"
test -d "$tmpdir/unrelated-dir" || fail "control unrelated-dir removed"

# --- Preconditions: a missing temp root must fail, an existing empty one passes.
missing_root="$tmpdir/missing-root"
if TMPDIR="$missing_root" "$janitor" --dry-run >/dev/null 2>"$tmpdir/missing.err"; then
    fail "janitor exited 0 for a missing temp root ($missing_root)"
fi
case "$(cat "$tmpdir/missing.err")" in
    *"no such directory: $missing_root"*) ;;
    *) fail "missing-root diagnostic did not name $missing_root: $(cat "$tmpdir/missing.err")" ;;
esac

empty_root="$tmpdir/empty-root"
mkdir -p "$empty_root"
empty_out="$(TMPDIR="$empty_root" "$janitor" --dry-run)"
case "$empty_out" in
    'no mediapm temp directories found') ;;
    *) fail "empty temp root did not report a clean sweep: $empty_out" ;;
esac

# A temp root that exists but cannot be enumerated is the same hazard as a
# missing one: `find` prints its own diagnostic, its exit status dies inside
# the janitor's process substitution, and the sweep reports a clean run it
# never performed. Skipped as root, where the permission bits are advisory.
if [ "$(id -u)" -ne 0 ]; then
    denied_root="$tmpdir/denied-root"
    mkdir -p "$denied_root"
    chmod 000 "$denied_root"
    if TMPDIR="$denied_root" "$janitor" --dry-run >/dev/null 2>"$tmpdir/denied.err"; then
        fail "janitor exited 0 for a temp root it cannot enumerate ($denied_root)"
    fi
    case "$(cat "$tmpdir/denied.err")" in
        *"no such directory: $denied_root"*) ;;
        *) fail "denied-root diagnostic did not name $denied_root: $(cat "$tmpdir/denied.err")" ;;
    esac
    # Restore the mode so the EXIT trap can reach inside the dir.
    chmod 755 "$denied_root"
fi

# --- Static part: migration-era workspace globs must be gone.
if grep -qE 'cli-add-hierarchy|examples/artifacts|stale stamped' "$janitor"; then
    fail "janitor still references migration-era workspace globs (cli-add-hierarchy/examples/artifacts/stale stamped)"
fi

echo "test-clean-mediapm-temp: OK"
