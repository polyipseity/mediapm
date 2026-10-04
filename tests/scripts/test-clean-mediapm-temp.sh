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
#   4. Scan failure: a `find` that dies part way through must fail the
#      janitor, sweep nothing, and leave no listing file behind.
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

# --- Scan failure: a find that dies part way through must not read as clean.
# `-maxdepth 1` opens the temp root and stats each entry, but never opens a
# depth-1 entry, so no mode, ACL, or owner on one can make the real find fail
# and there is nothing to provoke on the filesystem. The stub is the failure
# instead: a find that enumerated the root and died before reporting the
# sweep incomplete, which is the shape a truncated scan actually takes.
stub_bin="$tmpdir/stub-bin"
stub_find="$tmpdir/stub-find"
mkdir -p "$stub_bin" "$stub_find"

cat >"$stub_find/find" <<'STUB'
#!/bin/sh
# stand in for a find that listed the root and then died
printf '%s\0' "$1/mediapm-artifact-one" "$1/mediapm-artifact-two"
exit 1
STUB
chmod +x "$stub_find/find"

# The second stub reports where mktemp put the listing, so the leak assertion
# names the file the janitor actually created. Guessing where to look would
# not work: GNU mktemp honours TMPDIR and writes into the sweep root, the
# macOS one ignores TMPDIR and writes into the per-user temp dir.
cat >"$stub_bin/mktemp" <<'STUB'
#!/bin/sh
# stand in for mktemp, reporting the path it handed back
PATH="${PATH#*:}"
listing="$(mktemp "$@")" || exit $?
printf '%s\n' "$listing" >"$MEDIAPM_TEST_LISTING_CAPTURE"
printf '%s\n' "$listing"
STUB
chmod +x "$stub_bin/mktemp"

scan_root="$tmpdir/scan-root"
mkdir -p "$scan_root/mediapm-artifact-one" "$scan_root/mediapm-artifact-two"
capture="$tmpdir/listing-path"

if PATH="$stub_find:$stub_bin:$PATH" TMPDIR="$scan_root" MEDIAPM_TEST_LISTING_CAPTURE="$capture" \
    "$janitor" >"$tmpdir/scan.out" 2>"$tmpdir/scan.err"; then
    fail "janitor exited 0 for a find that died part way through ($scan_root)"
fi

# Nothing may be swept from a scan that never finished: a caller reading
# "removed 2" cannot tell the entry after it was never looked at.
case "$(cat "$tmpdir/scan.out")" in
    'no mediapm temp directories found') fail "a failed scan reported a clean sweep" ;;
    'removed '*) fail "a failed scan swept entries and claimed success: $(cat "$tmpdir/scan.out")" ;;
    *) ;;
esac
test -d "$scan_root/mediapm-artifact-one" || fail "a failed scan removed mediapm-artifact-one"
test -d "$scan_root/mediapm-artifact-two" || fail "a failed scan removed mediapm-artifact-two"

# The listing must not outlive the run that created it, and this run exits
# through the find-failure branch.
test -s "$capture" || fail "the stub mktemp recorded no listing path"
if [ -e "$(cat "$capture")" ]; then
    fail "a failed sweep left its listing behind: $(cat "$capture")"
fi

# The other way out of the loop is `rm -rf` refusing a tree, which `set -e`
# turns into an immediate abort with the listing still on disk. Only macOS
# offers a non-privileged way to make rm refuse (a BSD file flag), so this
# case is Darwin-only.
if [ "$(uname -s)" = "Darwin" ] && [ "$(id -u)" -ne 0 ] && command -v chflags >/dev/null 2>&1; then
    stuck_root="$tmpdir/stuck-root"
    mkdir -p "$stuck_root/mediapm-artifact-stuck/inner"
    chflags uchg "$stuck_root/mediapm-artifact-stuck/inner"
    rm -f "$capture"
    # Only the mktemp stub is on PATH here: the real find has to run for the
    # loop to reach the tree rm refuses.
    if PATH="$stub_bin:$PATH" TMPDIR="$stuck_root" MEDIAPM_TEST_LISTING_CAPTURE="$capture" \
        "$janitor" >"$tmpdir/stuck.out" 2>"$tmpdir/stuck.err"; then
        fail "janitor exited 0 for a tree it cannot remove ($stuck_root)"
    fi
    chflags -R nouchg "$stuck_root"
    test -s "$capture" || fail "the stub mktemp recorded no listing path for the aborted sweep"
    if [ -e "$(cat "$capture")" ]; then
        fail "an aborted sweep left its listing behind: $(cat "$capture")"
    fi
fi

# --- Static part: migration-era workspace globs must be gone.
if grep -qE 'cli-add-hierarchy|examples/artifacts|stale stamped' "$janitor"; then
    fail "janitor still references migration-era workspace globs (cli-add-hierarchy/examples/artifacts/stale stamped)"
fi

echo "test-clean-mediapm-temp: OK"
