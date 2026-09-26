#!/usr/bin/env bash
# Runs the test suite N times and reports every distinct test that failed at
# least once. Exists because a single gate run is not a reproducible claim:
# low-rate flakes are known, so "the gate passed" and "the gate failed" are
# both ordinary samples and neither is a verdict.
#
# Usage: scripts/flake-hunt.sh [--runs N] [--test FILTER]
#   --runs N   number of runs (default 10)
#   --test F   nextest filter substring; omit to run the real gate
#              (scripts/run-all-tests.sh, which also runs doctests and the
#              temp-dir janitor gate; `cargo test-all` is NOT equivalent)
#
# Prints one `run <n>: PASS|FAIL` line per run, one `FLAKE: <name>` line per
# distinct failing test, and exits 0 only when every run passed.
set -uo pipefail

runs=10
filter=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --runs)
      runs="$2"
      shift 2
      ;;
    --test)
      filter="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [ -z "${TMPDIR:-}" ]; then
  echo "warning: TMPDIR unset; mediapm temp trees may not be reclaimable" >&2
fi

declare -A failed=()
any_failed=0
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

for i in $(seq 1 "$runs"); do
  if [ -n "$filter" ]; then
    RUSTC_WRAPPER="" cargo nextest run --workspace --all-targets \
      -E "test($filter)" >"$tmp/run-$i.log" 2>&1
  else
    RUSTC_WRAPPER="" scripts/run-all-tests.sh >"$tmp/run-$i.log" 2>&1
  fi
  status=$?
  if [ "$status" -eq 0 ]; then
    echo "run $i: PASS"
  else
    any_failed=1
    echo "run $i: FAIL (exit $status)"
  fi
  # nextest prints each failing test twice (failures section + summary), and
  # the two spellings are identical, so per-run `sort -u` plus the associative
  # array collapse them into one name per test.
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    failed["$name"]=1
  done < <(sed -n 's/^ *FAIL \[[^]]*\] *//p' "$tmp/run-$i.log" | sort -u)
done

echo
if [ "$any_failed" -eq 0 ]; then
  echo "no flakes observed in ${runs} run(s)"
  exit 0
fi

# A run can fail without a parseable FAIL line (a doctest failure, a build
# error, a crash). That must never be reported as "no flakes observed".
if [ "${#failed[@]}" -eq 0 ]; then
  echo "FLAKE: <unattributed failure: no FAIL line parsed from a run log>"
elif [ "${#failed[@]}" -gt 1 ]; then
  echo "${#failed[@]} distinct failing test(s) across ${runs} run(s):"
else
  echo "1 distinct failing test across ${runs} run(s):"
fi
for name in "${!failed[@]}"; do
  echo "FLAKE: $name"
done

# A failing hunt is the case worth reading: keep the logs and say where.
echo "run logs kept in ${tmp}"
trap - EXIT
exit 1
