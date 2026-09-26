#!/usr/bin/env bash
# Fails when .agents/coverage-matrix.md cites an identifier that appears
# nowhere in the workspace.
#
# Why this exists: the same defect class has now appeared three times in this
# repository -- hand-maintained cross-references whose target no longer exists.
# An earlier revision of this script was far too broad and flagged 40 names,
# most of them legitimate citations of config fields and serde attributes
# (e.g. `skip_serializing_if`, present in 14 source files). It also searched
# only `src/`, so every test living in the `tests/` crate was a guaranteed
# false positive.
#
# The criterion is deliberately narrow: an identifier is reported only when it
# appears NOWHERE in `src/` or `tests/`. That is the real defect signal -- a
# citation pointing at nothing at all. It does not attempt to guess whether a
# citation "should" have been a test, which is not decidable from the matrix.
#
# Two kinds of citation resolve. The first is a name that appears in some file
# body. The second is an example target: the matrix cites examples by their
# target name (`mediapm_cli_add_tools`), which is the basename of
# `src/<crate>/examples/<name>.rs` and need not appear in any file body. Both
# `conductor_runtime_diagnostics` and `mediapm_cli_add_tools` are live examples
# cited only by filename.
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
matrix="${root}/.agents/coverage-matrix.md"

if [ ! -f "$matrix" ]; then
  echo "no coverage matrix at $matrix" >&2
  exit 2
fi

tmp="$(mktemp)"
examples="$(mktemp)"
trap 'rm -f "$tmp" "$examples"' EXIT

# Collect candidate identifiers from the matrix's second column (the test /
# evidence column) only. Candidates are bare snake_case names: no dots (which
# would make them field accesses like `report.pruned_tools`), no leading dot
# (config keys like `.\`), and not serde attribute spellings.
awk -F'|' 'NF >= 3 { print $3 }' "$matrix" \
  | grep -oE '`[a-z_][a-z0-9_]{17,}`' \
  | tr -d '`' \
  | sort -u > "$tmp"

if [ ! -s "$tmp" ]; then
  echo "coverage matrix: no candidate citations found (is the table shape unchanged?)"
  exit 2
fi

# Example target names, cited by basename rather than by any file body.
for example in "$root"/src/*/examples/*.rs; do
  [ -e "$example" ] || continue
  basename "$example" .rs >> "$examples"
done

missing=0
checked=0
while read -r name; do
  [ -n "$name" ] || continue
  checked=$((checked + 1))
  # Present anywhere in the workspace -- as a fn, a field, a serde attribute,
  # a config key. If it exists, the citation resolves to something real and is
  # not this check's business.
  if grep -rq --include='*.rs' -- "${name}" "$root/src" "$root/tests" 2>/dev/null; then
    continue
  fi
  # A citation naming a real example target is legitimate: the example file
  # exists, the matrix points at a live thing.
  if grep -qxF -- "${name}" "$examples"; then
    continue
  fi
  echo "MISSING: $name"
  missing=$((missing + 1))
done < "$tmp"

if [ "$missing" -ne 0 ]; then
  echo "coverage matrix: ${missing} of ${checked} citation(s) resolve to nothing in src/ or tests/"
  exit 1
fi

echo "coverage matrix: all ${checked} citation(s) resolve to something real"
exit 0
