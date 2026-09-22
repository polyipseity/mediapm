#!/usr/bin/env python3
"""Analyze per-source fetch durations from a tool-sync measurement JSONL.

Reads the ticks.jsonl produced by measure-tool-sync-split.sh and reports:
  - Per-tool per-source wall-clock and byte counts
  - Bandwidth estimate per source
  - Gate check: balanced sources, not bandwidth-bound

Usage:
  python3 analyze-per-source.py <ticks.jsonl> [--tool <name>]

Extracts per-source data from the fetch bar's item counter
({completed}/{total} in the prefix). Sources complete when the
counter increments.
"""
import json
import re
import sys

TOOL_SYNC_RE = re.compile(r'\[(res|fch|pro)\]')


def extract_phase(bar):
    for text in (bar.get("label", ""), bar.get("prefix", "")):
        m = TOOL_SYNC_RE.search(text)
        if m:
            return m.group(1)
    return None


def is_active(bar):
    status = bar.get("status", "")
    if status == "Active":
        return True
    pos = bar.get("position", 0)
    total = bar.get("total", 0)
    return total > 0 and pos < total


def tool_id(bar):
    return bar.get("label", "").split(" ", 1)[0] or ""


def load_ticks(path):
    ticks = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("type") == "tick":
                ticks.append(obj)
    ticks.sort(key=lambda t: t["tick"])
    return ticks


def extract_source_transitions(ticks, target_tool):
    """Track fetch bar item-counter transitions for a tool.

    Returns list of (source_idx, elapsed_secs, cumulative_bytes)
    at each point where the item counter increments (source completion).
    """
    prev_counter = None
    transitions = []
    for t in ticks:
        for b in t.get("bars", []):
            if tool_id(b) == target_tool and extract_phase(b) == "fch":
                prefix = b.get("prefix", "")
                m = re.search(r"\[fch\]\s*(\d+)/(\d+)", prefix)
                if m:
                    counter = m.group(1)
                    pos = b.get("position", 0)
                    if prev_counter is not None and counter != prev_counter:
                        transitions.append((counter, t["elapsed_secs"], pos))
                    prev_counter = counter
    return transitions


def find_tool_fetch_summary(ticks, target_tool):
    """Find the first and last [fch] bar for a tool."""
    first = None
    last_active = None
    last_success = None
    for t in ticks:
        for b in t.get("bars", []):
            if tool_id(b) == target_tool and extract_phase(b) == "fch":
                if first is None:
                    first = {"elapsed": t["elapsed_secs"], "pos": b.get("position", 0)}
                if is_active(b):
                    last_active = {"elapsed": t["elapsed_secs"], "pos": b.get("position", 0)}
                if b.get("status") == "Success":
                    last_success = {"elapsed": t["elapsed_secs"], "pos": b.get("position", 0)}
    return first, last_active or last_success


def analyze_tool(ticks, target_tool):
    """Print per-source analysis for one tool."""
    transitions = extract_source_transitions(ticks, target_tool)
    first, last = find_tool_fetch_summary(ticks, target_tool)

    if not first or not last:
        print(f"  no [fch] data found for {target_tool}")
        return

    total_bytes = last["pos"]
    total_time = last["elapsed"] - first["elapsed"]

    print(f"\n=== {target_tool} ===")
    print(f"  total_fetch: {total_time:.1f}s ({total_bytes} bytes)")

    if not transitions:
        print("  no item-counter transitions found (single-source or indeterminate)")
        return

    # Reconstruct per-source from transitions
    # First source completes at transitions[0], etc.
    sources = []
    prev_elapsed = first["elapsed"]
    prev_bytes = first["pos"]
    for counter, elapsed, cumul_bytes in transitions:
        duration = elapsed - prev_elapsed
        source_bytes = cumul_bytes - prev_bytes
        sources.append({
            "idx": len(sources),
            "duration": duration,
            "bytes": source_bytes,
            "rate_mbs": (source_bytes / 1024 / 1024 / duration) if duration > 0 else 0,
        })
        prev_elapsed = elapsed
        prev_bytes = cumul_bytes

    # Final source (after last transition to Success)
    final_elapsed = last["elapsed"]
    final_bytes = last["pos"]
    if final_elapsed > prev_elapsed:
        sources.append({
            "idx": len(sources),
            "duration": final_elapsed - prev_elapsed,
            "bytes": final_bytes - prev_bytes,
            "rate_mbs": ((final_bytes - prev_bytes) / 1024 / 1024 / (final_elapsed - prev_elapsed)) if (final_elapsed - prev_elapsed) > 0 else 0,
        })

    print(f"  sources: {len(sources)}")
    for src in sources:
        print(f"    source {src['idx']}: {src['duration']:.1f}s "
              f"({src['bytes']} bytes, {src['rate_mbs']:.2f} MB/s)")

    durations = [s["duration"] for s in sources]
    max_dur = max(durations) if durations else 0
    min_dur = min(durations) if durations else 0
    dominant_pct = (max_dur / sum(durations) * 100) if sum(durations) > 0 else 0

    print(f"  dominant source: {dominant_pct:.0f}% of total")
    print(f"  parallel speedup: {sum(durations):.1f}s → {max_dur:.1f}s "
          f"(save {sum(durations) - max_dur:.1f}s)")


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    ticks_path = sys.argv[1]
    target_tool = None
    if "--tool" in sys.argv:
        idx = sys.argv.index("--tool")
        if idx + 1 < len(sys.argv):
            target_tool = sys.argv[idx + 1]

    ticks = load_ticks(ticks_path)
    if not ticks:
        print("error: 0 ticks in JSONL", file=sys.stderr)
        sys.exit(1)

    # Discover tools with active [fch] bars
    tools = set()
    for t in ticks:
        for b in t.get("bars", []):
            if extract_phase(b) == "fch" and is_active(b):
                tools.add(tool_id(b))

    if target_tool:
        if target_tool not in tools:
            print(f"error: tool '{target_tool}' not found in JSONL", file=sys.stderr)
            sys.exit(1)
        tools = {target_tool}

    for tool in sorted(tools):
        analyze_tool(ticks, tool)

    # Bandwidth check: compare all tools' average rates
    print("\n=== bandwidth analysis ===")
    rates = []
    for tool in sorted(tools):
        first, last = find_tool_fetch_summary(ticks, tool)
        if first and last and last["elapsed"] > first["elapsed"]:
            rate = last["pos"] / (last["elapsed"] - first["elapsed"]) / 1024 / 1024
            rates.append((tool, rate))
            print(f"  {tool}: {rate:.2f} MB/s")

    if rates:
        avg_rate = sum(r for _, r in rates) / len(rates)
        min_rate = min(r for _, r in rates)
        print(f"  average: {avg_rate:.2f} MB/s, min: {min_rate:.2f} MB/s")
        if min_rate > 0 and avg_rate / min_rate < 2.0:
            print("  verdict: roughly uniform rates — not bandwidth-bound")
        else:
            print("  verdict: mixed rates — partially bandwidth-bound")


if __name__ == "__main__":
    main()
