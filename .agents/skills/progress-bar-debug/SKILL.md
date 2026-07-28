---
description: "Use when debugging progress bar state in mediapm by enabling JSONL tick output via MEDIAPM_PROGRESS_DEBUG."
name: "progress-bar-debug"
---

# Progress bar debug with JSONL tick dumps

Emit one JSONL line per tick (~50 ms) with the full state of every bar slot.

## Quick start

```bash
MEDIAPM_PROGRESS_DEBUG=auto cargo run ...
```

Creates `progress-debug-<PID>.jsonl` in CWD. Monitor live:

```bash
tail -f progress-debug-*.jsonl | jq .
```

## Env var values

| Value                 | Behavior                                          |
| --------------------- | ------------------------------------------------- |
| `auto` or empty       | Writes to `progress-debug-<PID>.jsonl` in CWD     |
| `/path/to/file.jsonl` | Writes to the specified path (creates/overwrites) |
| unset                 | No debug output (default)                         |

## JSONL schema

Each line is a JSON object with these fields:

### Top-level

| Field          | Type   | Description                                             |
| -------------- | ------ | ------------------------------------------------------- |
| `type`         | string | `"tick"` (discriminant, future: `"attach"`, `"finish"`) |
| `tick`         | int    | Monotonic tick counter (starts at 0)                    |
| `elapsed_secs` | float  | Seconds since sink creation                             |
| `bars`         | array  | Per-slot bar states (see below)                         |

### Bar slot (`bars[]`)

| Field                | Type          | Description                                       |
| -------------------- | ------------- | ------------------------------------------------- |
| `slot`               | int           | Slot index in renderer grid                       |
| `bound`              | bool          | Whether a bar is attached to this slot            |
| `label`              | string        | Bar label (always present)                        |
| `prefix`             | string        | Bar prefix (always present)                       |
| `position`           | int           | Work completed                                    |
| `total`              | int           | Total work (0 = indeterminate)                    |
| `status`             | string        | e.g. `"Active"`, `"Finished"`, `"Abandoned"`      |
| `elapsed_secs`       | float         | Seconds since handle creation                     |
| `rate_bytes_per_sec` | float         | EMA-smoothed rate (0.0 when inactive)             |
| `eta_secs`           | float or null | Estimated seconds remaining (`null` when unknown) |
| `message`            | string        | Custom message (empty when none)                  |
| `dirty`              | bool          | Whether source was dirty this tick                |

## Usage patterns

**Filter by bar slot:**

```bash
tail -f debug.jsonl | jq 'select(.bars[0].bound) | .bars[0]'
```

**Watch position progression:**

```bash
tail -f debug.jsonl | jq -r '[.tick, .bars[] | select(.bound) | "\(.slot): \(.position)/\(.total)"] | @tsv'
```

**Detect stuck bars (position unchanged for N ticks):**

```bash
tail -f debug.jsonl | jq -r '[.tick, .bars[] | select(.bound) | .position] | @tsv'
```

**Filter by status:**

```bash
jq -c 'select(.bars[] | .status != "Active")' progress-debug-*.jsonl
```

## Notes

- Debug output goes to a **file**, not stderr — it never competes with terminal rendering.
- The ticker thread races with manual `group.tick()` calls, so lines may arrive out of phase with wall-clock ticks.
- Stderr output is intentionally unsupported to avoid interfering with the progress bar renderer.
