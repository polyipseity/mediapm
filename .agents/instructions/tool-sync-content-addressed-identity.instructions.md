---
description: "Use when editing tool identity key schemes in src/mediapm/src/conductor_bridge/sync/mod.rs and documents.rs. Covers the '{name}@{hash}' key format, hash domain, idempotency semantics, and key parsing."
name: "Tool Sync Content-Addressed Identity"
applyTo: "src/mediapm/src/conductor_bridge/sync/mod.rs, src/mediapm/src/conductor_bridge/documents.rs"
---

# Tool sync content-addressed identity

## Purpose

- Provide deterministic, content-addressed tool identity keys so identical payloads produce identical keys (idempotency) and version changes produce new keys (orphaning).

## Key scheme

Format: `"{name}@{blake3(content_map_json)}"`

- `name` is the tool identifier (e.g. `"yt-dlp"`).
- `hash` is the lowercase hex blake3 hash of the content_map JSON serialized with `serde_json::to_string`.
- When content_map is empty (no payload fetched, internal launcher), the bare `"{name}"` is used — no `@` suffix.

## Semantics

- **Idempotent**: same payload content_map → same hash → same key. Re-running sync with identical tool version produces the same key, so the generated document entry is overwritten in-place (no orphaned entries).
- **Orphaned on version change**: new payload → new content_map → new hash → new key. The old `"{name}@{old_hash}"` entry remains in the generated document until the next garbage collection pass.
- **Bare keys for no-payload tools**: tools that don't fetch a payload (no provider sources, or internal launchers) use bare `"{name}"`. These entries are always overwritten in-place.

## Key parsing in `list_tools`

In `list_tools()` (`documents.rs`), keys are parsed by splitting on the last `@`:

- `key.rfind('@')` splits `"{name}@{version}"` into `(name, version)`.
- Bare keys (no `@`) use the entire key as `name` with an empty `version` string.
- This parsing is used for `mediapm tool list` output.

## Hash domain

- The hash covers only the content_map JSON, not the tool binary bytes or any other metadata.
- The content_map is a `BTreeMap<String, String>` — its JSON serialization is deterministic due to BTreeMap's sorted key order.
- CAS hash of the tool binary itself is stored separately in `content_map` values.
