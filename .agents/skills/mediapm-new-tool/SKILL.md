---
description: "Step-by-step guide for adding a new managed tool to the mediapm crate. Covers spec-first, test-first workflow, provider/preset/workstep implementation, and registration in all dispatchers."
name: "mediapm-new-tool"
---

# Adding a new managed tool

This skill guides you through the complete workflow for adding a new managed
tool to the mediapm workspace.

## 1. Spec first

Document the contract in `src/mediapm/AGENTS.md`:

- Tool identity, version sources (GitHub releases, custom URLs, etc.)
- Companion dependencies (ffmpeg, deno) if any
- Output variants for media source integration
- Any tool-specific configuration keys

## 2. Write provider tests

Create tests in `src/mediapm/src/tools/provider/<tool_name>.rs` or add to the
existing test module:

- Test `resolve_tool_fetch()` returns the expected fetch for sample version
  selectors
- Test canonical version resolution
- Test source URL construction

Run: `cargo test -p mediapm -- provider::<tool_name>`

## 3. Implement provider

Create `src/mediapm/src/tools/provider/<tool_name>.rs`:

```rust
// Module structure:
// - resolve_tool_fetch(requirement, metadata_cache) -> ResolvedToolFetch
// - resolve_latest_<tool_name>_tag(metadata_cache) -> Option<String>
// - build_<tool_name>_sources(version) -> Vec<ToolSource>
```

Register in `src/mediapm/src/tools/provider/mod.rs`:

- Add `mod <tool_name>;`
- Add to `resolve_tool_fetch()` dispatch: match on tool name, call
  `<tool_name>::resolve_tool_fetch()`

## 4. Write preset tests

Create or extend tests in `src/mediapm/src/tools/preset/<tool_name>.rs` or
`src/mediapm/src/tools/workflows/<tool_name>.rs`:

- Test `apply_preset()` output for the new tool's preset spec
- Test workflow step synthesis if applicable

## 5. Implement preset

Create `src/mediapm/src/tools/preset/<tool_name>.rs` or
`src/mediapm/src/tools/workflows/<tool_name>.rs`:

- Define preset spec builder function
- Define workflow step synthesis (if tool participates in a workflow)

Register in `src/mediapm/src/tools/preset/mod.rs`:

- Add module declaration
- Add to `apply_preset()` dispatch

## 6. Integration tests

Write end-to-end tests in `src/mediapm/tests/`:

- Test tool requirement parsing from `mediapm.ncl`
- Test full reconcile cycle (mock provider responses)
- Test materialization of tool payloads

## 7. Run full verification

```sh
cargo test -p mediapm
cargo run --package mediapm --example mediapm_demo
```

## Files reference

| Step | Files to create/modify |
|------|------------------------|
| Provider | `src/mediapm/src/tools/provider/<tool_name>.rs` |
| Provider dispatch | `src/mediapm/src/tools/provider/mod.rs` |
| Preset | `src/mediapm/src/tools/preset/<tool_name>.rs` |
| Preset dispatch | `src/mediapm/src/tools/preset/mod.rs` |
| Workflow (if used) | `src/mediapm/src/tools/workflows/<tool_name>.rs` |
| Tests (provider) | `src/mediapm/src/tools/provider/<tool_name>.rs` |
| Tests (preset/workflow) | `src/mediapm/src/tools/preset/<tool_name>.rs` |
| Tests (integration) | `src/mediapm/tests/int/<tool_name>.rs` |
| Spec | `src/mediapm/AGENTS.md` |
