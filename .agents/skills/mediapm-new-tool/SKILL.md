---
description: "Step-by-step guide for adding a new managed tool to the mediapm crate. Covers spec-first, test-first workflow, provider/preset/workstep implementation, and registration in all dispatchers."
name: "mediapm-new-tool"
---

# Adding a new managed tool

Complete workflow for adding a new managed tool to the mediapm workspace.

## 1. Spec first

Document the contract in `src/mediapm/AGENTS.md`: tool identity, version sources (GitHub releases, custom URLs, etc.), companion dependencies (ffmpeg, deno) and their role flags via `dependency_types()` (same-step, cross-step, or both), output variants for media source integration, and any tool-specific configuration keys.

## 2. Write provider tests

Create tests in `src/mediapm/src/tools/provider/<tool_name>.rs` (or add to the existing test module): `resolve_tool_fetch()` returns the expected fetch for sample version selectors, canonical version resolution, source URL construction. Run: `cargo test -p mediapm -- provider::<tool_name>`

## 3. Implement provider

Create `src/mediapm/src/tools/provider/<tool_name>.rs`:

```rust
// Module structure:
// - resolve_tool_fetch(requirement, metadata_cache) -> (ResolvedToolFetch, ResolvedToolMetadata)
//   where ResolvedToolMetadata carries human_readable_version, canonical_version,
//   metadata_cached, metadata_fetch_count, and resolved_tag / resolved_version /
//   resolved_vcs_hash (all Option<String>; None when the provider has no value)
// - resolve_latest_<tool_name>_tag(metadata_cache) -> Option<String>
// - build_<tool_name>_sources(version) -> Vec<ToolSource>
```

Register in `src/mediapm/src/tools/provider/mod.rs`: add `mod <tool_name>;` and a `resolve_tool_fetch()` dispatch arm matching the tool name.

## 4. Write preset tests

Create or extend tests in `src/mediapm/src/tools/preset/<tool_name>.rs` or `src/mediapm/src/tools/workflows/<tool_name>.rs`: `apply_preset()` output for the new tool's preset spec, workflow step synthesis if applicable.

## 5. Implement preset

Create `src/mediapm/src/tools/preset/<tool_name>.rs` or `src/mediapm/src/tools/workflows/<tool_name>.rs`: define the preset spec builder and workflow step synthesis (if the tool participates in a workflow). Register in `src/mediapm/src/tools/preset/mod.rs`: add the module declaration and an `apply_preset()` dispatch arm.

## 6. Integration tests

Write end-to-end tests in `src/mediapm/tests/`: tool requirement parsing from `mediapm.ncl`, full reconcile cycle (mock provider responses), materialization of tool payloads.

## 7. Run full verification

```sh
cargo test -p mediapm
cargo run --package mediapm --example mediapm_demo
```

## Files reference

| Step                    | Files to create/modify                           |
| ----------------------- | ------------------------------------------------ |
| Provider                | `src/mediapm/src/tools/provider/<tool_name>.rs`  |
| Provider dispatch       | `src/mediapm/src/tools/provider/mod.rs`          |
| Preset                  | `src/mediapm/src/tools/preset/<tool_name>.rs`    |
| Preset dispatch         | `src/mediapm/src/tools/preset/mod.rs`            |
| Workflow (if used)      | `src/mediapm/src/tools/workflows/<tool_name>.rs` |
| Tests (provider)        | `src/mediapm/src/tools/provider/<tool_name>.rs`  |
| Tests (preset/workflow) | `src/mediapm/src/tools/preset/<tool_name>.rs`    |
| Tests (integration)     | `src/mediapm/tests/int/<tool_name>.rs`           |
| Spec                    | `src/mediapm/AGENTS.md`                          |
