---
description: "Use when creating commits or editing commit workflow/policy files in this repository."
name: "Commit Message Policy"
applyTo: "**"
---

# Commit Message Policy

- Headers follow Conventional Commits: `type(scope): subject`. Scope is mandatory.
- Do not start headers with crate-prefix forms (`mediapm:`, `conductor:`, `cas:`, or similar `<crate-or-tool>:`). Crate/tool identity belongs in the scope.

Examples:

- ✅ `feat(mediapm): align demo-online sidecar hierarchy`
- ✅ `fix(conductor): reject invalid content_map traversal keys`
- ❌ `mediapm: align demo-online sidecar hierarchy`
- ❌ `conductor: reject invalid content_map traversal keys`
