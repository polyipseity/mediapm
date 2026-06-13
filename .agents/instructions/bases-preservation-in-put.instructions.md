# InMemoryIndex::put() must preserve bases

The `InMemoryIndex::put()` implementation MUST preserve existing constraint
bases when the incoming entry has `bases: None`. Rationale:

1. `CasStore::put()` (write-through path in `store.rs`) always writes entries
   with `bases: None` — it doesn't know about constraint state.
2. `set_constraint()` may be called before a subsequent `put()` for the same
   hash (e.g., conductor's `resolve_input_binding` → `set_constraint` →
   `persist_resolved_input` → `put`).
3. Without bases-preservation, the `put()` call silently wipes the constraint
   that was just set.

The BG engine WAL consumer also explicitly preserves bases by querying
`index.get()` first, but that's a secondary safeguard — the `Index::put()`
implementation itself must be safe to call with `bases: None` from any caller.

## Test coverage

- `put_preserves_existing_bases` in `index.rs` tests this behavior.
