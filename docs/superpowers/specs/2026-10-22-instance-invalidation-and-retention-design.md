# Instance invalidation and retention

Design for making invalidation reversible and for making conductor GC decide reachability from the instance graph rather than from a clock. Approved in session on 2026-10-22.

## Problem

Two defects share one cause: the decision "must this recorded result be used again" is kept somewhere other than the result.

1. **Invalidation is a one-way destructive action.** `mediapm media invalidate --invalidate-calls` clears `variant_hashes`, writes the unread `steps_completed`, wipes `last_impure_sync_at` for *every* media, and queues the workflow in `MediaPmState::pending_force_rerun`. Nothing can undo it: the state an undo would restore has already been destroyed. At the conductor level, `state invalidate-tool-call <key>` deletes the instance outright, so its outputs and blobs lose their only reference and become unrecoverable before the operator can change their mind.
2. **GC decides reachability from the wrong signal.** `Conductor::run_gc()` passes an empty `referenced_keys` set, and `last_referenced_at` is only written at insertion, so eviction means "seven days since creation". A workspace that hits an instance every day still loses it on day eight, while an instance nothing consumes lingers for the same week. There is no notion of a superseded instance and no way to express "this chain is dead because its consumer moved on".

The re-run intent is also queued in a place the run must remember to consult, which is how the destructive writes and the queue drift apart.

## Principle

**The marker lives on the run result, and the only consumer of a result is the probe.** An instance that must not be reused is an instance whose probe misses. Nothing else has to remember to check anything.

**Reachability is a property of the graph, not of a clock.** An instance is reachable while the current plan's instance set can still reach its bytes; it becomes unreachable when nothing live consumes them, and only then does a grace period begin.

## Design

### 1. Invalidation marker on the instance

`ToolCallInstance` gains `invalidated_at: Option<Timestamp>`.

- `probe_cache` returns `None` for an instance whose marker is set. Every reuse decision already goes through the probe, so the marker cannot be bypassed.
- A re-execution rewrites the record with no marker, so the re-run clears it. Without that, an unchanged-inputs re-execution would land on the same key and miss forever.
- A cache hit does not clear the marker; it cannot hit in the first place.
- `mediapm` no longer stores a queue: `MediaPmState::pending_force_rerun` is removed. `--force-rerun` stays as a run-scoped escape hatch that bypasses the probe for one run and writes nothing.

### 2. Two commands, one marker

- `mediapm-conductor state invalidate-tool-call <key>` sets the marker; `state revalidate-tool-call <key>` clears it. An unknown key is a non-zero exit; re-marking or clearing a marker that is already in the requested state reports it and succeeds.
- `mediapm media invalidate <media-id> <step-index>` resolves that step index to its step ids from the generated document and marks every instance whose `step_ids` contain one of them. `--undo` clears the same markers. `--invalidate-calls` and `--regenerate` stay as accepted aliases; both now mean the same thing because there is no progress left to clear.
- Media step ids carry no media identity and instances are shared by content, so marking one media's step also marks the bytes another media would reuse. That is accepted: instances are shared results, and the alternative (a per-media map of which instance a step last used) goes stale exactly when it matters, so a step the operator asked to re-run would silently not re-run.

### 3. Reachability: seed, closure, grace

- **Seed.** A successful `run_workflow` records the instance keys it produced or hit, per workflow, in `aux.live_instances: BTreeMap<String, BTreeSet<Hash>>`. It is replaced wholesale by each successful run; a failed run leaves the previous seed.
- **Closure.** Edges derive from hashes that already exist: `materialized_inputs` name → hash on the consumer, `outputs[name].hash` on the producer. Index outputs by hash and walk outward from the seed, following every producer of a reached hash. Sharing widens the closure, never narrows it.
- **Grace.** `last_referenced_at` is refreshed only for keys in the closure at each GC pass. A superseded chain stays protected while a live consumer still lists its bytes; once that consumer re-runs onto new inputs the edge disappears and the seven-day clock starts. Blob reclamation follows, because phase 2's root set is the surviving instances.
- **Safety rules.** A workflow with no recorded seed contributes nothing to the closure, so that pass evicts and prunes nothing and logs once (the mistake is "kept too long", never "deleted in use"); a document that declares no workflow at all is treated the same way, because config discovery skips missing files and an empty merge is legal. A key in the closure is never evicted regardless of `invalidated_at`, so an invalidate cannot become a deletion before the re-run happens. A reachable key with no TTL clock gets one rather than being read as "never referenced".
- TTL stays seven days, not configurable.

### 4. Background maintenance gets an owner that can see state

Ownership moves to the `Conductor` façade: it holds the authoritative state (`Mutex<ConductorState>` becomes `Arc<Mutex<…>>`, behind a gate shared with every other whole-state read-modify-write), the CAS, and the storage paths. A tick loads the merged config, runs the full GC cycle, and persists through the façade's state writer, which replaces the file atomically. The guard stays `mediapm_cas::BackgroundMaintenanceGuard`, starts on the first `ensure_actor_client`, runs every 86 400 s, and logs failures without failing anything. The coordinator's `start_background_gc` and its CAS-only sweep are deleted: two sweeps of one store buy nothing and race.

**A tick runs the full cycle, because every live byte is declared or recorded.** Running the full cycle on a timer originally deleted payloads the application had imported into the shared store: no root set computed from the conductor's state and document included them, because they were named by neither. The fix is not to withhold reclamation but to complete the document — `external_data` is the single channel that roots an externally supplied hash, and the invariant now requires every hash a document names to be declared there, whether it appears in a tool `content_map`, in a `${external_data.<hash>}` reference in a step input, a tool default input, or a tool command part, or as a hash-valued step input. The reference scan is the template resolver's own parse, so a reference written with a post-processing selector, and a reference inside either branch of a conditional, count by the hash they read. With that enforced at load, a document the pass can read is a document whose bytes are all named: the application's staged payloads through their declarations, and everything a run produced through its instance records. The pass therefore reclaims bytes again, and an installation that only ever syncs no longer grows without bound.

The window this leaves is an application that stages bytes *before* the document that declares them exists — mediapm imports a payload while provisioning and writes the declaring document at the end of the sync — because a pass in that interval reads the previous document. The cost is a failed sync the next run repairs by re-importing, not a silent loss.

### 5. Version bumps

Both are additive-or-removal changes on strict wire types, so both move one integer.

| State | 3 → 4 | Contents |
|---|---|---|
| conductor | `ToolCallInstance.invalidated_at`, `aux.live_instances` | `versions/v4.rs` with `from_v3_into_v4`; V3 stays readable |
| mediapm | `pending_force_rerun` removed | `versions/v4.rs` with `from_v3_into_v4` dropping the field |

## Testing

- Probe: a marked instance misses; clearing it hits; a re-execution records a fresh instance with no marker; a cache hit never clears.
- Migration: V3 documents decode to V4 with the new fields at their defaults; a V4 document with an unknown field is rejected; round-trip is stable.
- Reachability: a producer stays while a live consumer lists its blob and ages out once the consumer re-runs onto new inputs; a workflow with no seed evicts nothing; a marked instance in the closure is not evicted.
- mediapm: invalidate leaves `variant_hashes` and every `last_impure_sync_at` untouched while marking the instances; invalidate then `--undo` then sync reports every step cached; a failed forced run leaves the marker set.
- CLI: unknown key exits non-zero; re-marking is idempotent.
- Background: the guard runs the full cycle on its interval, keeps a declared payload, reclaims an undeclared orphan, and is cancelled on drop.
- Invariant: a `${external_data.<hash>}` reference or a hash-valued step input with no `external_data` entry is rejected at load, naming the input; declaring it passes.
- Demo: unchanged path stays green.

## Out of scope

`Conductor::run_gc`'s blob pass keeps its current scope; a per-media history of which instance a step used; configurable TTLs; any change to `force_rerun` semantics.
