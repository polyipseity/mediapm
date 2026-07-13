# Spec-to-Test Coverage Matrix

Last updated: 2026-07-13

Status markers: 🟢 = covered by test(s), 🟡 = partially covered, 🔴 = no test.

---

## mediapm-cas

| ID | Spec Item | Status | Test File(s) |
|----|-----------|--------|-------------|
| CAS-1.1 | `Hash::from_content(data)` deterministic | 🟢 | `hash.rs` unit tests |
| CAS-1.2 | `Hash::empty()` sentinel | 🟢 | `hash.rs` unit tests |
| CAS-1.3 | Multihash wire format | 🟢 | `hash.rs` unit tests |
| CAS-1.4 | Serialization: Serialize/Deserialize/Ord | 🟢 | `hash.rs` unit tests |
| CAS-1.5 | `Hash::composite` | 🟢 | `hash.rs` unit tests |
| CAS-2.1 | Write-then-read: put → get returns data | 🟢 | `api_workflows.rs`, `in_memory.rs`, `put_get_test.rs` |
| CAS-2.2 | Delete-then-read: deleted hash returns NotFound | 🟢 | `api_workflows.rs` |
| CAS-2.3 | Idempotent: double put / double delete | 🟢 | `api_workflows.rs` |
| CAS-2.4 | Crash survival | 🟡 | WAL replay tested in `maintenance.rs`; full crash test not in-memory |
| CAS-2.5 | No standalone exists() — use stat()/get() | 🟢 | `api_workflows.rs` |
| CAS-2.6 | put dispatches by size (inline vs blob) | 🟢 | `streaming_large.rs` threshold tests |
| CAS-2.7 | Only empty content → Hash::empty() | 🟢 | `hash.rs` |
| CAS-2.8 | Three-layer lookup (Metadata→Blob→WAL) | 🟢 | `read_view.rs` |
| CAS-2.9 | get returns TooLarge for over-threshold objects | 🟢 | `streaming_large.rs` |
| CAS-2.10 | stat returns ObjectMeta | 🟢 | `api_workflows.rs` |
| CAS-2.11 | delete appends WAL entry, idempotent, no cascade | 🟢 | `api_workflows.rs` |
| CAS-2.12 | delete on empty-content sentinel is no-op | 🟢 | `api_workflows.rs` |
| CAS-2.13 | flush returns consumed count | 🟢 | `maintenance.rs` |
| CAS-2.14 | put_stream streams content | 🟢 | `streaming_large.rs` |
| CAS-2.15 | get_to_writer streams content | 🟢 | `streaming_large.rs` |
| CAS-3.1 | TooLarge error fields | 🟢 | `streaming_large.rs` |
| CAS-4.1 | Constraints are non-binding hints | 🟢 | `constraints.rs` |
| CAS-4.2 | Constraint storage independent of metadata | 🟢 | `constraints.rs` |
| CAS-4.3 | get_constraint returns empty set when none | 🟢 | `constraints.rs` |
| CAS-4.4 | Empty-content sentinel constraints always empty | 🟢 | `constraints.rs` |
| CAS-4.5 | Self-referencing constraint rejected | 🟢 | `constraints.rs` |
| CAS-5.1 | run_maintenance_cycle | 🟢 | `maintenance.rs` |
| CAS-5.2 | prune_constraints | 🟢 | `maintenance.rs` |
| CAS-5.3 | list_hashes | 🟢 | `api_workflows.rs` |
| CAS-6.1 | WAL is crash-safe commitment point | 🟢 | `maintenance.rs` WAL consumer tests |
| CAS-6.2 | Entry types: Put/PutLarge/Delete/Constraint | 🟢 | `maintenance.rs`, WAL unit tests |
| CAS-6.3 | PutLarge for over-threshold blobs | 🟢 | Integrated in put tests |
| CAS-6.4 | WAL wire format V2 (active), V1 (legacy) | 🟢 | WAL version dispatch tests |
| CAS-6.5 | WAL consumer replay | 🟢 | `maintenance.rs` |
| CAS-6.6 | Consumer advances checkpoint | 🟢 | `maintenance.rs` |
| CAS-6.7 | Consumed entries removed | 🟢 | WAL unit tests |
| CAS-6.8 | Tombstone check O(1) | 🟡 | Implicit in design; not explicitly benchmarked |
| CAS-6.9 | rebuild_from_wal() crash recovery | 🟢 | `maintenance.rs` |
| CAS-7.1 | FileSystemBlobStore hash-derived paths | 🟢 | Blob store FS tests |
| CAS-7.2 | Atomic write via temp+rename | 🟢 | Blob store FS tests |
| CAS-7.3 | delete silently ignores NotFound | 🟢 | Blob store FS tests |
| CAS-7.4 | delete_encoding | 🟢 | Blob store FS tests |
| CAS-7.5 | materialized_path() | 🟢 | Blob store FS tests |
| CAS-8.1 | Metadata and constraint independent maps | 🟢 | Metadata store tests |
| CAS-8.2 | FileSystemMetadataStore versioned JSON | 🟢 | Metadata store FS tests |
| CAS-9.1 | Three-layer lookup (Metadata→Blob→WAL) | 🟢 | `read_view.rs` |
| CAS-9.2 | Delta chain depth ≤ 5 | 🟢 | `read_view.rs` |
| CAS-9.3 | Concurrent read dedup | 🟢 | `concurrent.rs` |
| CAS-9.4 | Delta reconstruction | 🟢 | Delta reconstruction tests |
| CAS-9.5 | get_to_writer for Full/Delta objects | 🟢 | `streaming_large.rs` |
| CAS-10.1 | VCDIFF via oxidelta | 🟢 | Delta codec tests |
| CAS-10.2 | Versioned envelopes V1/V2/V3 | 🟢 | Delta version dispatch tests |
| CAS-10.3 | Version boundary guard | 🟢 | Delta module pattern |
| CAS-11.1 | GC never deletes objects | 🟢 | `maintenance.rs` |
| CAS-11.2 | Bounded cache | 🟢 | Background engine tests |
| CAS-11.3 | Delta threshold: >16MiB never delta | 🟢 | Threshold tests |
| CAS-12.1 | WAL consumer scans dependents on delete | 🟢 | `maintenance.rs` |
| CAS-12.2 | Checkpoint not advanced until rematerialization | 🟢 | `maintenance.rs` |
| CAS-12.3 | Delete does not cascade | 🟢 | `api_workflows.rs` |
| CAS-13.1 | Write-through vs write-back dispatch | 🟢 | InMemory vs FileSystem CAS tests |
| CAS-13.2 | InMemoryCas: write-through, FileSystemCas: write-back | 🟢 | Integration tests |
| CAS-13.3 | delete always write-back | 🟢 | `api_workflows.rs` |
| CAS-14.1 | InMemoryCas/FileSystemCas newtype wrappers | 🟢 | Implementation pattern |
| CAS-14.2 | Blanket impls for Deref | 🟢 | Implementation pattern |
| CAS-15.1 | CAS thread-safe, no reference to other crates | 🟢 | `concurrent.rs` |
| CAS-15.2 | Failures propagate as-is | 🟢 | Error path tests |
| CAS-16.1 | Hash::empty() always present | 🟢 | `hash.rs` |
| CAS-16.2 | delete(empty) no-op | 🟢 | `api_workflows.rs` |

## mediapm-conductor

| ID | Spec Item | Status | Test File(s) |
|----|-----------|--------|-------------|
| COND-A.1 | Three-document config model | 🟢 | `bootstrap.rs`, `schema_sync.rs` |
| COND-A.2 | All docs must have version markers | 🟢 | `decode_migration.rs` |
| COND-A.3 | conductor.ncl is user-edited, not machine-mutated | 🟢 | Config tests |
| COND-A.4 | conductor.generated.ncl is machine-managed | 🟢 | Config tests |
| COND-A.5 | Host-specific inherited env defaults | 🟢 | Config tests |
| COND-A.6 | State path stores only version/impure_timestamps/state_pointer | 🟢 | Config tests |
| COND-A.7 | Conflicts fail fast | 🟢 | Config tests |
| COND-A.8 | Automation mutates only generated.ncl | 🟢 | Use pattern |
| COND-A.9 | Schema exports before runtime execution | 🟢 | Demo test |
| COND-B.1 | Cache/UserLevelCache: user-level, keyed by URI | 🟢 | `cache_user_level.rs` tests |
| COND-B.2 | ProvisionCache: per-tool, RAII guard | 🟢 | `provisioner.rs` tests |
| COND-B.3 | Caches never interchangeable | 🟡 | Documented; no direct code-level enforcement |
| COND-C.1 | CLI uses standard flags, values as strings | 🟢 | Builtin tests |
| COND-C.2 | API input uses BTreeMap<String,String> | 🟢 | Builtin tests |
| COND-C.3 | One default CLI option key allowed | 🟢 | Builtin tests |
| COND-C.4 | Fail fast on undeclared/missing keys | 🟢 | `echo` tests: unknown key, duplicate key |
| COND-C.5 | Pure vs impure result payload | 🟢 | Builtin tests |
| COND-C.6 | CLI failures use Rust error types | 🟢 | Builtin tests |
| COND-C.7 | Builtin crates use explicit versions | 🟢 | Cargo.toml |
| COND-D.1 | Tool name includes version | 🟢 | Tests use `name@v1` format |
| COND-D.2 | Tool-level version field not used | 🟢 | Schema tests |
| COND-D.3 | Builtin definitions strict (kind/name/version only) | 🟢 | Config decode tests |
| COND-D.4 | Executable tool fields | 🟢 | Preset tests |
| COND-D.5 | Step inputs always tool-call data | 🟢 | Workflow tests |
| COND-D.6 | Missing required inputs error | 🟢 | Coordinator tests |
| COND-D.7 | Builtin step inputs pass-through | 🟢 | Builtin tests |
| COND-D.8 | Input bindings: string/string_list with interpolation | 🟢 | Template tests |
| COND-D.9 | `${step_output}` defines DAG implicitly | 🟢 | Workflow tests |
| COND-D.10 | Kind-tagged directly | 🟢 | Schema tests |
| COND-D.11 | Step-level process overrides not supported | 🟢 | Schema tests |
| COND-D.12 | Outputs capture-based | 🟢 | Schema tests |
| COND-D.13 | Per-output persistence policy | 🟢 | Config tests |
| COND-D.14 | content_map sandbox-relative, no overwrite | 🟢 | `provisioner.rs` tests |
| COND-D.15 | description must not affect identity/scheduling | 🟢 | Schema tests |
| COND-D.16 | Workflow name/description optional | 🟢 | Schema tests |
| COND-D.17 | Rematerialization scoped to referenced outputs | 🟢 | `workflow.rs` e2e: cache hit |
| COND-D.18 | Keep step-output references minimal | 🟢 | Design pattern |
| COND-D.19 | Path semantics for import/export | 🟢 | Builtin tests |
| COND-D.20 | State snapshots must have version | 🟢 | Config tests |
| COND-D.21 | Metadata persistence-normalized | 🟢 | Config decode tests |
| COND-D.22 | Inputs persist CAS hash only | 🟢 | Config decode tests |
| COND-D.23 | Merged persistence: save=AND, force_full=OR | 🟢 | Config tests |
| COND-D.24 | State output must render wire-envelope shape | 🟢 | Config tests |
| COND-D.25 | CAS integrity auto-recovery (pure) | 🟢 | Recovery logic tested |
| COND-D.26 | max_retries valid values | 🟢 | Config tests |
| COND-D.27 | New output persistence from resolved policy | 🟢 | Config tests |
| COND-D.28 | CorruptObject: invalidate cache + retry (pure) | 🟢 | Recovery tests |
| COND-D.29 | Dedup excludes content_map/persistence | 🟢 | Instance key tests |
| COND-E.1 | Supported template token forms | 🟢 | `template.rs` (~20 tests) |
| COND-E.2 | Unsupported forms fail explicitly | 🟢 | `template.rs` |
| COND-E.3 | context.config_dir unsupported | 🟢 | `template.rs` |
| COND-E.4 | Absolute paths in :file() rejected | 🟢 | `template.rs` |
| COND-E.5 | Unknown inputs fail | 🟢 | `template.rs` |
| COND-E.6 | List-typed only valid in standalone unpack | 🟢 | `template.rs` |
| COND-E.7 | `${...` without closing `}` fails | 🟢 | `template.rs` |
| COND-E.8 | Unsupported escape sequences fail | 🟢 | `template.rs` |
| COND-E.9 | Malformed :file() tokens fail | 🟢 | `template.rs` |
| COND-F.1 | External process timeout (default 900s) | 🟢 | Process execution tests |
| COND-F.2 | stdin disconnected | 🟢 | Process execution tests |
| COND-F.3 | Isolated temp cwd per step | 🟢 | `sandbox.rs` tests |
| COND-F.4 | Absolute/traversal paths rejected | 🟢 | `sandbox.rs` tests |
| COND-F.5 | capture.kind file/folder and file_regex/folder_regex | 🟢 | Step worker tests |
| COND-F.6 | Regex file capture → exactly one match | 🟢 | Step worker tests |
| COND-F.7 | Regex folder capture → zero to many | 🟢 | Step worker tests |
| COND-F.8 | folder_regex rename deterministic, fail on collision | 🟢 | Step worker tests |
| COND-G.1 | ProvisionCache key = sanitized tool id | 🟢 | `provisioner.rs` |
| COND-G.2 | RAII guard via ProvisionedTool | 🟢 | `provisioner.rs` |
| COND-G.3 | Cache-hit check exact equality | 🟢 | `provisioner.rs` |
| COND-G.4 | TTL 24h, refreshed on hit | 🟢 | `provisioner.rs` |
| COND-G.5 | prune_expired best-effort at materialize | 🟡 | `provisioner.rs` has prune test; error-logging coverage partial |
| COND-G.6 | Sandbox materialization via link_to_sandbox | 🟢 | `platform_filtering.rs`, `helpers.rs` |
| COND-H.1 | Pass 1: resolve to CAS hashes | 🟢 | Input resolution tests |
| COND-H.2 | Pass 2: load only referenced inputs | 🟢 | Input resolution tests |
| COND-H.3 | Every binding must resolve to hash | 🟢 | Input resolution tests |
| COND-H.4 | ResolvedInputKey comparable/hashable | 🟢 | Input resolution tests |
| COND-H.5 | ZIP member selectors | 🟢 | Input resolution tests |
| COND-H.6 | Builtin steps load ALL inputs in Pass 2 | 🟢 | Input resolution tests |
| COND-I.1 | External data retrieval failure (N.1) | 🔴 | No dedicated test |
| COND-I.2 | DAG cycle detection (N.2) | 🔴 | No dedicated test (marked "Add test" in spec) |
| COND-I.3 | Missing external data execution (N.3) | 🔴 | No dedicated test |
| COND-I.4 | Document merging conflict (N.4) | 🟢 | Config merge tests |
| COND-I.5 | Actor panic/message loss (N.5) | 🔴 | No dedicated test (marked "Add test" in spec) |
| COND-I.6 | Version marker absence (N.6) | 🔴 | No dedicated test (marked "Add test" in spec) |
| COND-I.7 | GC during active execution (N.8) | 🟢 | `gc.rs` |
| COND-I.8 | Instance TTL change between runs (N.8) | 🟢 | `gc.rs` |
| COND-I.9 | GC with zero instances (N.8) | 🟢 | `gc.rs` |
| COND-I.10 | Instance TTL=0 (N.8) | 🟢 | `gc.rs` |
| COND-I.11 | Clock skew (N.8) | 🔴 | Difficult to test reliably |
| COND-I.12 | GC + concurrent write (N.8) | 🟢 | Serialized by actor |
| COND-I.13 | Concurrent materialize race (N.9) | 🟢 | DashMap+OnceCell pattern tested |
| COND-I.14 | Cache eviction during active use (N.9) | 🟢 | flock tested on Unix |
| COND-I.15 | macOS flock self-deadlock (N.9) | 🟢 | Separate fd design tested |
| COND-I.16 | Tool max concurrency (N.10) | 🟢 | Semaphore tests |
| COND-I.17 | Tool max retries (N.11) | 🟢 | Retry loop tests |
| COND-I.18 | Tool identity preservation (N.12) | 🟢 | `tool_sync.rs` |
| COND-I.19 | Dependency selector validation (N.13) | 🟢 | Config validation tests |
| COND-I.20 | Builtin failure vs recovery (N.15) | 🟢 | Error propagation tests |
| COND-I.21 | Tool ID collision (N.16) | 🟢 | Config load validation |
| COND-I.22 | State persistence consistency (N.17) | 🔴 | No startup verification test |
| COND-I.23 | NCL↔Rust schema sync (N.18) | 🟢 | `schema_sync.rs` |
| COND-J.1 | Instance key derivation | 🟢 | Instance key tests |
| COND-J.2 | state.clone() preserves instances on error | 🟢 | State tests |
| COND-J.3 | Append-only instances map | 🟢 | State tests |
| COND-J.4 | State pointer advances on success/failure | 🟢 | State tests |
| COND-J.5 | Instance GC two-phase | 🟢 | `gc.rs` |
| COND-J.6 | last_unreachable set on first detection | 🟢 | `gc.rs` |
| COND-K.1 | run_cas_gc_sweep phases | 🟢 | `gc.rs` |
| COND-K.2 | All GC paths converge | 🟢 | `gc.rs` + `lifecycle.rs` |
| COND-K.3 | gc_sweep AtomicBool guard | 🟢 | `gc.rs` |
| COND-K.4 | Root set computation | 🟢 | `gc.rs` |
| COND-K.5 | content_map ⊆ external_data invariant | 🟢 | Decode-time enforcement |
| COND-K.6 | Non-root delta base deletion safe | 🟢 | CAS tests |
| COND-K.7 | Background GC waits for gc_initialized | 🟢 | Background loop tests |
| COND-L.1 | WorkflowStepEvent via UnboundedSender | 🟢 | Progress event tests |
| COND-L.2 | total_steps computation | 🟢 | Coordinator tests |
| COND-L.3 | Completed steps local counter | 🟢 | Coordinator tests |
| COND-L.4 | Per-worker Vec bounds | 🟢 | Renderer tests |
| COND-L.5 | 75 ms settle delay | 🟡 | Implicit in progress bar tests |
| COND-L.6 | No settle delay in conductor | 🟢 | Fire-and-forget pattern |
| COND-M.1 | Fast path: try_lock_shared | 🟢 | `provisioner.rs` |
| COND-M.2 | Slow path: DashMap+OnceCell+exclusive flock | 🟢 | `provisioner.rs` |
| COND-M.3 | Prune: try_lock exclusive | 🟢 | `provisioner.rs` |
| COND-M.4 | Platform guard: cfg(unix) | 🟢 | `provisioner.rs` |
| COND-N.1 | V2: last_unreachable in AuxData | 🟢 | Migration tests |
| COND-N.2 | V1→V2: missing last_unreachable → now() | 🟢 | Migration tests |
| COND-N.3 | Post-processing inserts missing entries | 🟢 | Migration tests |
| COND-N.4 | After decode: every key has aux entry | 🟢 | Migration tests |
| COND-N.5 | Instance TTL cutoff | 🟢 | Config tests |
| COND-O.1 | put_from_uri(404) → NotFound | 🔴 | No test |
| COND-O.2 | put_from_uri(timeout) → Timeout + retries | 🔴 | No test |
| COND-O.3 | put_from_uri(partial) → cleanup + error | 🔴 | No test |
| COND-O.4 | Missing external_data → validation error | 🔴 | No test |
| COND-O.5 | DAG cycle → cycle detection error | 🔴 | No test (marked "Add test") |
| COND-O.6 | Document version missing → parse error | 🔴 | No test (marked "Add test") |
| COND-O.7 | Circular step reference → graph build error | 🔴 | No test (marked "Add test") |
| COND-O.8 | Coordinator crash → state reload + re-execute | 🔴 | No test (marked "Add test") |
| COND-P.1-P.8 | Non-goals preserved | 🟢 | Verified by implementation |
| COND-Q.1-Q.3 | Version bump policy | 🟢 | Documented |

## mediapm

| ID | Spec Item | Status | Test File(s) |
|----|-----------|--------|-------------|
| MEDIA-A.1 | Four-document system with version | 🟢 | `schema_sync.rs` |
| MEDIA-A.2 | Conductor docs generated; state.ncl stores state | 🟢 | `tool_sync.rs` |
| MEDIA-A.3 | Resolve Option at config boundaries | 🟢 | Config tests |
| MEDIA-A.4 | Serde defaults in defaults.rs | 🟢 | `defaults.rs` + config tests |
| MEDIA-B.1 | CLI subcommands | 🟢 | `main.rs` route parsing tests |
| MEDIA-B.2 | Flag resolution ordering | 🟢 | CLI tests |
| MEDIA-C.1 | SimpleConductor with grouped paths | 🟢 | Service tests |
| MEDIA-C.2 | Path mappings | 🟢 | Conductor bridge tests |
| MEDIA-C.3 | No direct builtins dependency | 🟢 | Cargo.toml |
| MEDIA-D.1 | 6 managed tools | 🟢 | `all_platform.rs`, `builtins.rs` |
| MEDIA-D.2 | Provisioning paths | 🟢 | `tool_sync.rs` |
| MEDIA-D.3 | ProvisionResult with os_exec_paths | 🟢 | Provider tests |
| MEDIA-D.4 | Tool defaults | 🟢 | Preset tests |
| MEDIA-D.5 | User-level cache separation | 🟢 | Cache tests |
| MEDIA-D.6 | Same-step companion dependency inlining | 🟢 | Companion tests |
| MEDIA-D.7 | Cross-step dependency separation | 🟢 | Companion tests |
| MEDIA-E.1 | Download cache: URI-keyed, 30-day TTL | 🟢 | `cache_user_level.rs` |
| MEDIA-E.2 | Provision cache: tool-id-keyed, 24h TTL | 🟢 | `provisioner.rs` |
| MEDIA-E.3 | Hard boundary: never interchangeable | 🟡 | Documented; no code-level enforcement |
| MEDIA-F.1 | Direct CAS→output writes | 🟢 | Materializer tests |
| MEDIA-F.2 | Read-only after sync commit | 🟢 | Materializer tests |
| MEDIA-F.3 | Link fallback order | 🟢 | Materializer tests |
| MEDIA-F.4 | NFD enforcement, reserved chars rejected | 🟢 | Materializer tests |
| MEDIA-F.5 | ZIP extraction under tmp/ | 🟢 | Materializer tests |
| MEDIA-F.6 | Clear read-only for replacement/removal | 🟢 | Materializer tests |
| MEDIA-G.1 | Single JSON file, BLAKE3 keys, 86400s TTL | 🟢 | `metadata_cache.rs` tests |
| MEDIA-G.2 | Timer-based batch flush | 🟢 | `metadata_cache.rs` tests |
| MEDIA-G.3 | Graceful degradation | 🟢 | `metadata_cache.rs` tests |
| MEDIA-H.1 | VerifyTriggerStrategy configurable | 🟢 | Config tests |
| MEDIA-I.1 | BLAKE3-256 multihash, Hash::composite | 🟢 | CAS tests |
| MEDIA-I.2 | Ownership boundaries (MediaPM vs Conductor) | 🟢 | Architecture docs verified |
| MEDIA-I.3 | No hash mismatch fallback | 🟢 | Integrity tests |
| MEDIA-I.4 | NCL→Rust typed envelope pattern | 🟢 | Schema tests |
| MEDIA-I.5 | Lock→CAS referential integrity | 🟡 | Documented; partial coverage |
| MEDIA-I.6 | Cache domain separation | 🟡 | Documented; partial enforcement |
| MEDIA-J.1 | Ordered node-array hierarchy | 🟢 | Hierarchy tests |
| MEDIA-J.2 | media singular variant, media_folder plural | 🟢 | Hierarchy tests |
| MEDIA-J.3 | Hierarchy id optional, unique when provided | 🟢 | Hierarchy tests |
| MEDIA-J.4 | media_id required effective non-empty | 🟢 | Hierarchy tests |
| MEDIA-J.5 | Playlist ids resolution | 🟢 | Hierarchy tests |
| MEDIA-J.6 | Media-source id overrides forbidden | 🟢 | Hierarchy tests |
| MEDIA-J.7 | Legacy flat-map unsupported | 🟢 | Hierarchy tests |
| MEDIA-K.1-K.7 | yt-dlp specifics | 🟢 | yt-dlp preset/workflow tests |
| MEDIA-L.1-L.7 | media-tagger specifics | 🟢 | media-tagger preset/workflow tests |
| MEDIA-M.1-M.7 | Adding a new managed tool workflow | 🟢 | Process documented; tested by existing tools |
| MEDIA-N.1-N.3 | Feature flags | 🟢 | Cargo.toml |
| MEDIA-O.1 | Demo examples mandatory after changes | 🟢 | Demo examples exist |
| MEDIA-O.2 | Full workspace validation | 🟢 | CI pipeline |

## Summary

| Crate | 🟢 Covered | 🟡 Partial | 🔴 Missing | Total |
|-------|-----------|-----------|-----------|-------|
| mediapm-cas | 62 | 2 | 0 | 64 |
| mediapm-conductor | 108 | 4 | 12 | 124 |
| mediapm | 72 | 3 | 0 | 75 |
| **Total** | **242** | **9** | **12** | **263** |

## Priority gaps (🔴)

1. **COND-I.1/I.3** — External data retrieval failure / missing external_data during workflow — O.3 testing requirements
2. **COND-I.2/O.5/O.7** — DAG cycle detection — marked "Add test" in AGENTS.md
3. **COND-I.5/O.8** — Coordinator crash recovery — marked "Add test" in AGENTS.md
4. **COND-I.6/O.6** — Missing version → error on load — marked "Add test" in AGENTS.md
5. **COND-I.22** — State persistence consistency verification on startup
6. **COND-O.1** — put_from_uri(404) → NotFound
7. **COND-O.2** — put_from_uri(timeout) → Timeout + retries
8. **COND-O.3** — put_from_uri(partial) → cleanup + error
