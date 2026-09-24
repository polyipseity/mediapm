//! Integration tests for rendered progress output.
//!
//! The target is wired through `tests/progress_output.rs`, which maps this file
//! as the test crate's root module. `common` owns the harness; each remaining
//! module covers one area of the contract:
//!
//! - `render` — what one finished frame looks like: status markers, rate, ETA, suffixes.
//! - `layout` — how bars are assigned to the fixed grid of reserved slots.
//! - `lifecycle` — commit-on-join, finalize, slot recycling, pre-roll.
//! - `consumer` — the call sequences the real consumers use.
//! - `elapsed` — the elapsed field, driven by an injected clock.
//! - `resize` — width and height behaviour.
//! - `spinner` — spinner animation and the two public bar styles.
//! - `debug` — `MEDIAPM_PROGRESS_DEBUG` JSONL output.
//!
//! Rendering assertions are exact `assert_eq!` comparisons against the whole
//! captured frame, with the expected text inline via `concat!`, so a layout
//! regression reports the frame it drew rather than one failed substring.

mod common;
mod consumer;
mod debug;
mod elapsed;
mod layout;
mod lifecycle;
mod render;
mod resize;
mod spinner;
