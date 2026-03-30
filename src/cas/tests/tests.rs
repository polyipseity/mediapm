//! Integration test harness for `mediapm-cas`.
//!
//! The `int` module contains API-level integration checks, `e2e` runs longer
//! real-world style workflows that span multiple operations and sessions, and
//! `prop` contains property-based tests.
//!
//! Keep this file intentionally thin: all scenario logic belongs in the
//! leaf modules to preserve focused test ownership.

mod e2e;
mod int;
mod prop;
