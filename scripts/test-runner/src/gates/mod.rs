//! Repository hygiene gates that run after the test suite.
//!
//! Both gates answer one question: did the suite leave the tree in a state
//! the next person can work with. They used to exist twice, once in
//! `run-all-tests.sh` and once in `run-all-tests.ps1`; keeping them here
//! removes the parity requirement instead of maintaining it.

pub mod janitor;
pub mod tempdir;
