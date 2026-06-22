//! Shared helpers for conductor examples.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Convenient result type for examples.
pub(crate) type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Best-effort temporary directory guard for ephemeral examples.
#[derive(Debug)]
pub(crate) struct EphemeralRunDir {
    path: PathBuf,
}

impl EphemeralRunDir {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for EphemeralRunDir {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

pub(crate) fn create_ephemeral_run_dir(example_name: &str) -> ExampleResult<EphemeralRunDir> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let seq = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = format!("{example_name}-{pid}-{timestamp_ns}-{seq}");
    let p = std::env::temp_dir().join("mediapm-conductor-examples").join(dir);
    fs::create_dir_all(&p)?;
    Ok(EphemeralRunDir { path: p })
}

pub(crate) fn write_text_file(path: &Path, content: &str) -> ExampleResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(())
}
