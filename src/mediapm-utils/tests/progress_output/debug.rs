//! Progress debug-instrumentation tests (JSONL, one snapshot per drawn frame).
//!
//! The sink contract has three parts: every drawn frame emits one JSON line,
//! the line carries the documented bar state, and the file-based sink appends
//! rather than truncates so several screens of one process share one log.
//!
//! Most tests here use an in-memory `Write` sink instead of a file. The debug
//! writer is injected into the terminal, so a shared buffer is enough to read
//! the output at any point — which also removes the need to drop the terminal
//! (and with it the race the file-based tests had) before asserting anything.
//! The two tests that must exercise the file path use `MEDIAPM_PROGRESS_DEBUG`,
//! because auto-creating the file *is* the behaviour under test there.

use std::io::Write;
use std::sync::{Arc, Mutex};

use mediapm_utils::progress::ProgressDebugSink;

use super::common::{ENV_LOCK, EnvVarGuard, mk_with_debug_sink, mk_with_size};

/// A `Write` sink that appends to a shared buffer.
///
/// Cloning shares the same buffer, so the test keeps a reader while the
/// terminal owns the writer.
#[derive(Clone, Default)]
struct SharedSink(Arc<Mutex<Vec<u8>>>);

impl SharedSink {
    /// The buffer decoded as text; incomplete UTF-8 is a bug worth surfacing.
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("sink lock").clone())
            .expect("the sink writes UTF-8 JSONL")
    }

    /// The non-empty lines written so far.
    fn lines(&self) -> Vec<String> {
        self.text().lines().map(str::to_string).collect()
    }
}

impl Write for SharedSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("sink lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Wrap a shared sink for the terminal.
fn sink() -> (SharedSink, ProgressDebugSink) {
    let shared = SharedSink::default();
    let debug = ProgressDebugSink::new(Box::new(shared.clone()));
    (shared, debug)
}

/// A drawn frame emits exactly one JSONL line carrying the documented fields
/// and the bar's own state.
#[test]
fn progress_debug_emits_one_line_per_draw() {
    let (shared, debug) = sink();
    let (terminal, _term) = mk_with_debug_sink(4, 80, 4, debug);
    let screen = terminal.screen().build();
    let bar = screen.add_bar(4, "test-label");
    bar.set_position(2);

    screen.tick();
    let after_first = shared.lines();
    assert_eq!(after_first.len(), 1, "one drawn frame emits exactly one line: {after_first:?}");

    let line = &after_first[0];
    for field in [r#""type":"tick""#, r#""tick""#, r#""elapsed_secs""#, r#""bars""#] {
        assert!(line.contains(field), "line is missing {field}: {line}");
    }
    for field in [
        r#""slot":0"#,
        r#""bound":true"#,
        r#""label":"test-label""#,
        r#""position":2"#,
        r#""total":4"#,
        r#""status""#,
        r#""dirty""#,
    ] {
        assert!(line.contains(field), "bar state is missing {field}: {line}");
    }

    screen.tick();
    assert!(shared.lines().len() > 1, "every further drawn frame appends a line");
}

/// A screen with no bars still emits snapshots, and every slot reports itself
/// unbound — the invariant the previous version only appeared to test, by
/// matching the `"bars":` prefix that an array of bound bars also satisfies.
#[test]
fn progress_debug_without_bars_marks_every_slot_unbound() {
    let (shared, debug) = sink();
    let (terminal, _term) = mk_with_debug_sink(4, 80, 4, debug);
    let screen = terminal.screen().build();

    screen.tick();
    let lines = shared.lines();
    assert!(!lines.is_empty(), "a tick with no bars still emits a snapshot");
    for line in &lines {
        assert!(!line.contains(r#""bound":true"#), "no slot may be bound: {line}");
        assert_eq!(
            line.matches(r#""bound":false"#).count(),
            4,
            "every reserved slot is reported, all unbound: {line}"
        );
    }
}

/// Setting `MEDIAPM_PROGRESS_DEBUG` makes the terminal create its own
/// file-backed sink, so an ordinary run can be logged without code changes.
#[test]
fn progress_debug_env_var_enables_a_file_sink() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let debug_path = dir.path().join("debug-env.jsonl");
    let debug_path_str = debug_path.to_str().expect("utf-8 temp path").to_string();

    let _lock = ENV_LOCK.lock().expect("env lock poisoned");
    // SAFETY: the lock is held for the whole mutation window.
    let _guard = unsafe { EnvVarGuard::set("MEDIAPM_PROGRESS_DEBUG", &debug_path_str) };

    let (terminal, _term) = mk_with_size(4, 80);
    let screen = terminal.screen().build();
    screen.add_bar(1, "env-bar").finish_success();
    screen.tick();
    screen.join();
    drop(terminal);

    let contents = std::fs::read_to_string(&debug_path).expect("the env var creates the sink file");
    assert!(!contents.is_empty(), "the auto-created sink received the frame");
    for field in [r#""type":"tick""#, r#""env-bar""#] {
        assert!(contents.contains(field), "the auto-created sink is missing {field}: {contents}");
    }
}

/// Two screens in one process share the debug file: the second must append, or
/// the first screen's frames are lost.
#[test]
fn progress_debug_appends_across_screens() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let debug_path = dir.path().join("debug-append.jsonl");
    let debug_path_str = debug_path.to_str().expect("utf-8 temp path").to_string();

    let _lock = ENV_LOCK.lock().expect("env lock poisoned");
    // SAFETY: the lock is held for the whole mutation window.
    let _guard = unsafe { EnvVarGuard::set("MEDIAPM_PROGRESS_DEBUG", &debug_path_str) };

    for label in ["first-screen", "second-screen"] {
        let (terminal, _term) = mk_with_size(4, 80);
        let screen = terminal.screen().build();
        screen.add_bar(1, label).finish_success();
        screen.tick();
        screen.join();
        drop(terminal);
    }

    let contents = std::fs::read_to_string(&debug_path).expect("the env var creates the sink file");
    assert!(
        contents.contains("first-screen"),
        "the second screen truncated the first screen's lines: {contents}"
    );
    assert!(contents.contains("second-screen"), "the second screen logged its own lines");
}
