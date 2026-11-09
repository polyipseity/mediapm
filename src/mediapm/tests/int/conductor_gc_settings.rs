//! Pins the hop from a `mediapm.ncl` lifecycle group to the conductor
//! facade's GC knobs: the document is the user-facing knob, a stated
//! service override beats it, and an unset pair falls back to the
//! built-in defaults.

use mediapm::config::{MediaRuntimeStorage, RuntimeLifecycleConfig};
use mediapm::{MediaPmService, load_mediapm_document, save_mediapm_document};

/// Document-stated lifecycle values reach the facade (RED today: the
/// constructor passes the raw override, so the facade holds defaults).
#[tokio::test]
async fn document_lifecycle_values_reach_the_conductor_facade() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("scaffold");
    drop(service);
    let ncl = root.path().join("mediapm.ncl");
    let mut doc = load_mediapm_document(&ncl).expect("scaffolded document loads");
    doc.runtime.lifecycle.instance_ttl_seconds = Some(90_000);
    doc.runtime.lifecycle.background_gc_interval_seconds = Some(42);
    save_mediapm_document(&ncl, &doc).expect("stated document saves");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("reopen");
    assert_eq!(
        service.conductor().gc_settings(),
        (90_000, 42),
        "a document-stated lifecycle group must reach the sweep"
    );
}

/// A stated service override beats the document (green on both sides of
/// the change; guards the pick from over-correcting to config-only).
#[tokio::test]
async fn a_stated_override_beats_the_document() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("scaffold");
    drop(service);
    let ncl = root.path().join("mediapm.ncl");
    let mut doc = load_mediapm_document(&ncl).expect("scaffolded document loads");
    doc.runtime.lifecycle.instance_ttl_seconds = Some(90_000);
    doc.runtime.lifecycle.background_gc_interval_seconds = Some(42);
    save_mediapm_document(&ncl, &doc).expect("stated document saves");
    let overrides = MediaRuntimeStorage {
        lifecycle: RuntimeLifecycleConfig {
            instance_ttl_seconds: 77,
            background_gc_interval_seconds: 88,
        },
        ..MediaRuntimeStorage::default()
    };
    let service = MediaPmService::new_fs_at_with_runtime_storage_overrides(
        root.path().to_path_buf(),
        overrides,
    )
    .await
    .expect("reopen with overrides");
    assert_eq!(
        service.conductor().gc_settings(),
        (77, 88),
        "a stated override must win over the document"
    );
}

/// No lifecycle stated (the scaffold creates a `mediapm.ncl` without
/// lifecycle values): the built-in defaults (green both sides).
#[tokio::test]
async fn defaults_hold_when_nothing_states_a_value() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("scaffold");
    assert_eq!(
        service.conductor().gc_settings(),
        (
            mediapm::config::defaults::default_instance_ttl_seconds(),
            mediapm::config::defaults::default_background_gc_interval_seconds()
        ),
        "an unstated pair resolves to the built-in defaults"
    );
}

/// The merge keeps the document half when the override states nothing
/// (RED today: the override half is taken unconditionally).
#[test]
fn merge_keeps_document_lifecycle_when_the_override_states_nothing() {
    let mut config = MediaRuntimeStorage::default();
    config.lifecycle.instance_ttl_seconds = 90_000;
    config.lifecycle.background_gc_interval_seconds = 42;
    let merged = mediapm::merge_runtime_storage(&config, &MediaRuntimeStorage::default());
    assert_eq!(
        (merged.lifecycle.instance_ttl_seconds, merged.lifecycle.background_gc_interval_seconds),
        (90_000, 42),
        "the document half must survive the merge"
    );
}

/// An override equal to the built-in default is the unstated sentinel: the
/// document must still win (pins R-29-01's stated "cost if wrong" corner).
#[tokio::test]
async fn an_override_equal_to_the_default_does_not_usurp_a_stated_document() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let service = MediaPmService::new_fs_at(root.path()).await.expect("scaffold");
    drop(service);
    let ncl = root.path().join("mediapm.ncl");
    let mut doc = load_mediapm_document(&ncl).expect("scaffolded document loads");
    doc.runtime.lifecycle.instance_ttl_seconds = Some(90_000);
    doc.runtime.lifecycle.background_gc_interval_seconds = Some(42);
    save_mediapm_document(&ncl, &doc).expect("stated document saves");
    let overrides = MediaRuntimeStorage {
        lifecycle: RuntimeLifecycleConfig {
            // Both equal to the built-in defaults: the unstated sentinel.
            instance_ttl_seconds: mediapm::config::defaults::default_instance_ttl_seconds(),
            background_gc_interval_seconds:
                mediapm::config::defaults::default_background_gc_interval_seconds(),
        },
        ..MediaRuntimeStorage::default()
    };
    let service = MediaPmService::new_fs_at_with_runtime_storage_overrides(
        root.path().to_path_buf(),
        overrides,
    )
    .await
    .expect("reopen with overrides");
    assert_eq!(
        service.conductor().gc_settings(),
        (90_000, 42),
        "an override equal to the built-in default is unstated; the document must win"
    );
}
