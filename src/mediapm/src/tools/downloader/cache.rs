//! Mediapm aliases for shared user-level download cache primitives.
//!
//! The underlying implementation lives in `mediapm-conductor` so conductor and
//! mediapm reuse the same cache engine. When invoked under `mediapm`, the
//! cache root is `<os-cache-dir>/mediapm/cache/`.

pub(crate) use mediapm_conductor::{
    UserDownloadCache as ToolDownloadCache, UserDownloadCachePruneReport as ToolCachePruneReport,
    default_mediapm_user_download_cache_root as default_global_tool_cache_root,
};

#[cfg(test)]
mod tests {
    use mediapm_conductor::use_user_download_cache_enabled;

    use super::default_global_tool_cache_root;

    /// Protects runtime cache-toggle behavior by preserving default-on
    /// semantics for omitted configuration.
    #[test]
    fn runtime_cache_toggle_defaults_to_enabled() {
        assert!(use_user_download_cache_enabled(None));
        assert!(use_user_download_cache_enabled(Some(true)));
        assert!(!use_user_download_cache_enabled(Some(false)));
    }

    /// Protects mediapm cache root so it resolves to the flat
    /// `<os-cache-dir>/mediapm/cache` layout with no namespace subdirectory.
    #[test]
    fn default_global_cache_root_uses_flat_mediapm_cache_layout() {
        let actual = default_global_tool_cache_root();

        if let Some(path) = actual {
            assert!(
                path.ends_with("cache"),
                "mediapm cache root must end with 'cache', got: {}",
                path.display()
            );
            let parent = path.parent().unwrap();
            assert!(
                parent.ends_with("mediapm"),
                "mediapm cache root parent must be 'mediapm', got: {}",
                parent.display()
            );
        }
    }
}
