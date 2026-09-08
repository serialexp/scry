//! Shared ownership rules for keys in a Scry object-storage bucket.
//!
//! Telemetry readers must classify keys before looking at filename suffixes.
//! Otherwise control-plane objects such as `_scry/.../*.meta.json` can be
//! mistaken for block sidecars and enter the derived block catalog.

/// The permanent catalog-control root retained for compatibility with D-055.
pub const CATALOG_CONTROL_ROOT: &str = "_catalog";

/// The reserved root for Scry control-plane products.
///
/// Reader support for this root ships before any writer is allowed to create
/// objects beneath it.
pub const SCRY_CONTROL_ROOT: &str = "_scry";

/// Top-level ownership of an object-storage key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectNamespace<'a> {
    /// A telemetry block object or another non-control object.
    Telemetry,
    /// Existing catalog bootstrap state below `_catalog/`.
    CatalogControl,
    /// A Scry control-plane object below `_scry/<subsystem>/`.
    ScryControl { subsystem: Option<&'a str> },
}

/// Classify an object key by its first complete path segment.
///
/// Matching is segment-aware: `_scryevil/...` and `_catalogue/...` are not
/// reserved. The bare `_catalog` and `_scry` keys are reserved as well, so a
/// malformed control key cannot fall through into telemetry discovery.
pub fn classify_object_key(key: &str) -> ObjectNamespace<'_> {
    let mut segments = key.split('/');
    match segments.next().unwrap_or_default() {
        CATALOG_CONTROL_ROOT => ObjectNamespace::CatalogControl,
        SCRY_CONTROL_ROOT => ObjectNamespace::ScryControl {
            subsystem: segments.next().filter(|segment| !segment.is_empty()),
        },
        _ => ObjectNamespace::Telemetry,
    }
}

/// Return whether a key belongs to a reserved Scry control namespace.
pub fn is_reserved_control_key(key: &str) -> bool {
    !matches!(classify_object_key(key), ObjectNamespace::Telemetry)
}

/// Return whether a top-level signal/prefix name is reserved for control data.
///
/// Callers that construct a prefix from an untrusted or externally supplied
/// signal must reject both the bare roots and strings with a following slash.
pub fn is_reserved_control_prefix(prefix: &str) -> bool {
    matches!(
        prefix.split('/').next().unwrap_or_default(),
        CATALOG_CONTROL_ROOT | SCRY_CONTROL_ROOT
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_reserved_roots_by_complete_segment() {
        assert_eq!(
            classify_object_key("_catalog/snapshot.sqlite"),
            ObjectNamespace::CatalogControl
        );
        assert_eq!(
            classify_object_key("_catalog"),
            ObjectNamespace::CatalogControl
        );
        assert_eq!(
            classify_object_key("_scry/errors/v1/projections/commit.meta.json"),
            ObjectNamespace::ScryControl {
                subsystem: Some("errors")
            }
        );
        assert_eq!(
            classify_object_key("_scry"),
            ObjectNamespace::ScryControl { subsystem: None }
        );
        assert_eq!(
            classify_object_key("_scry//commit.meta.json"),
            ObjectNamespace::ScryControl { subsystem: None }
        );
    }

    #[test]
    fn does_not_reserve_lookalike_or_embedded_segments() {
        for key in [
            "_scryevil/errors/commit.meta.json",
            "_catalogue/snapshot.meta.json",
            "logs/2026/09/07/_scry/block.meta.json",
            "metrics/2026/09/07/writer/block.meta.json",
            "",
        ] {
            assert_eq!(
                classify_object_key(key),
                ObjectNamespace::Telemetry,
                "unexpected classification for {key:?}"
            );
        }
    }

    #[test]
    fn reserved_control_predicate_covers_meta_json_decoys() {
        assert!(is_reserved_control_key(
            "_scry/alerts/v1/rules/rule.meta.json"
        ));
        assert!(is_reserved_control_key("_catalog/decoy.meta.json"));
        assert!(!is_reserved_control_key(
            "logs/2026/09/07/writer/block.meta.json"
        ));
    }

    #[test]
    fn classifies_untrusted_prefixes_without_matching_lookalikes() {
        assert!(is_reserved_control_prefix("_scry"));
        assert!(is_reserved_control_prefix("_scry/errors"));
        assert!(is_reserved_control_prefix("_catalog"));
        assert!(!is_reserved_control_prefix("_scryevil"));
        assert!(!is_reserved_control_prefix("logs"));
    }
}
