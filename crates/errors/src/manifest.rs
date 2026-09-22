//! Deployment manifest compatibility re-export.
//!
//! Deployment identity is shared by control-plane products and is owned by
//! `scry-objstore`; keep this module so existing error-domain callers do not need
//! to change paths atomically with that extraction.

pub use scry_objstore::manifest::*;
