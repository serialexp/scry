use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::CompiledJsonTemplate;

pub const MAX_TARGET_NAME_BYTES: usize = 256;
pub const MAX_ENDPOINT_BYTES: usize = 2_048;
pub const MAX_HEADERS: usize = 32;
pub const MAX_HEADER_BYTES: usize = 1_024;
pub const MAX_TIMEOUT_MILLIS: u32 = 120_000;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NotificationTargetId(pub Uuid);

impl NotificationTargetId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}
impl Default for NotificationTargetId {
    fn default() -> Self {
        Self::new()
    }
}
impl std::fmt::Display for NotificationTargetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogicalSecretId(pub Uuid);
impl LogicalSecretId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}
impl Default for LogicalSecretId {
    fn default() -> Self {
        Self::new()
    }
}
impl std::fmt::Display for LogicalSecretId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltInTargetFormat {
    GenericJson,
    Slack,
    CrossNotifier,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TargetFormat {
    BuiltIn { format: BuiltInTargetFormat },
    CustomJson { template: CompiledJsonTemplate },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetHeader {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotificationTargetKind {
    GenericWebhook {
        url: String,
        #[serde(default)]
        headers: Vec<TargetHeader>,
    },
    SlackWebhook,
}

/// Secret-free immutable target revision. Secret bytes and encryption metadata never
/// belong in this record or in API projections.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationTarget {
    pub schema_version: u32,
    pub id: NotificationTargetId,
    pub revision: u64,
    pub name: String,
    pub enabled: bool,
    pub kind: NotificationTargetKind,
    pub format: TargetFormat,
    pub timeout_millis: u32,
    pub logical_secret_id: LogicalSecretId,
    pub secret_generation: u64,
    pub created_at_unix_nano: u64,
    pub updated_at_unix_nano: u64,
}

/// Deliberately redacted control-plane/API view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationTargetProjection {
    pub id: NotificationTargetId,
    pub revision: u64,
    pub name: String,
    pub enabled: bool,
    pub kind: NotificationTargetKind,
    pub format: TargetFormat,
    pub timeout_millis: u32,
    pub has_secret: bool,
    pub created_at_unix_nano: u64,
    pub updated_at_unix_nano: u64,
}

impl From<&NotificationTarget> for NotificationTargetProjection {
    fn from(target: &NotificationTarget) -> Self {
        Self {
            id: target.id,
            revision: target.revision,
            name: target.name.clone(),
            enabled: target.enabled,
            kind: target.kind.clone(),
            format: target.format.clone(),
            timeout_millis: target.timeout_millis,
            has_secret: target.secret_generation > 0,
            created_at_unix_nano: target.created_at_unix_nano,
            updated_at_unix_nano: target.updated_at_unix_nano,
        }
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum TargetValidationError {
    #[error("target name must be nonempty and at most {MAX_TARGET_NAME_BYTES} bytes")]
    InvalidName,
    #[error("target timeout must be between 1 and {MAX_TIMEOUT_MILLIS} milliseconds")]
    InvalidTimeout,
    #[error("target endpoint or headers exceed their bounds")]
    Bounds,
}

impl NotificationTarget {
    pub fn validate(&self) -> Result<(), TargetValidationError> {
        if self.name.is_empty() || self.name.len() > MAX_TARGET_NAME_BYTES {
            return Err(TargetValidationError::InvalidName);
        }
        if self.timeout_millis == 0 || self.timeout_millis > MAX_TIMEOUT_MILLIS {
            return Err(TargetValidationError::InvalidTimeout);
        }
        if let NotificationTargetKind::GenericWebhook { url, headers } = &self.kind {
            if url.len() > MAX_ENDPOINT_BYTES
                || headers.len() > MAX_HEADERS
                || headers
                    .iter()
                    .any(|h| h.name.len() + h.value.len() > MAX_HEADER_BYTES)
            {
                return Err(TargetValidationError::Bounds);
            }
        }
        Ok(())
    }
}
