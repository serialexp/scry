use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MONITOR_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MonitorId(pub Uuid);

impl MonitorId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MonitorId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MonitorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    Metrics,
    Logs,
    Traces,
    Profiles,
}

impl Signal {
    pub fn table_name(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Profiles => "profiles",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EqualityMatcher {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarQuery {
    pub target_id: String,
    pub signal: Signal,
    pub matchers: Vec<EqualityMatcher>,
    pub lookback_seconds: u64,
    pub sql: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Comparator {
    Lt,
    Lte,
    Gt,
    Gte,
    Eq,
    Ne,
}

impl Comparator {
    pub fn compare(self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Lt => value < threshold,
            Self::Lte => value <= threshold,
            Self::Gt => value > threshold,
            Self::Gte => value >= threshold,
            Self::Eq => value == threshold,
            Self::Ne => value != threshold,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoDataPolicy {
    NoData,
    Firing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionErrorPolicy {
    KeepLast,
    Error,
    Firing,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarCondition {
    pub comparator: Comparator,
    pub threshold: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Monitor {
    pub schema_version: u32,
    pub id: MonitorId,
    #[serde(with = "crate::serde_u64")]
    pub revision: u64,
    pub name: String,
    pub enabled: bool,
    pub query: ScalarQuery,
    pub condition: ScalarCondition,
    pub every_seconds: u64,
    pub jitter_seconds: u64,
    pub for_seconds: u64,
    pub recover_for_seconds: u64,
    pub no_data: NoDataPolicy,
    pub execution_error: ExecutionErrorPolicy,
    pub labels: Vec<(String, String)>,
    pub annotations: Vec<(String, String)>,
    #[serde(with = "crate::serde_u64")]
    pub created_at_unix_nano: u64,
    #[serde(with = "crate::serde_u64")]
    pub updated_at_unix_nano: u64,
}
