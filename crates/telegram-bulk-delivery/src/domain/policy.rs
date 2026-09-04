use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AmbiguityPolicy {
    AtLeastOnce,
    AtMostOnce,
    MethodAware,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicySnapshot {
    pub retry_max_attempts: u16,
    pub ambiguity_policy: AmbiguityPolicy,
    pub fairness_weight: u8,
    pub job_deadline_secs: u64,
}
