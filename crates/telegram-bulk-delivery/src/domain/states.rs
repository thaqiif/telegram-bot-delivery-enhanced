use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Accepting,
    Queued,
    Running,
    Completed,
    FailedInternal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecipientState {
    Queued,
    Leased,
    Delayed,
    Succeeded,
    Failed,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookState {
    NotConfigured,
    Pending,
    Delivering,
    Delivered,
    Exhausted,
}
