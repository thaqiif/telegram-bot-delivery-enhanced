use super::{BotId, JobId, JobState, WebhookState};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub bot_id: BotId,
    pub method: String,
    pub state: JobState,
    pub total: u32,
    pub accepted_at_unix: Option<i64>,
    pub deadline_unix: Option<i64>,
    pub webhook_state: WebhookState,
}
