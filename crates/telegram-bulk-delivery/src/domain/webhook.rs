use super::{BotId, EventId, JobId, WebhookState};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub id: EventId,
    pub job_id: JobId,
    pub bot_id: BotId,
    pub state: WebhookState,
    pub page_count: u16,
}
