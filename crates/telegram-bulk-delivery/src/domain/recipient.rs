use super::{BotId, JobId, RecipientState};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recipient {
    pub job_id: JobId,
    pub idx: u32,
    pub bot_id: BotId,
    pub patch_json: String,
    pub chat_id: Option<String>,
    pub state: RecipientState,
    pub attempt_count: u16,
    pub retry_consumed: u16,
    pub wire_started: bool,
    pub lease_owner: Option<String>,
    pub lease_until_unix: Option<i64>,
    pub not_before_unix: i64,
}
