use crate::domain::BotId;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Debug, Clone)]
pub enum Behavior {
    Success { message_id: i64 },
    Latency { millis: u64, message_id: i64 },
    Flood { retry_after: u32 },
    ServerError,
    Unauthorized,
    Migrate { chat_id: i64 },
    BlackholeAfterWire,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedSend {
    pub bot_id: BotId,
    pub method: String,
    pub chat_id: Option<String>,
    pub wire_started: bool,
}
#[derive(Clone, Default)]
pub struct Simulator {
    behaviors: Arc<Mutex<VecDeque<Behavior>>>,
    captured: Arc<Mutex<Vec<CapturedSend>>>,
    by_scope: Arc<Mutex<HashMap<String, usize>>>,
}
impl Simulator {
    pub fn push(&self, b: Behavior) {
        self.behaviors.lock().expect("behaviors").push_back(b)
    }
    pub fn captured(&self) -> Vec<CapturedSend> {
        self.captured.lock().expect("captured").clone()
    }
    pub fn scope_count(&self, s: &str) -> usize {
        *self.by_scope.lock().expect("scopes").get(s).unwrap_or(&0)
    }
    pub async fn send(
        &self,
        bot_id: BotId,
        method: &str,
        chat_id: Option<&str>,
        wire_started: bool,
    ) -> Result<(u16, Value), ()> {
        assert!(wire_started, "send attempted before MarkWireStarted commit");
        self.captured.lock().expect("captured").push(CapturedSend {
            bot_id,
            method: method.into(),
            chat_id: chat_id.map(str::to_owned),
            wire_started,
        });
        let scope = format!("{}:{}", method.to_ascii_lowercase(), chat_id.unwrap_or("-"));
        *self
            .by_scope
            .lock()
            .expect("scopes")
            .entry(scope)
            .or_default() += 1;
        let behavior = self
            .behaviors
            .lock()
            .expect("behaviors")
            .pop_front()
            .unwrap_or(Behavior::Success { message_id: 1 });
        match behavior {
            Behavior::Success { message_id } => Ok((
                200,
                json!({"ok":true,"result":{"message_id":message_id,"date":1}}),
            )),
            Behavior::Latency { millis, message_id } => {
                tokio::time::sleep(Duration::from_millis(millis)).await;
                Ok((200, json!({"ok":true,"result":{"message_id":message_id}})))
            }
            Behavior::Flood { retry_after } => Ok((
                429,
                json!({"ok":false,"parameters":{"retry_after":retry_after}}),
            )),
            Behavior::ServerError => Ok((503, json!({"ok":false}))),
            Behavior::Unauthorized => Ok((401, json!({"ok":false}))),
            Behavior::Migrate { chat_id } => Ok((
                400,
                json!({"ok":false,"parameters":{"migrate_to_chat_id":chat_id}}),
            )),
            Behavior::BlackholeAfterWire => Err(()),
        }
    }
}
