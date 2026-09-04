use std::{env, fs, io};
fn main() -> io::Result<()> {
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "crates/telegram-api-meta/fixtures/bot-api-10.3.json".into());
    let value = serde_json::json!({"bot_api_version":telegram_api_meta::BOT_API_VERSION,"methods":telegram_api_meta::METHODS,"non_bulk_methods":telegram_api_meta::NON_BULK_METHODS});
    fs::write(
        path,
        serde_json::to_vec_pretty(&value).expect("metadata serializes"),
    )
}
