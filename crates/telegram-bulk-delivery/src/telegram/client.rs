use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use telegram_api_meta::{is_multipart, method, MethodSpec};
#[derive(Clone)]
pub struct TelegramClient {
    http: Client,
    base: String,
}
impl TelegramClient {
    pub fn new(base: impl Into<String>) -> Result<Self, reqwest::Error> {
        Ok(Self {
            http: Client::builder().pool_max_idle_per_host(4).build()?,
            base: base.into(),
        })
    }
    pub async fn send_json(
        &self,
        token: &str,
        spec: &MethodSpec,
        params: &Value,
    ) -> Result<(u16, Value), reqwest::Error> {
        debug_assert!(!is_multipart(spec));
        let url = format!(
            "{}/bot{}/{}",
            self.base.trim_end_matches('/'),
            token,
            spec.name
        );
        let response = self
            .http
            .post(url)
            .timeout(Duration::from_secs(20))
            .json(params)
            .send()
            .await?;
        let status = response.status().as_u16();
        let body = response.json().await?;
        Ok((status, body))
    }
    pub async fn get_me(&self, token: &str) -> Result<(u16, Value), reqwest::Error> {
        let spec = method("getMe");
        let url = format!("{}/bot{}/getMe", self.base.trim_end_matches('/'), token);
        let response = self
            .http
            .post(url)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;
        let status = response.status().as_u16();
        let body = response.json().await?;
        debug_assert!(spec.is_none());
        Ok((status, body))
    }
}
