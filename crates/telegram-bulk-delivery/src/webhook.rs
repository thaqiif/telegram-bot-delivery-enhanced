//! Paged completion webhooks (plan §15): deterministic in-order page creation,
//! HMAC-SHA256 over exact stored body bytes, SSRF-safe delivery, per-page retry.

use crate::auth::{EncryptedSecret, KeyRing, Purpose};
use crate::domain::BotId;
use crate::store::{Store, WebhookMark, WebhookPageRow, WebhookTerminal, WriterCmd};
use hmac::{Hmac, Mac};
use rand::Rng;
use reqwest::{redirect::Policy, Client};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio::time::interval;
use url::Url;

type HmacSha256 = Hmac<Sha256>;

/// Maximum serialized bytes for one webhook page body (256 KiB ceiling).
pub const MAX_WEBHOOK_PAGE_BYTES: usize = 256 * 1024;
/// Reserve for the envelope so results stay within the ceiling.
const ENVELOPE_RESERVE_BYTES: usize = 4096;
/// Upper bound on pages delivered per pump (keeps a pump call bounded).
const MAX_PAGES_PER_PUMP: usize = 200;
/// Request timeout for one webhook page POST.
const REQUEST_TIMEOUT_SECS: u64 = 15;

/// Hex-encoded HMAC-SHA256 of `body` under `secret` (`X-Bulk-Signature: v1=<hex>`).
pub fn hmac_hex(secret: &[u8], body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

pub fn sha256_hex(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

/// Build the exact canonical JSON bodies for a completed job's webhook pages,
/// in order. There is always at least one page (empty `results` when there are
/// no successes). Each body stays within `max_page_bytes`. Rows are
/// `(chat_id, telegram_message_id, time_send_unix)` where message id may be null.
#[allow(clippy::too_many_arguments)]
pub fn page_bodies(
    job_id: &str,
    event_id: &str,
    job_state: &str,
    total: u32,
    succeeded: u32,
    failed: u32,
    ambiguous: u32,
    rows: impl IntoIterator<Item = (Option<String>, Option<i64>, i64)>,
    max_page_bytes: usize,
) -> Vec<Vec<u8>> {
    let mut page_results: Vec<Vec<Value>> = vec![Vec::new()];
    let mut page_sizes: Vec<usize> = vec![0];
    for (chat, msg, ts) in rows {
        let tuple = json!([chat, msg, ts]);
        let len = serde_json::to_vec(&tuple).map(|b| b.len()).unwrap_or(0);
        let cur = page_results.len() - 1;
        if !page_results[cur].is_empty()
            && page_sizes[cur] + len + ENVELOPE_RESERVE_BYTES > max_page_bytes
        {
            page_results.push(Vec::new());
            page_sizes.push(0);
        }
        let cur = page_results.len() - 1;
        page_sizes[cur] += len + 1; // reserve for a separator comma
        page_results[cur].push(tuple);
    }
    let page_count = page_results.len();
    page_results
        .into_iter()
        .enumerate()
        .map(|(idx, results)| {
            let mut results = results;
            loop {
                let body = serde_json::to_vec(&json!({
                    "job_id": job_id,
                    "event_id": event_id,
                    "state": job_state,
                    "total": total,
                    "succeeded": succeeded,
                    "failed": failed,
                    "ambiguous": ambiguous,
                    "page": idx,
                    "page_count": page_count,
                    "results": results,
                }))
                .expect("webhook envelope serializes");
                if body.len() <= max_page_bytes || results.is_empty() {
                    return body;
                }
                results.pop();
            }
        })
        .collect()
}

/// SSRF / scheme policy. The default denies everything except HTTPS to public,
/// non-private hosts. Tests (or operators that explicitly opt in) may trust
/// specific hosts and permit HTTP to them so a local receiver can be used.
#[derive(Debug, Clone, Default)]
pub struct WebhookSsrPolicy {
    pub allow_insecure_http: bool,
    pub trusted_hosts: Vec<String>,
}

impl WebhookSsrPolicy {
    fn is_trusted(&self, host: &str) -> bool {
        let h = normalize_host(host);
        self.trusted_hosts.iter().any(|t| normalize_host(t) == h)
    }
}

fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').trim().to_ascii_lowercase()
}

/// True when the DNS name is denied before any resolution happens.
pub fn denied_name(host0: &str) -> bool {
    let h = normalize_host(host0);
    const EXACT: &[&str] = &[
        "localhost",
        "localhost.localdomain",
        "localhost.localhost",
        "metadata",
        "metadata.google.internal",
        "metadata.google.com",
        "instance-data",
        "169.254.169.254",
        "0.0.0.0",
    ];
    if EXACT.contains(&h.as_str()) {
        return true;
    }
    for suffix in ["internal", "localhost", "local"] {
        if h.ends_with(&format!(".{suffix}")) {
            return true;
        }
    }
    // Numeric literals (decimal / hex / plain IP) that decode to a denied address.
    if let Some(ip) = parse_ip_literal(&h) {
        return is_denied_ip(ip);
    }
    false
}

/// Parse a host into an IP if it is an address literal (including 32-bit
/// decimal `2130706433` and hex `0x7f000001` forms) or `[v6]` bracket form.
fn parse_ip_literal(host0: &str) -> Option<IpAddr> {
    let host = normalize_host(host0);
    if let Ok(ip) = IpAddr::from_str(&host) {
        return Some(ip);
    }
    let unbracketed = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(&host);
    if let Ok(v6) = Ipv6Addr::from_str(unbracketed) {
        return Some(IpAddr::V6(v6));
    }
    let num = host
        .parse::<u32>()
        .ok()
        .or_else(|| {
            host.strip_prefix("0x")
                .and_then(|h| u32::from_str_radix(h, 16).ok())
        })
        .or_else(|| {
            host.strip_prefix("0X")
                .and_then(|h| u32::from_str_radix(h, 16).ok())
        });
    if let Some(v) = num {
        return Some(IpAddr::V4(Ipv4Addr::from(v)));
    }
    None
}

/// True when the address may never be a webhook target, regardless of scheme.
pub fn is_denied_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            o[0] == 0 || // 0.0.0.0/8 unspecified
            o[0] == 127 || // loopback
            o[0] == 10 || // RFC1918
            (o[0] == 172 && (16..=31).contains(&o[1])) || // RFC1918
            (o[0] == 192 && o[1] == 168) || // RFC1918
            (o[0] == 169 && o[1] == 254) || // link-local incl. 169.254.169.254
            (o[0] == 100 && (64..=127).contains(&o[1])) || // CGNAT 100.64/10
            o[0] >= 224 // multicast + broadcast
        }
        IpAddr::V6(v6) => {
            let s = v6.segments();
            s == [0; 8] || // ::
            s == [0, 0, 0, 0, 0, 0, 0, 1] || // ::1
            (s[0] & 0xfe00) == 0xfc00 || // ULA fc00::/7
            (s[0] & 0xffc0) == 0xfe80 || // link-local fe80::/10
            (s[0] & 0xff00) == 0xff00 || // multicast ff00::/8
            (s[0..2] == [0x64, 0xff9b] && s[2..6] == [0, 0, 0, 0]) || // NAT64 64:ff9b::/96
            // IPv4-mapped IPv6 is judged by its embedded IPv4 address.
            v6.to_ipv4_mapped()
                .is_some_and(|v4| is_denied_ip(IpAddr::V4(v4)))
        }
    }
}

pub type ResolveResult = Result<Vec<IpAddr>, String>;

pub trait HostResolver: Send + Sync {
    fn resolve(&self, host: &str, port: u16)
        -> Pin<Box<dyn Future<Output = ResolveResult> + Send>>;
}

/// Default resolver backed by the system (`tokio::net::lookup_host`).
pub struct SystemResolver;

impl HostResolver for SystemResolver {
    fn resolve(
        &self,
        host: &str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = ResolveResult> + Send>> {
        let host = host.to_string();
        Box::pin(async move {
            let mut ips: Vec<IpAddr> = Vec::new();
            for addr in tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(|e| e.to_string())?
            {
                let ip = addr.ip();
                if !ips.contains(&ip) {
                    ips.push(ip);
                }
            }
            Ok(ips)
        })
    }
}

/// Delivers eligible webhook pages on a 1 s poll, honoring per-page retry
/// schedules and in-order page gating. Never runs on the writer thread.
#[derive(Clone)]
pub struct WebhookDeliverer {
    store: Arc<Store>,
    keys: Arc<KeyRing>,
    sem: Arc<Semaphore>,
    policy: WebhookSsrPolicy,
    resolver: Arc<dyn HostResolver>,
    http: Client,
}

impl WebhookDeliverer {
    pub fn new(
        store: Arc<Store>,
        keys: Arc<KeyRing>,
        max_inflight: usize,
        policy: WebhookSsrPolicy,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            store,
            keys,
            sem: Arc::new(Semaphore::new(max_inflight.max(1))),
            policy,
            resolver: Arc::new(SystemResolver),
            http: Self::build_client(None)?,
        })
    }

    pub fn with_resolver(
        store: Arc<Store>,
        keys: Arc<KeyRing>,
        max_inflight: usize,
        policy: WebhookSsrPolicy,
        resolver: Arc<dyn HostResolver>,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            store,
            keys,
            sem: Arc::new(Semaphore::new(max_inflight.max(1))),
            policy,
            resolver,
            http: Self::build_client(None)?,
        })
    }

    fn build_client(pinned: Option<(&str, &[IpAddr])>) -> Result<Client, reqwest::Error> {
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .pool_max_idle_per_host(4)
            // SSRF pinning pins the validated address set directly; an env
            // proxy would re-resolve the target on its own and could reach a
            // private or metadata address despite the pinned DNS result.
            .no_proxy();
        if let Some((host, ips)) = pinned {
            let addrs: Vec<SocketAddr> = ips
                .iter()
                .map(|ip| SocketAddr::new(*ip, 0)) // port 0 => URL port wins
                .collect();
            builder = builder.resolve_to_addrs(host, &addrs);
        }
        builder.build()
    }

    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut ticker = interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let now = now_unix();
                    if let Err(e) = self.pump_at(now).await {
                        eprintln!("webhook deliverer error: {e}");
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        // Complete one final bounded pump so pages already
                        // claimed by this process are durably acknowledged
                        // before a graceful shutdown returns. A hard crash
                        // still follows normal lease/retry recovery.
                        if let Err(e) = self.pump_at(now_unix()).await {
                            eprintln!("webhook shutdown drain failed: {e}");
                        }
                        return;
                    }
                }
            }
        }
    }

    /// Drive delivery until no page is eligible at `now_unix`. Returns pages
    /// successfully delivered (page 0 or later) or otherwise handled.
    pub async fn pump_at(&self, now_unix: i64) -> Result<usize, String> {
        let mut handled = 0usize;
        loop {
            if handled >= MAX_PAGES_PER_PUMP {
                return Ok(handled);
            }
            let rows = self
                .store
                .readers
                .pending_webhook_pages(now_unix)
                .map_err(|e| e.to_string())?;
            if rows.is_empty() {
                return Ok(handled);
            }
            let mut tasks = Vec::with_capacity(rows.len());
            for row in rows {
                match self.sem.clone().try_acquire_owned() {
                    Ok(permit) => {
                        let me = self.clone();
                        tasks.push(tokio::spawn(async move {
                            let _permit = permit;
                            me.deliver_one(&row, now_unix).await
                        }));
                    }
                    Err(_) => break, // semaphore saturated; try again next pump
                }
            }
            for task in tasks {
                match task.await {
                    Ok(Ok(())) => handled += 1,
                    Ok(Err(e)) => eprintln!("webhook page attempt failed: {e}"),
                    Err(e) => eprintln!("webhook task join error: {e}"),
                }
            }
        }
    }

    async fn deliver_one(&self, row: &WebhookPageRow, now_unix: i64) -> Result<(), String> {
        let event_id = row.event_id.clone();
        let page = u16::try_from(row.page_index).unwrap_or(u16::MAX);
        let max_attempts = row.webhook_max_attempts.unwrap_or(10).clamp(1, 255) as u16;
        let retry_base_ms = row.webhook_retry_base_ms.unwrap_or(1000).max(1) as u64;
        let retry_max_ms = (row
            .webhook_retry_max_ms
            .unwrap_or(300_000)
            .max(retry_base_ms as i64)) as u64;
        let attempt = (row.page_attempt_count.max(0) as u64).saturating_add(1) as u16;

        // SSRF policy gates: url present/valid, scheme https (or http to a
        // trusted host when explicitly allowed), name not denied, then all
        // resolved addresses acceptable.
        let Some(url) = &row.webhook_url else {
            return record_attempt(
                &self.store,
                &event_id,
                page,
                attempt,
                now_unix,
                None,
                Some("webhook_url_missing".to_string()),
                None,
                Some(WebhookTerminal::Exhausted {
                    error: "webhook_url_missing".to_string(),
                }),
            );
        };
        let Ok(parsed) = Url::parse(url) else {
            return record_attempt(
                &self.store,
                &event_id,
                page,
                attempt,
                now_unix,
                None,
                Some("invalid_webhook_url".to_string()),
                None,
                Some(WebhookTerminal::Exhausted {
                    error: "invalid_webhook_url".to_string(),
                }),
            );
        };
        let Some(host) = parsed.host_str() else {
            return record_attempt(
                &self.store,
                &event_id,
                page,
                attempt,
                now_unix,
                None,
                Some("webhook_url_no_host".to_string()),
                None,
                Some(WebhookTerminal::Exhausted {
                    error: "webhook_url_no_host".to_string(),
                }),
            );
        };
        let trusted = self.policy.is_trusted(host);
        let scheme = parsed.scheme();
        let scheme_ok = match scheme {
            "https" => true,
            "http" => self.policy.allow_insecure_http && trusted,
            _ => false,
        };
        if !scheme_ok {
            return record_attempt(
                &self.store,
                &event_id,
                page,
                attempt,
                now_unix,
                None,
                Some(format!("ssrf_scheme_denied:{scheme}")),
                None,
                Some(WebhookTerminal::Exhausted {
                    error: format!("ssrf_scheme_denied:{scheme}"),
                }),
            );
        }
        if !trusted && denied_name(host) {
            let e = format!("ssrf_host_denied:{host}");
            return record_attempt(
                &self.store,
                &event_id,
                page,
                attempt,
                now_unix,
                None,
                Some(e.clone()),
                None,
                Some(WebhookTerminal::Exhausted { error: e }),
            );
        }

        // Resolve and pin. For a literal address there is no DNS, so no pinning
        // is needed; for a hostname every resolved address must be public.
        let port = parsed
            .port_or_known_default()
            .unwrap_or(if scheme == "http" { 80 } else { 443 });
        // Trusted hosts bypass the SSRF deny rules entirely (operator opted in).
        // A literal IP has no DNS to rebind, so no pinning is needed. A hostname
        // is resolved first and refused if ANY address is non-public.
        let client = if trusted || parse_ip_literal(host).is_some() {
            self.http.clone()
        } else {
            let ips = match self.resolver.resolve(host, port).await {
                Ok(ips) => ips,
                Err(e) => {
                    // A resolver failure must not livelock the pump (the page
                    // would otherwise stay immediately eligible forever): record
                    // a transient attempt with bounded backoff, exhausting only
                    // once the page's attempt budget is spent.
                    let err_text = format!("ssrf_resolve_failed:{e}");
                    if attempt >= max_attempts {
                        return record_attempt(
                            &self.store,
                            &event_id,
                            page,
                            attempt,
                            now_unix,
                            None,
                            Some(err_text.clone()),
                            None,
                            Some(WebhookTerminal::Exhausted { error: err_text }),
                        );
                    }
                    let backoff_ms = retry_backoff_ms(retry_base_ms, retry_max_ms, attempt);
                    let next_attempt =
                        now_unix.saturating_add(backoff_ms.div_ceil(1000).max(1) as i64);
                    return record_attempt(
                        &self.store,
                        &event_id,
                        page,
                        attempt,
                        now_unix,
                        None,
                        Some(err_text),
                        Some(next_attempt),
                        None,
                    );
                }
            };
            if ips.is_empty() {
                let e = format!("ssrf_no_addresses:{host}");
                return record_attempt(
                    &self.store,
                    &event_id,
                    page,
                    attempt,
                    now_unix,
                    None,
                    Some(e.clone()),
                    None,
                    Some(WebhookTerminal::Exhausted { error: e }),
                );
            }
            if ips.iter().any(|ip| is_denied_ip(*ip)) {
                let e = format!("ssrf_ip_denied:{host}:{ips:?}");
                return record_attempt(
                    &self.store,
                    &event_id,
                    page,
                    attempt,
                    now_unix,
                    None,
                    Some(e.clone()),
                    None,
                    Some(WebhookTerminal::Exhausted { error: e }),
                );
            }
            Self::build_client(Some((host, &ips))).map_err(|e| e.to_string())?
        };

        let Some(body) = &row.body else {
            return record_attempt(
                &self.store,
                &event_id,
                page,
                attempt,
                now_unix,
                None,
                Some("webhook_body_missing".to_string()),
                None,
                Some(WebhookTerminal::Exhausted {
                    error: "webhook_body_missing".to_string(),
                }),
            );
        };

        // Decrypt the webhook secret only here, in the deliverer (never writer).
        let secret = match (
            &row.webhook_secret_nonce,
            &row.webhook_secret_ciphertext,
            &row.webhook_secret_kid,
        ) {
            (Some(nonce), Some(ciphertext), Some(kid)) => {
                let enc = EncryptedSecret {
                    key_id: kid.clone(),
                    nonce: match <[u8; 12]>::try_from(nonce.as_slice()) {
                        Ok(n) => n,
                        Err(_) => {
                            let e = "webhook_secret_bad_nonce".to_string();
                            return record_attempt(
                                &self.store,
                                &event_id,
                                page,
                                attempt,
                                now_unix,
                                None,
                                Some(e.clone()),
                                None,
                                Some(WebhookTerminal::Exhausted { error: e }),
                            );
                        }
                    },
                    ciphertext: ciphertext.clone(),
                };
                match self
                    .keys
                    .decrypt(bot_id(row)?, Purpose::WebhookSecret, &enc)
                {
                    Ok(v) => v,
                    Err(_) => {
                        let e = "webhook_secret_decrypt_failed".to_string();
                        return record_attempt(
                            &self.store,
                            &event_id,
                            page,
                            attempt,
                            now_unix,
                            None,
                            Some(e.clone()),
                            None,
                            Some(WebhookTerminal::Exhausted { error: e }),
                        );
                    }
                }
            }
            _ => {
                let e = "webhook_secret_missing".to_string();
                return record_attempt(
                    &self.store,
                    &event_id,
                    page,
                    attempt,
                    now_unix,
                    None,
                    Some(e.clone()),
                    None,
                    Some(WebhookTerminal::Exhausted { error: e }),
                );
            }
        };

        let body_sha = sha256_hex(body);
        let signature = hmac_hex(&secret, body);
        let page_count = row.page_count as u16;

        let resp = client
            .post(url.clone())
            .header("Content-Type", "application/json")
            .header(
                "User-Agent",
                concat!("telegram-bulk-delivery/", env!("CARGO_PKG_VERSION")),
            )
            .header("X-Bulk-Signature", format!("v1={signature}"))
            .header("X-Bulk-Event-Id", &event_id)
            .header("X-Bulk-Job-Id", &row.job_id)
            .header("X-Bulk-Page", page.to_string())
            .header("X-Bulk-Page-Count", page_count.to_string())
            .header("X-Bulk-Attempt", attempt.to_string())
            .header("X-Bulk-Timestamp", now_unix.to_string())
            .header(
                "X-Bulk-Idempotency-Key",
                format!("{event_id}:{page}:{}", &body_sha[..8]),
            )
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .body(body.clone())
            .send()
            .await;

        let (http_status, error): (Option<u16>, Option<String>) = match resp {
            Ok(r) => (Some(r.status().as_u16()), None),
            Err(e) => (None, Some(e.to_string())),
        };
        let is_success = http_status
            .map(|s| (200..300).contains(&s))
            .unwrap_or(false);
        if is_success {
            return record_attempt(
                &self.store,
                &event_id,
                page,
                attempt,
                now_unix,
                http_status,
                None,
                None,
                Some(WebhookTerminal::Delivered),
            );
        }

        // Not delivered: 404/410 are dead-ends once a few attempts have been
        // made; otherwise retry until the configured attempt budget is spent.
        let status = http_status.unwrap_or(0);
        let dead_end = (status == 404 || status == 410) && attempt >= 3;
        if attempt >= max_attempts || dead_end {
            let e = format!(
                "exhausted status={status} error={}",
                error.clone().unwrap_or_else(|| "no error".to_string())
            );
            return record_attempt(
                &self.store,
                &event_id,
                page,
                attempt,
                now_unix,
                http_status,
                error,
                None,
                Some(WebhookTerminal::Exhausted { error: e }),
            );
        }
        // Bounded full-jitter: next = min(max, base*2^(attempt-1) + Uniform(0, base)).
        let next_attempt = now_unix.saturating_add(
            retry_backoff_ms(retry_base_ms, retry_max_ms, attempt)
                .div_ceil(1000)
                .max(1) as i64,
        );
        record_attempt(
            &self.store,
            &event_id,
            page,
            attempt,
            now_unix,
            http_status,
            error,
            Some(next_attempt),
            None,
        )
    }
}

/// Bounded full-jitter delay (ms) before a webhook page's next attempt.
fn retry_backoff_ms(base_ms: u64, max_ms: u64, attempt: u16) -> u64 {
    let shift = u32::from(attempt.saturating_sub(1)).min(30);
    let exp = base_ms.saturating_mul(1u64.checked_shl(shift).unwrap_or(u64::MAX));
    max_ms.min(exp.saturating_add(rand::thread_rng().gen_range(0..=base_ms)))
}

fn bot_id(row: &WebhookPageRow) -> Result<BotId, String> {
    <[u8; 32]>::try_from(row.bot_id.as_slice())
        .map(BotId)
        .map_err(|_| "invalid bot_id".to_string())
}

#[allow(clippy::too_many_arguments)]
fn record_attempt(
    store: &Store,
    event_id: &str,
    page: u16,
    attempt: u16,
    at_unix: i64,
    http_status: Option<u16>,
    error: Option<String>,
    next_attempt_unix: Option<i64>,
    terminal: Option<WebhookTerminal>,
) -> Result<(), String> {
    store
        .writer
        .execute(WriterCmd::MarkWebhook {
            event_id: event_id.to_string(),
            mark: WebhookMark::Attempt {
                page,
                attempt,
                at_unix,
                http_status,
                error,
                next_attempt_unix,
                terminal,
            },
        })
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssrf_denied_ips() {
        for ip in [
            "0.0.0.0",
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "100.127.255.255",
            "224.0.0.1",
            "255.255.255.255",
        ] {
            let ip: IpAddr = ip.parse().unwrap();
            assert!(is_denied_ip(ip), "expected denied: {ip}");
        }
        for ip in [
            "::",
            "::1",
            "fc00::1",
            "fd00::1",
            "fe80::1",
            "ff02::1",
            "::ffff:192.168.1.1",
            "64:ff9b::7f00:1",
        ] {
            let ip: IpAddr = ip.parse().unwrap();
            assert!(is_denied_ip(ip), "expected denied: {ip}");
        }
        for ip in ["8.8.8.8", "1.1.1.1", "93.184.216.34"] {
            let ip: IpAddr = ip.parse().unwrap();
            assert!(!is_denied_ip(ip), "expected allowed: {ip}");
        }
        // IPv4-mapped loopback decoded via to_ipv4_mapped.
        assert!(is_denied_ip("::ffff:127.0.0.1".parse().unwrap()));
        // Public addresses carried in mapped form must be evaluated as IPv4.
        assert!(!is_denied_ip("::ffff:8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_hmac_signing_hex() {
        let sig1 = hmac_hex(b"test-secret", b"test-body");
        assert_eq!(sig1.len(), 64);
        assert!(sig1.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(sig1, hmac_hex(b"test-secret", b"test-body"));
        assert_ne!(sig1, hmac_hex(b"test-secret", b"other"));
        assert_ne!(sig1, hmac_hex(b"other-secret", b"test-body"));
        assert_ne!(sig1, hmac_hex(b"", b"test-body"));
    }

    #[test]
    fn test_page_body_format() {
        let rows = vec![
            (Some("123".to_string()), Some(456), 1_234_567_890i64),
            (Some("-100123".to_string()), None, 1_234_567_891i64),
        ];
        let pages = page_bodies(
            "job1",
            "job1-event",
            "completed",
            10,
            2,
            8,
            0,
            rows,
            MAX_WEBHOOK_PAGE_BYTES,
        );
        assert_eq!(pages.len(), 1);
        let v: Value = serde_json::from_slice(&pages[0]).unwrap();
        assert_eq!(v["job_id"], "job1");
        assert_eq!(v["event_id"], "job1-event");
        assert_eq!(v["state"], "completed");
        assert_eq!(v["page"], 0);
        assert_eq!(v["page_count"], 1);
        assert_eq!(v["results"].as_array().unwrap().len(), 2);
        assert_eq!(v["results"][0], json!(["123", 456, 1234567890]));
        assert_eq!(v["results"][1], json!(["-100123", null, 1234567891]));
    }

    #[test]
    fn test_empty_successes_yields_one_page() {
        let pages = page_bodies("j", "e", "completed", 5, 0, 5, 0, vec![], 1024);
        assert_eq!(pages.len(), 1);
        let v: Value = serde_json::from_slice(&pages[0]).unwrap();
        assert_eq!(v["results"].as_array().unwrap().len(), 0);
        assert_eq!(v["total"], 5);
    }

    #[test]
    fn test_100k_successes_page_bounded_and_in_order() {
        // Emulates ~100k delivered successes split into ≤256 KiB pages.
        let rows = (0u64..100_000).map(|i| {
            let chat = Some(format!("{i}"));
            let msg = Some((i % 1_000_000) as i64);
            let ts = 1_700_000_000i64 + (i % 1000) as i64;
            (chat, msg, ts)
        });
        let pages = page_bodies(
            "job-x",
            "job-x-event",
            "completed",
            100_000,
            100_000,
            0,
            0,
            rows,
            MAX_WEBHOOK_PAGE_BYTES,
        );
        assert!(pages.len() > 1, "100k successes must span multiple pages");
        assert_eq!(
            serde_json::from_slice::<Value>(&pages[0]).unwrap()["page_count"]
                .as_u64()
                .unwrap() as usize,
            pages.len()
        );
        let mut last_page: i64 = -1;
        for (i, body) in pages.iter().enumerate() {
            assert!(
                body.len() <= MAX_WEBHOOK_PAGE_BYTES,
                "page {} exceeded ceiling: {} bytes",
                i,
                body.len()
            );
            let v: Value = serde_json::from_slice(body).unwrap();
            let page_no = v["page"].as_i64().unwrap();
            assert_eq!(page_no, i as i64, "pages must be contiguous in order");
            assert!(page_no > last_page);
            last_page = page_no;
            let results = v["results"].as_array().unwrap();
            assert!(!results.is_empty());
            for tuple in results {
                assert_eq!(tuple.as_array().unwrap().len(), 3);
            }
        }
        let total_rows: usize = pages
            .iter()
            .map(|b| {
                serde_json::from_slice::<Value>(b).unwrap()["results"]
                    .as_array()
                    .unwrap()
                    .len()
            })
            .sum();
        assert_eq!(total_rows, 100_000);
    }

    #[test]
    fn test_denied_names() {
        for n in [
            "localhost",
            "LOCALHOST.",
            "localhost.localdomain",
            "metadata",
            "metadata.google.internal",
            "metadata.google.com",
            "instance-data",
            "169.254.169.254",
            "x.internal",
            "foo.localhost",
            "svc.local",
            "2130706433",
            "0x7f000001",
            "127.0.0.1",
            "::1",
        ] {
            assert!(denied_name(n), "expected denied: {n}");
        }
        for n in [
            "example.com",
            "api.telegram.org",
            "webhook.example.net",
            "8.8.8.8",
            "2001:4860:4860::8888",
        ] {
            assert!(!denied_name(n), "expected allowed: {n}");
        }
    }

    #[test]
    fn test_policy_defaults_deny_http() {
        let p = WebhookSsrPolicy::default();
        assert!(!p.is_trusted("example.com"));
        let u = Url::parse("http://example.com/hook").unwrap();
        assert_eq!(u.scheme(), "http");
        // http is only allowed when allow_insecure_http && trusted
        assert!(!(p.allow_insecure_http && p.is_trusted("example.com")));
    }
}
