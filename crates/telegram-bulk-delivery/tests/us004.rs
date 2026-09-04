//! US-004: durable signed completion webhooks.
//!
//! Exercises the full writer -> outbox -> deliverer -> receiver path:
//!   * atomic page creation at job terminalization,
//!   * ≤256 KiB in-order paged delivery with HMAC-SHA256 over exact body bytes,
//!   * X-Bulk-* headers incl. the idempotency key,
//!   * per-page retry accounting (retry-then-success and exhaustion),
//!   * the SSRF matrix (scheme, denied names, denied literals, post-resolve deny).

use axum::{body::Bytes, extract::State, http::HeaderMap, routing::post, Router};
use serde_json::{json, Value};
use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    time::SystemTime,
};
use telegram_bulk_delivery::{
    auth::{KeyRing, Purpose},
    domain::BotId,
    store::{CompletionOutcome, RecipientInsert, Store, WriterCmd, WriterResult},
    webhook::{
        hmac_hex, sha256_hex, HostResolver, ResolveResult, WebhookDeliverer, WebhookSsrPolicy,
        MAX_WEBHOOK_PAGE_BYTES,
    },
};
use tempfile::TempDir;

const MASTER: [u8; 32] = [9u8; 32];
const KEY_ID: &str = "k1";
const SECRET: &[u8] = b"webhook-test-secret-0123456789abcdef";

fn now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[derive(Clone)]
struct Sink {
    caps: Arc<Mutex<Vec<Capture>>>,
    mode: Arc<Mutex<Mode>>,
}
#[derive(Clone)]
struct Capture {
    headers: HeaderMap,
    body: Vec<u8>,
}
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    OkAll,
    FailFirstThenOk,
    Always500,
}

impl Sink {
    fn new(mode: Mode) -> Self {
        Self {
            caps: Arc::new(Mutex::new(Vec::new())),
            mode: Arc::new(Mutex::new(mode)),
        }
    }
    fn count(&self) -> usize {
        self.caps.lock().unwrap().len()
    }
    fn captures(&self) -> Vec<Capture> {
        self.caps.lock().unwrap().clone()
    }
    async fn serve(self) -> (tokio::task::JoinHandle<()>, SocketAddr) {
        let app = Router::new()
            .route("/hook", post(Self::receive))
            .with_state(self);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (handle, addr)
    }
    async fn receive(
        State(me): State<Sink>,
        headers: HeaderMap,
        body: Bytes,
    ) -> axum::http::StatusCode {
        let caps = me.caps.clone();
        let m = *me.mode.lock().unwrap();
        caps.lock().unwrap().push(Capture {
            headers,
            body: body.to_vec(),
        });
        let n = caps.lock().unwrap().len();
        match m {
            Mode::OkAll => axum::http::StatusCode::OK,
            Mode::FailFirstThenOk => {
                if n <= 1 {
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    axum::http::StatusCode::OK
                }
            }
            Mode::Always500 => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

fn config_json(url: &str, max_attempts: i64, base_ms: i64, max_ms: i64) -> Value {
    json!({
        "target_msgs_per_sec": 20.0,
        "retry_max_attempts": 8,
        "retry_base_ms": 500,
        "retry_max_ms": 60000,
        "retryable_classes_json": "[\"flood\",\"transient\"]",
        "ambiguity_policy": "at_least_once",
        "job_deadline_secs": 86400,
        "fairness_weight": 1,
        "completion_webhook_url": url,
        "webhook_max_attempts": max_attempts,
        "webhook_retry_base_ms": base_ms,
        "webhook_retry_max_ms": max_ms,
    })
}

fn chat(i: u32, len: usize) -> String {
    let head = format!("{i:0>8}");
    let mut s = head;
    while s.len() < len {
        s.push('x');
    }
    s
}

/// Create a bot + completed job whose recipients all succeeded, producing
/// webhook outbox pages for `webhook_url`. Returns (dir, store, keys, bot,
/// job_id, completion_unix) on a STORE THAT ALREADY HAS THE BOT REGISTERED.
struct Harness {
    _dir: TempDir,
    store: Arc<Store>,
    keys: Arc<KeyRing>,
    bot: BotId,
}

fn open_harness() -> Harness {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(d.path().join("db"), 128, 8).unwrap());
    let keys = Arc::new(KeyRing::derive(&MASTER, KEY_ID).unwrap());
    let bot = BotId([7; 32]);
    store
        .writer
        .execute(WriterCmd::UpsertBot {
            bot_id: bot,
            token_nonce: [0; 12],
            token_ciphertext: vec![1],
            token_kid: KEY_ID.into(),
            telegram_user_id: Some(1),
            now_unix: 1,
        })
        .unwrap();
    Harness {
        _dir: d,
        store,
        keys,
        bot,
    }
}

/// Drive a job with `n` succeeded recipients to completion. Each recipient gets
/// `chat_len`-character chat id so we can force several pages cheaply.
#[allow(clippy::too_many_arguments)]
fn complete_success_job(
    h: &Harness,
    job_id: &str,
    url: &str,
    n: u32,
    chat_len: usize,
    max_attempts: i64,
    base_ms: i64,
    max_ms: i64,
) -> i64 {
    let t0 = now();
    let enc = h
        .keys
        .encrypt(h.bot, Purpose::WebhookSecret, SECRET)
        .unwrap();
    h.store
        .writer
        .execute(WriterCmd::UpsertConfig {
            bot_id: h.bot,
            config_json: config_json(url, max_attempts, base_ms, max_ms),
            webhook_secret: Some((enc.nonce, enc.ciphertext, enc.key_id)),
            updated_at_unix: t0,
        })
        .unwrap();
    h.store
        .writer
        .execute(WriterCmd::BeginAcceptJob {
            job_id: job_id.into(),
            bot_id: h.bot,
            method: "sendMessage".into(),
            policy_snapshot_json: r#"{"retry_max_attempts":8,"ambiguity_policy":"at_least_once","fairness_weight":1,"job_deadline_secs":100}"#
                .into(),
            config_version: 1,
            deadline_unix: Some(t0 + 100_000),
        })
        .unwrap();
    let mut batch = Vec::new();
    for i in 0..n {
        batch.push(RecipientInsert {
            idx: i,
            bot_id: h.bot,
            patch_json: "{}".into(),
            chat_id: Some(chat(i, chat_len)),
            chat_kind: Some("private".into()),
        });
        if batch.len() == 1000 {
            h.store
                .writer
                .execute(WriterCmd::InsertRecipientBatch {
                    job_id: job_id.into(),
                    rows: std::mem::take(&mut batch),
                })
                .unwrap();
        }
    }
    if !batch.is_empty() {
        h.store
            .writer
            .execute(WriterCmd::InsertRecipientBatch {
                job_id: job_id.into(),
                rows: batch,
            })
            .unwrap();
    }
    h.store
        .writer
        .execute(WriterCmd::PromoteJob {
            job_id: job_id.into(),
            total: n,
            shared_params_json: "{}".into(),
            accepted_at_unix: t0,
            file_rows: vec![],
            nonterminal_cap: None,
        })
        .unwrap();
    // Terminalize every recipient as a success through the real lease path.
    let owner = format!("us004:{job_id}");
    let mut done = 0u32;
    while done < n {
        let lease = h
            .store
            .writer
            .execute(WriterCmd::LeaseBatch {
                bot_id: h.bot,
                owner: owner.clone(),
                now_unix: t0,
                lease_secs: 300,
                max: 1,
            })
            .unwrap();
        let (job, idx, lease_token) = match lease {
            WriterResult::Lease(v) => v.expect("expected a leaseable recipient"),
            _ => panic!("unexpected lease result"),
        };
        h.store
            .writer
            .execute(WriterCmd::CompleteAttempt {
                job_id: job,
                idx,
                lease_token,
                outcome: CompletionOutcome::Succeeded {
                    message_id: Some(i64::from(idx) + 1),
                    sent_at_unix: t0,
                },
            })
            .unwrap();
        done += 1;
    }
    h.store
        .writer
        .execute(WriterCmd::Checkpoint {
            mode: telegram_bulk_delivery::store::CheckpointMode::Passive,
        })
        .unwrap();
    t0
}

fn trusted_policy() -> WebhookSsrPolicy {
    WebhookSsrPolicy {
        allow_insecure_http: true,
        trusted_hosts: vec!["127.0.0.1".into()],
    }
}

fn job_webhook_state(h: &Harness, job: &str) -> String {
    h.store
        .readers
        .job(h.bot, job.into())
        .unwrap()
        .expect("job missing")
        .webhook_state
}

/// A no-network fixpoint check: whatever page 0 delivers must match the exact
/// canonical body the writer produced.
fn assert_signature_and_body(caps: &[Capture], job: &str, total: u32, chat_len: usize) {
    assert!(!caps.is_empty());
    let page_count = caps[0]
        .headers
        .get("x-bulk-page-count")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert_eq!(caps.len(), page_count, "every page delivered exactly once");
    let mut seen_pages = Vec::new();
    for c in caps {
        assert!(
            c.body.len() <= MAX_WEBHOOK_PAGE_BYTES,
            "page exceeded 256 KiB: {}",
            c.body.len()
        );
        let page: usize = c
            .headers
            .get("x-bulk-page")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        seen_pages.push(page);
        assert_eq!(
            c.headers.get("x-bulk-event-id").unwrap().to_str().unwrap(),
            format!("{job}-event")
        );
        assert_eq!(
            c.headers.get("x-bulk-job-id").unwrap().to_str().unwrap(),
            job
        );
        assert_eq!(
            c.headers
                .get("x-bulk-page-count")
                .unwrap()
                .to_str()
                .unwrap(),
            page_count.to_string()
        );
        assert_eq!(
            c.headers.get("x-bulk-attempt").unwrap().to_str().unwrap(),
            "1"
        );
        // HMAC over the exact stored bytes.
        let want_sig = format!("v1={}", hmac_hex(SECRET, &c.body));
        assert_eq!(
            c.headers.get("x-bulk-signature").unwrap().to_str().unwrap(),
            want_sig
        );
        // Idempotency key = event_id:page:sha256(body)[..8]
        let key = format!("{job}-event:{page}:{}", &sha256_hex(&c.body)[..8]);
        assert_eq!(
            c.headers
                .get("x-bulk-idempotency-key")
                .unwrap()
                .to_str()
                .unwrap(),
            key
        );
        let v: Value = serde_json::from_slice(&c.body).unwrap();
        assert_eq!(v["job_id"], job);
        assert_eq!(v["event_id"], format!("{job}-event"));
        assert_eq!(v["state"], "completed");
        assert_eq!(v["total"], i64::from(total));
        assert_eq!(v["succeeded"], i64::from(total));
        assert_eq!(v["page"], page as i64);
        assert_eq!(v["page_count"], page_count as i64);
        for tuple in v["results"].as_array().unwrap() {
            let t = tuple.as_array().unwrap();
            assert_eq!(t.len(), 3, "tuple-only results");
            let chat = t[0].as_str().unwrap();
            assert_eq!(chat.len(), chat_len, "chat id carried through intact");
        }
    }
    assert_eq!(seen_pages.len(), page_count);
    // In-order: each page arrived only after its predecessor was delivered.
    let mut sorted = seen_pages.clone();
    sorted.sort_unstable();
    assert_eq!(seen_pages, sorted, "pages must arrive in ascending order");
    assert_eq!(*sorted.first().unwrap(), 0);
    assert_eq!(*sorted.last().unwrap(), page_count - 1);
    let rows: usize = caps
        .iter()
        .map(|c| {
            serde_json::from_slice::<Value>(&c.body).unwrap()["results"]
                .as_array()
                .unwrap()
                .len()
        })
        .sum();
    assert_eq!(rows, total as usize, "all successes paged exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_order_paged_delivery_with_hmac_and_idempotency() {
    let h = open_harness();
    let sink = Sink::new(Mode::OkAll);
    let (server, addr) = sink.clone().serve().await;
    let job = "job-inorder".to_string();
    // 260 successes with 2.6 KiB chat ids force several ≤256 KiB pages.
    let n = 260u32;
    let chat_len = 2600;
    let t0 = complete_success_job(
        &h,
        &job,
        &format!("http://{addr}/hook"),
        n,
        chat_len,
        10,
        1000,
        3000,
    );

    let d = WebhookDeliverer::new(h.store.clone(), h.keys.clone(), 4, trusted_policy()).unwrap();
    let _ = d.pump_at(t0 + 5).await.unwrap();

    let caps = sink.captures();
    assert_signature_and_body(&caps, &job, n, chat_len);
    assert_eq!(job_webhook_state(&h, &job), "delivered");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_500_then_success_accounts_attempts() {
    let h = open_harness();
    let sink = Sink::new(Mode::FailFirstThenOk);
    let (_server, addr) = sink.clone().serve().await;
    let job = "job-retry".to_string();
    let n = 5u32;
    let t0 = complete_success_job(
        &h,
        &job,
        &format!("http://{addr}/hook"),
        n,
        12,
        10,
        1000,
        3000,
    );

    let d = WebhookDeliverer::new(h.store.clone(), h.keys.clone(), 4, trusted_policy()).unwrap();
    let _ = d.pump_at(t0 + 1).await.unwrap(); // attempt 1 -> 500, scheduled for retry
    let _ = d.pump_at(t0 + 4).await.unwrap(); // attempt 2 -> 200

    let caps = sink.captures();
    assert_eq!(caps.len(), 2, "exactly two attempts, no duplicates");
    let attempts: Vec<u8> = caps
        .iter()
        .map(|c| {
            c.headers
                .get("x-bulk-attempt")
                .unwrap()
                .to_str()
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect();
    assert_eq!(attempts, vec![1, 2]);
    for c in &caps {
        assert_eq!(c.headers.get("x-bulk-page").unwrap().to_str().unwrap(), "0");
        let want_sig = format!("v1={}", hmac_hex(SECRET, &c.body));
        assert_eq!(
            c.headers.get("x-bulk-signature").unwrap().to_str().unwrap(),
            want_sig
        );
    }
    assert_eq!(job_webhook_state(&h, &job), "delivered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhaustion_leaves_remaining_pages_unsent_and_marks_job() {
    let h = open_harness();
    let sink = Sink::new(Mode::Always500);
    let (_server, addr) = sink.clone().serve().await;
    let job = "job-exhaust".to_string();
    // 3 pages of success outbox.
    let n = 260u32;
    let chat_len = 2600;
    let t0 = complete_success_job(
        &h,
        &job,
        &format!("http://{addr}/hook"),
        n,
        chat_len,
        2,
        1000,
        3000,
    );

    let d = WebhookDeliverer::new(h.store.clone(), h.keys.clone(), 4, trusted_policy()).unwrap();
    let _ = d.pump_at(t0 + 1).await.unwrap(); // attempt 1 -> 500, retry
    let _ = d.pump_at(t0 + 4).await.unwrap(); // attempt 2 >= max_attempts -> exhausted

    let caps = sink.captures();
    assert_eq!(caps.len(), 2, "only page-0 attempts were made");
    let attempts: Vec<u8> = caps
        .iter()
        .map(|c| {
            c.headers
                .get("x-bulk-attempt")
                .unwrap()
                .to_str()
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect();
    assert_eq!(
        attempts,
        vec![1, 2],
        "two attempts, then the budget is spent"
    );
    for c in &caps {
        assert_eq!(c.headers.get("x-bulk-page").unwrap().to_str().unwrap(), "0");
    }
    assert_eq!(job_webhook_state(&h, &job), "exhausted");
    // Later pumps must stay quiet: the event is terminal and pages 1..N unsent.
    let _ = d.pump_at(t0 + 400).await.unwrap();
    assert_eq!(sink.count(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ssrf_default_policy_rejects_without_any_network() {
    let h = open_harness();
    let sink = Sink::new(Mode::OkAll);
    let (_server, addr) = sink.clone().serve().await;
    let d = WebhookDeliverer::new(
        h.store.clone(),
        h.keys.clone(),
        2,
        WebhookSsrPolicy::default(),
    )
    .unwrap();
    let t0 = now();
    let attacks = [
        format!("http://{addr}/hook"),  // insecure scheme + local
        format!("https://{addr}/hook"), // loopback literal
        "https://169.254.169.254/latest/meta-data/".to_string(),
        "https://metadata.google.internal/computeMetadata/v1/".to_string(),
        "http://10.0.0.5/hook".to_string(),
        "https://2130706433/hook".to_string(), // decimal loopback
        "https://0x7f000001/hook".to_string(), // hex loopback
        "https://[::1]/hook".to_string(),
    ];
    for (i, url) in attacks.iter().enumerate() {
        let job = format!("job-ssrf-{i}");
        complete_success_job(&h, &job, url, 3, 12, 10, 1000, 3000);
        let _ = d.pump_at(t0 + 5 + i as i64).await.unwrap();
        assert_eq!(
            job_webhook_state(&h, &job),
            "exhausted",
            "url {url} must be rejected"
        );
    }
    assert_eq!(sink.count(), 0, "no request may reach the receiver");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebinding_resolver_result_is_denied_after_resolve() {
    let h = open_harness();
    let sink = Sink::new(Mode::OkAll);
    let (_server, _addr) = sink.clone().serve().await;
    // A benign-looking public name that "rebinds" to a loopback address.
    let resolver: Arc<dyn HostResolver> = Arc::new(Fixed(vec!["127.0.0.1".parse().unwrap()]));
    let d = WebhookDeliverer::with_resolver(
        h.store.clone(),
        h.keys.clone(),
        2,
        WebhookSsrPolicy::default(),
        resolver,
    )
    .unwrap();
    let job = "job-rebind".to_string();
    let t0 = complete_success_job(
        &h,
        &job,
        "https://hooks.example.net/hook",
        3,
        12,
        10,
        1000,
        3000,
    );
    let _ = d.pump_at(t0 + 5).await.unwrap();
    assert_eq!(job_webhook_state(&h, &job), "exhausted");
    assert_eq!(sink.count(), 0, "resolved loopback must never be contacted");
}

struct Fixed(Vec<IpAddr>);
impl HostResolver for Fixed {
    fn resolve(
        &self,
        _host: &str,
        _port: u16,
    ) -> Pin<Box<dyn Future<Output = ResolveResult> + Send>> {
        let ips = self.0.clone();
        Box::pin(async move { Ok(ips) })
    }
}
