use crate::{
    api_model::{parse_envelope, EnvelopeError},
    auth::{KeyRing, Purpose},
    domain::BotId,
    http::resolve_api_url,
    store::{FileRow, RecipientInsert, Store, StoreError, WriterCmd},
};
use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use http_body_util::BodyExt;
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::Digest;
use std::{
    collections::HashMap,
    io::{BufReader, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<Store>,
    pub keys: Arc<KeyRing>,
    pub telegram_base: String,
    pub client: reqwest::Client,
    pub telegram: Arc<Semaphore>,
    pub large_body: Arc<Semaphore>,
    pub large_ingest: Arc<Semaphore>,
    pub body_budget: Arc<Semaphore>,
    pub max_body: usize,
    pub max_multipart_body: usize,
    pub data_root: PathBuf,
    pub max_recipients: usize,
    pub shared_limit: usize,
    pub patch_limit: usize,
    pub negative_401: Arc<Mutex<HashMap<BotId, i64>>>,
    /// SQLite database path (with its `-wal`) used by the storage
    /// high-watermark admission check.
    pub database_path: PathBuf,
    /// Upper bound on concurrent inbound HTTP requests handled by this process
    /// (`max_concurrent_http`); overflow returns 503.
    pub http_concurrency: Arc<Semaphore>,
    pub free_disk_reserve_bytes: u64,
    pub storage_high_watermark_bytes: u64,
    /// Global ceiling on non-terminal recipients, enforced atomically at
    /// `PromoteJob` (`global_nonterminal_recipients`).
    pub nonterminal_cap: u32,
}
#[derive(Debug, Serialize)]
struct ErrorBody {
    ok: bool,
    error_code: u16,
    description: String,
}
/// A request body that makes no progress for this long is aborted so a stalled
/// tenant cannot hold the (single) large-ingest permit indefinitely.
const SPOOL_READ_IDLE: Duration = Duration::from_secs(20);
/// Hard upper bound on spooling one request body, closing the trickle hole a
/// client could otherwise keep open by sending a frame just often enough to
/// defeat the idle timeout.
const SPOOL_READ_TOTAL: Duration = Duration::from_secs(120);

/// Upper bound on distinct invalid tokens cached with a negative 401. Past
/// this the cache prunes expired entries and evicts the soonest to expire, so
/// a flood of distinct invalid tokens cannot grow the map without limit.
const NEGATIVE_401_CAP: usize = 4096;

fn request_timeout() -> Response {
    error(StatusCode::REQUEST_TIMEOUT, "request body read timed out")
}

fn error(status: StatusCode, msg: impl Into<String>) -> Response {
    (
        status,
        axum::Json(ErrorBody {
            ok: false,
            error_code: status.as_u16(),
            description: msg.into(),
        }),
    )
        .into_response()
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
/// Remember that `token`'s derived id is not a known bot until `until_unix`,
/// keeping the cache bounded under a flood of distinct invalid tokens.
fn negative_cache_remember(s: &ApiState, id: BotId, until_unix: i64) {
    let mut cache = s.negative_401.lock().unwrap();
    if cache.len() >= NEGATIVE_401_CAP {
        let now_unix = now();
        cache.retain(|_, until| *until > now_unix);
        if cache.len() >= NEGATIVE_401_CAP {
            // Still at capacity with live entries: evict the soonest to expire.
            if let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, until)| **until)
                .map(|(k, _)| *k)
            {
                cache.remove(&oldest);
            }
        }
    }
    cache.insert(id, until_unix);
}
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/bot{token}/getBulkDeliveryConfig", get(get_config))
        .route("/bot{token}/setBulkDeliveryConfig", post(set_config))
        .route("/bot{token}/resetBulkDeliveryConfig", post(reset_config))
        .route("/bot{token}/bulk/jobs/{job}", get(status))
        .route("/bot{token}/bulk/jobs/{job}/results", get(results))
        .route("/bot{token}/{method}", post(submit))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            http_concurrency_guard,
        ))
        .with_state(state)
}

/// `max_concurrent_http` ceiling: reject with 503 (overloaded) once the
/// configured number of requests are already being handled. Liveness endpoints
/// live on the separate health router and stay reachable under overload.
async fn http_concurrency_guard(
    State(s): State<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    // /metrics stays reachable under overload: observability matters most
    // exactly when every HTTP permit is held by large uploads. Liveness lives
    // on the separate health router and is never guarded.
    if request.uri().path() == "/metrics" {
        return next.run(request).await;
    }
    let Ok(permit) = s.http_concurrency.clone().try_acquire_owned() else {
        return overloaded();
    };
    let response = next.run(request).await;
    drop(permit);
    response
}

/// Operator metrics in Prometheus text format. Exposes only aggregate counters
/// and gauges — never tokens, chat ids, bodies, or other high-cardinality data.
/// Bind on localhost (default) or gate behind an operator proxy.
async fn metrics(State(s): State<ApiState>) -> Response {
    let mut response = s.store.metrics.render().into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
    );
    response
}
async fn authenticate(s: &ApiState, token: &str) -> Result<BotId, Response> {
    let id = s.keys.bot_id(token.as_bytes());
    if s.store
        .readers
        .bot(id)
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?
        .is_some()
    {
        return Ok(id);
    }
    if s.negative_401
        .lock()
        .unwrap()
        .get(&id)
        .is_some_and(|until| *until > now())
    {
        return Err(error(StatusCode::UNAUTHORIZED, "invalid bot token"));
    }
    let _permit = s
        .telegram
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "telegram unavailable"))?;
    // getMe is only reached for an unknown bot (no `bots` row), which by the
    // bots -> bot_configs FK can have no config row yet, so there is no
    // per-bot base to resolve here: always global. A config-claimed bot
    // registers via the claim path and never needs this getMe fallback.
    let url = resolve_api_url(&s.telegram_base, None, token, "getMe");
    let response = s
        .client
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "token validation unavailable",
            )
        })?;
    if response.status() == StatusCode::UNAUTHORIZED {
        negative_cache_remember(s, id, now() + 60);
        s.store.metrics.record_token_validation_failure();
        return Err(error(StatusCode::UNAUTHORIZED, "invalid bot token"));
    }
    if !response.status().is_success() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "token validation unavailable",
        ));
    }
    let value: Value = response.json().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "invalid token validation response",
        )
    })?;
    if !value.get("ok").and_then(Value::as_bool).unwrap_or(false) {
        s.store.metrics.record_token_validation_failure();
        return Err(error(StatusCode::UNAUTHORIZED, "invalid bot token"));
    }
    let user = value.pointer("/result/id").and_then(Value::as_i64);
    let enc = s
        .keys
        .encrypt(id, Purpose::BotToken, token.as_bytes())
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    s.store
        .writer
        .execute(WriterCmd::UpsertBot {
            bot_id: id,
            token_nonce: enc.nonce,
            token_ciphertext: enc.ciphertext,
            token_kid: enc.key_id,
            telegram_user_id: user,
            now_unix: now(),
        })
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    Ok(id)
}
fn defaults() -> Value {
    json!({"target_msgs_per_sec":20.0,"retry_max_attempts":8,"retry_base_ms":500,"retry_max_ms":60000,"retry_jitter":"full","retryable_classes":["flood","transient"],"ambiguity_policy":"at_least_once","job_deadline_secs":86400,"fairness_weight":1,"completion_webhook_url":null,"webhook_secret_configured":false,"webhook_max_attempts":10,"webhook_retry_base_ms":1000,"webhook_retry_max_ms":300000,"telegram_api_base":null})
}
#[allow(clippy::result_large_err)]
fn current(s: &ApiState, id: BotId) -> Result<Value, Response> {
    Ok(
        match s
            .store
            .readers
            .config(id)
            .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?
        {
            Some(v) => serde_json::from_str(&v)
                .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
            None => defaults(),
        },
    )
}
async fn get_config(State(s): State<ApiState>, Path(token): Path<String>) -> Response {
    let id = match authenticate(&s, &token).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    match current(&s, id) {
        Ok(v) => axum::Json(json!({"ok":true,"result":v})).into_response(),
        Err(e) => e,
    }
}
async fn body_bytes(mut body: Body, max: usize) -> Result<Bytes, Response> {
    // Same idle/total read bounds as `spool_body`: a stalled request body here
    // (setBulkDeliveryConfig) must not pin an `http_concurrency` permit for
    // every other tenant.
    let deadline = tokio::time::Instant::now() + SPOOL_READ_TOTAL;
    let mut out = Vec::new();
    loop {
        let frame = match tokio::time::timeout(SPOOL_READ_IDLE, body.frame()).await {
            Ok(f) => f,
            Err(_) => return Err(request_timeout()),
        };
        let Some(frame) = frame else { break };
        if tokio::time::Instant::now() > deadline {
            return Err(request_timeout());
        }
        let frame = frame.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid body"))?;
        if let Ok(data) = frame.into_data() {
            if out.len() + data.len() > max {
                return Err(error(StatusCode::PAYLOAD_TOO_LARGE, "payload too large"));
            }
            out.extend_from_slice(&data)
        }
    }
    Ok(Bytes::from(out))
}
fn clamp(mut v: Value) -> Value {
    let d = defaults();
    let o = v.as_object_mut().unwrap();
    for (k, x) in d.as_object().unwrap() {
        o.entry(k.clone()).or_insert(x.clone());
    }
    fn n(o: &mut Map<String, Value>, k: &str, min: i64, max: i64) {
        if let Some(x) = o.get(k).and_then(Value::as_i64) {
            o.insert(k.into(), json!(x.clamp(min, max)));
        }
    }
    n(o, "retry_max_attempts", 0, 16);
    n(o, "retry_base_ms", 1, 5000);
    n(o, "retry_max_ms", 1, 300000);
    n(o, "job_deadline_secs", 1, 172800);
    n(o, "fairness_weight", 1, 16);
    n(o, "webhook_max_attempts", 1, 20);
    n(o, "webhook_retry_base_ms", 1, 10000);
    n(o, "webhook_retry_max_ms", 1, 900000);
    if let Some(x) = o.get("target_msgs_per_sec").and_then(Value::as_f64) {
        o.insert("target_msgs_per_sec".into(), json!(x.clamp(0.1, 25.0)));
    }
    // telegram_api_base: nullable string. Coerce garbage to null; strings must
    // be trimmed and actually http(s):// (or a {token}-containing http(s) base).
    // Anything else is treated as unset (global fallback). No reachability check.
    match o.get("telegram_api_base") {
        None | Some(Value::Null) => {}
        Some(Value::String(_)) => {
            let trimmed = o
                .get("telegram_api_base")
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or("");
            let keep = trimmed.starts_with("http://") || trimmed.starts_with("https://");
            if keep {
                o.insert("telegram_api_base".into(), json!(trimmed));
            } else {
                o.insert("telegram_api_base".into(), Value::Null);
            }
        }
        Some(_) => {
            o.insert("telegram_api_base".into(), Value::Null);
        }
    }
    o.insert(
        "retryable_classes_json".into(),
        Value::String(
            serde_json::to_string(
                o.get("retryable_classes")
                    .unwrap_or(&json!(["flood", "transient"])),
            )
            .unwrap(),
        ),
    );
    v
}
// Shared tail for both claim and non-claim paths: merge patch -> clamp ->
// webhook-secret -> UpsertConfig -> response.
async fn finish_set_config(
    s: &ApiState,
    id: BotId,
    patch: Value,
    mut base_config: Value,
) -> Response {
    if let (Value::Object(base), Value::Object(p)) = (&mut base_config, patch) {
        base.extend(p)
    }
    let mut config = clamp(base_config);
    let mut secret = None;
    let mut encrypted_secret = None;
    if config
        .get("completion_webhook_url")
        .is_some_and(|v| !v.is_null())
        && config
            .get("webhook_secret_configured")
            .and_then(Value::as_bool)
            != Some(true)
    {
        let mut b = [0u8; 32];
        OsRng.fill_bytes(&mut b);
        secret = Some(base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            b,
        ));
        match s.keys.encrypt(id, Purpose::WebhookSecret, &b) {
            Ok(value) => encrypted_secret = Some((value.nonce, value.ciphertext, value.key_id)),
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
        config["webhook_secret_configured"] = json!(true)
    }
    if let Err(e) = s.store.writer.execute(WriterCmd::UpsertConfig {
        bot_id: id,
        config_json: config.clone(),
        webhook_secret: encrypted_secret,
        updated_at_unix: now(),
    }) {
        return error(StatusCode::SERVICE_UNAVAILABLE, e.to_string());
    }
    if let Some(x) = secret {
        config["webhook_secret"] = json!(x)
    }
    axum::Json(json!({"ok":true,"result":config})).into_response()
}

async fn set_config(
    State(s): State<ApiState>,
    Path(token): Path<String>,
    req: Request,
) -> Response {
    // 1) Derive BotId first (cheap, no I/O, no network).
    let id = s.keys.bot_id(token.as_bytes());

    // 2) Known-bot fast path: read readers.bot once and reuse `is_known` in the
    //    non-claim branch below so known bots skip authenticate()'s second
    //    readers.bot read and its getMe semaphore (meant for outbound Telegram).
    let is_known = match s.store.readers.bot(id) {
        Ok(v) => v.is_some(),
        Err(e) => return error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    };

    // 3) Read the patch body ONCE before any branch — both paths need it.
    let bytes = match body_bytes(req.into_body(), s.max_body.min(256 * 1024)).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let patch: Value = match serde_json::from_slice(&bytes) {
        Ok(Value::Object(o)) => Value::Object(o),
        _ => return error(StatusCode::BAD_REQUEST, "config must be an object"),
    };

    // CLAIM GATE: only an http(s):// -prefixed NON-EMPTY STRING may claim.
    // JSON null, "", whitespace, or any non-http(s) value (e.g. "ftp://x",
    // numbers, objects) MUST NOT claim — those fall through to
    // authenticate()/getMe exactly as today, so the register-without-getMe
    // surface stays as narrow as the feature allows.
    let patch_has_base = patch
        .get("telegram_api_base")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| v.starts_with("http://") || v.starts_with("https://"))
        .is_some();

    if !is_known && patch_has_base {
        // CLAIM PATH — register without getMe.
        // No s.telegram semaphore, no reqwest call, no negative-cache write,
        // no token-validation-failure metric.
        // Encrypt the token exactly as authenticate does, then UpsertBot with
        // telegram_user_id = None (unknown until a future getMe if ever).
        let enc = match s.keys.encrypt(id, Purpose::BotToken, token.as_bytes()) {
            Ok(v) => v,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        if let Err(e) = s.store.writer.execute(WriterCmd::UpsertBot {
            bot_id: id,
            token_nonce: enc.nonce,
            token_ciphertext: enc.ciphertext,
            token_kid: enc.key_id,
            telegram_user_id: None,
            now_unix: now(),
        }) {
            return error(StatusCode::SERVICE_UNAVAILABLE, e.to_string());
        }

        // Proceed through the shared config-merge/clamp/UpsertConfig tail.
        // base_config is defaults() because no config row exists yet.
        return finish_set_config(&s, id, patch, defaults()).await;
    }

    // NON-CLAIM PATH: known bots skip authenticate (their bot_id hash is already
    // valid); unknown bots keep the full authenticate/getMe semantics
    // (negative-cache, metrics, UpsertBot with telegram_user_id=Some).
    let id = if is_known {
        id
    } else {
        match authenticate(&s, &token).await {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let base_config = match current(&s, id) {
        Ok(v) => v,
        Err(e) => return e,
    };
    finish_set_config(&s, id, patch, base_config).await
}
async fn reset_config(State(s): State<ApiState>, Path(token): Path<String>) -> Response {
    let id = match authenticate(&s, &token).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let c = clamp(defaults());
    if let Err(e) = s.store.writer.execute(WriterCmd::UpsertConfig {
        bot_id: id,
        config_json: c.clone(),
        webhook_secret: None,
        updated_at_unix: now(),
    }) {
        return error(StatusCode::SERVICE_UNAVAILABLE, e.to_string());
    }
    axum::Json(json!({"ok":true,"result":c})).into_response()
}
fn chat(v: &Map<String, Value>) -> (Option<String>, Option<String>) {
    let Some(x) = v.get("chat_id") else {
        return (None, Some("unknown".into()));
    };
    let text = match x {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return (None, Some("unknown".into())),
    };
    // Telegram chat identifiers are numeric IDs or short @usernames; do not
    // let arbitrary patch text become an unbounded in-memory limiter key.
    if text.len() > 256 {
        return (None, Some("unknown".into()));
    }
    let kind = text
        .parse::<i64>()
        .ok()
        .map(|n| {
            if n > 0 {
                "private"
            } else if text.starts_with("-100") {
                "supergroup"
            } else {
                "group"
            }
        })
        .unwrap_or("unknown");
    (Some(text), Some(kind.into()))
}
async fn spool_body(
    s: &ApiState,
    mut body: Body,
    path: &std::path::Path,
    max: usize,
) -> Result<(u64, Vec<OwnedSemaphorePermit>), Response> {
    let mut file = std::fs::File::create(path)
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mut held = Vec::new();
    let mut total = 0u64;
    let deadline = tokio::time::Instant::now() + SPOOL_READ_TOTAL;
    loop {
        // Per-frame idle timeout plus a whole-body deadline: a tenant that
        // stalls (or trickles a frame only often enough to beat the idle
        // timeout) cannot hold the single large-ingest permit indefinitely.
        let frame = match tokio::time::timeout(SPOOL_READ_IDLE, body.frame()).await {
            Ok(f) => f,
            Err(_) => return Err(request_timeout()),
        };
        let Some(frame) = frame else { break };
        if tokio::time::Instant::now() > deadline {
            return Err(request_timeout());
        }
        let frame = frame.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid body"))?;
        if let Ok(data) = frame.into_data() {
            total += data.len() as u64;
            if total > max as u64 {
                return Err(error(StatusCode::PAYLOAD_TOO_LARGE, "payload too large"));
            }
            let permits = u32::try_from(data.len())
                .map_err(|_| error(StatusCode::PAYLOAD_TOO_LARGE, "payload too large"))?;
            held.push(
                s.body_budget
                    .clone()
                    .try_acquire_many_owned(permits)
                    .map_err(|_| overloaded())?,
            );
            file.write_all(&data)
                .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
    }
    file.sync_data()
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((total, held))
}

#[allow(clippy::result_large_err, clippy::type_complexity)]
fn ingest_payload(
    s: &ApiState,
    id: BotId,
    job: &str,
    reader: impl std::io::Read,
    known_large: bool,
) -> Result<(Map<String, Value>, u32, Vec<OwnedSemaphorePermit>), Response> {
    let mut batch = Vec::with_capacity(1000);
    let mut total = 0u32;
    let mut unknown_permits = Vec::new();
    // The batch flush below runs inside serde's streaming visitor, whose custom
    // error type flattens any discriminant to a message string. A durable write
    // failure (disk full, writer down) must NOT be reported to the client as
    // "payload too large", so it is captured on the side and surfaced after the
    // reader has been drained.
    let mut store_failed: Option<String> = None;
    // An unknown-length request that crosses the 10_000-recipient boundary must
    // acquire the large-ingest permit; when the permit is busy this is an
    // overload (503), NOT a malformed envelope (400). The serde error path
    // cannot carry the distinction, so it is captured on the side.
    let mut large_denied = false;
    let parsed = parse_envelope(
        reader,
        s.shared_limit,
        s.patch_limit,
        s.max_recipients,
        |idx, patch| {
            if idx == 9_999 && !known_large {
                let body_permit = s.large_body.clone().try_acquire_owned().map_err(|_| {
                    large_denied = true;
                    EnvelopeError::TooLarge
                })?;
                let ingest_permit = s.large_ingest.clone().try_acquire_owned().map_err(|_| {
                    large_denied = true;
                    EnvelopeError::TooLarge
                })?;
                unknown_permits.push(body_permit);
                unknown_permits.push(ingest_permit);
            }
            let (chat_id, chat_kind) = chat(&patch);
            batch.push(RecipientInsert {
                idx,
                bot_id: id,
                patch_json: Value::Object(patch).to_string(),
                chat_id,
                chat_kind,
            });
            total = idx + 1;
            if batch.len() == 1000 {
                let rows = std::mem::take(&mut batch);
                // Once a durable write has failed, keep draining the request
                // body but stop issuing writes to the store.
                if store_failed.is_none() {
                    if let Err(e) = s.store.writer.execute(WriterCmd::InsertRecipientBatch {
                        job_id: job.to_owned(),
                        rows,
                    }) {
                        store_failed = Some(e.to_string());
                    }
                }
            }
            Ok(())
        },
    );
    if large_denied {
        return Err(overloaded());
    }
    if let Some(msg) = &store_failed {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, msg.clone()));
    }
    let parameters = parsed.map_err(|e| error(StatusCode::BAD_REQUEST, e.to_string()))?;
    if !batch.is_empty() {
        if let Err(e) = s.store.writer.execute(WriterCmd::InsertRecipientBatch {
            job_id: job.to_owned(),
            rows: batch,
        }) {
            return Err(error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()));
        }
    }
    Ok((parameters, total, unknown_permits))
}

async fn submit(
    State(s): State<ApiState>,
    Path((token, method)): Path<(String, String)>,
    headers: HeaderMap,
    req: Request,
) -> Response {
    let Some(method_spec) = telegram_api_meta::method(&method) else {
        let description = if telegram_api_meta::is_non_bulk(&method) {
            "method_not_bulk_capable"
        } else {
            "unknown bulk method"
        };
        return error(StatusCode::BAD_REQUEST, description);
    };
    let method = method_spec.name.to_owned();
    // Reject before allocating/spooling when the cgroup is already within the
    // protected headroom. `memory.current` includes page cache; RSS is only the
    // fallback on hosts without a readable cgroup controller.
    if crate::observe::admission_memory_pressure() {
        return overloaded();
    }
    // Operator storage ceilings: reserve free disk for SQLite durability and
    // honor the high-watermark before this request allocates or spools.
    if !storage_admission(&s) {
        return overloaded();
    }
    let id = match authenticate(&s, &token).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let multipart = content_type.starts_with("multipart/form-data");
    let known_large = multipart
        || headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .is_some_and(|v| v > 1024 * 1024);
    let _large_body = if known_large {
        match s.large_body.clone().try_acquire_owned() {
            Ok(v) => Some(v),
            Err(_) => return overloaded(),
        }
    } else {
        None
    };
    let _known_ingest = if known_large {
        match s.large_ingest.clone().try_acquire_owned() {
            Ok(v) => Some(v),
            Err(_) => return overloaded(),
        }
    } else {
        None
    };
    let job = uuid::Uuid::now_v7().to_string();
    let config = match current(&s, id) {
        Ok(v) => clamp(v),
        Err(e) => return e,
    };
    let deadline = now()
        + config
            .get("job_deadline_secs")
            .and_then(Value::as_i64)
            .unwrap_or(86400);
    if let Err(e) = s.store.writer.execute(WriterCmd::BeginAcceptJob {
        job_id: job.clone(),
        bot_id: id,
        method: method.clone(),
        policy_snapshot_json: config.to_string(),
        config_version: config
            .get("config_version")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        deadline_unix: Some(deadline),
    }) {
        return error(StatusCode::SERVICE_UNAVAILABLE, e.to_string());
    }
    let tmp_directory = s.data_root.join("tmp").join(&job);
    if let Err(e) = std::fs::create_dir_all(&tmp_directory) {
        let _ = s.store.writer.execute(WriterCmd::AbortAcceptJob {
            job_id: job.clone(),
        });
        return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    }
    let spool = tmp_directory.join("request.part");
    let limit = if multipart {
        s.max_multipart_body
    } else {
        s.max_body
    };
    let (_, _body_permits) = match spool_body(&s, req.into_body(), &spool, limit).await {
        Ok(v) => v,
        Err(e) => {
            let _ = s.store.writer.execute(WriterCmd::AbortAcceptJob {
                job_id: job.clone(),
            });
            let _ = std::fs::remove_dir_all(&tmp_directory);
            let _ = std::fs::remove_dir_all(s.data_root.join("files").join(&job));
            return e;
        }
    };
    let mut file_rows = Vec::new();
    let result = if multipart {
        let boundary = match multer::parse_boundary(&content_type) {
            Ok(v) => v,
            Err(_) => {
                abort_accepting(&s, &job, &tmp_directory);
                return error(StatusCode::BAD_REQUEST, "invalid multipart boundary");
            }
        };
        let source = match tokio::fs::File::open(&spool).await {
            Ok(v) => v,
            Err(e) => {
                abort_accepting(&s, &job, &tmp_directory);
                return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
        };
        let stream = tokio_util::io::ReaderStream::new(source);
        let mut form = multer::Multipart::new(stream, boundary);
        let mut payload_path = None;
        'fields: loop {
            let field = match form.next_field().await {
                Ok(Some(f)) => f,
                Ok(None) => break 'fields,
                // A malformed/truncated body must not silently truncate the last
                // file and persist a short sha256/size as durable metadata.
                Err(e) => {
                    abort_accepting(&s, &job, &tmp_directory);
                    return error(
                        StatusCode::BAD_REQUEST,
                        format!("malformed multipart body: {e}"),
                    );
                }
            };
            let name = field.name().unwrap_or("").to_owned();
            let filename = field.file_name().map(str::to_owned);
            let content_type = field.content_type().map(ToString::to_string);
            let file_id = uuid::Uuid::now_v7().to_string();
            let part = match crate::store::files::temp_path(&s.data_root, &job, &file_id) {
                Ok(v) => v,
                Err(e) => {
                    abort_accepting(&s, &job, &tmp_directory);
                    return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
                }
            };
            let mut output = match std::fs::File::create(&part) {
                Ok(v) => v,
                Err(e) => {
                    abort_accepting(&s, &job, &tmp_directory);
                    return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
                }
            };
            let mut digest = sha2::Sha256::new();
            let mut size = 0u64;
            let mut field = field;
            loop {
                match field.chunk().await {
                    Ok(Some(chunk)) => {
                        size += chunk.len() as u64;
                        use sha2::Digest;
                        digest.update(&chunk);
                        if let Err(e) = output.write_all(&chunk) {
                            abort_accepting(&s, &job, &tmp_directory);
                            return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        abort_accepting(&s, &job, &tmp_directory);
                        return error(
                            StatusCode::BAD_REQUEST,
                            format!("truncated multipart body: {e}"),
                        );
                    }
                }
            }
            if name == "payload_json" {
                payload_path = Some(part);
            } else {
                let hash: [u8; 32] = digest.finalize().into();
                let durable = match crate::store::files::finalize(
                    &s.data_root,
                    &job,
                    &file_id,
                    &part,
                    size,
                    hash,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        abort_accepting(&s, &job, &tmp_directory);
                        return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
                    }
                };
                file_rows.push(FileRow {
                    file_id,
                    field_name: name.clone(),
                    attach_name: Some(name),
                    content_type,
                    original_filename: filename,
                    sha256: durable.sha256,
                    size_bytes: durable.size,
                    blob_path: durable.path.to_string_lossy().into_owned(),
                    retain_until_unix: deadline + 604800,
                });
            }
        }
        let payload = match payload_path {
            Some(v) => v,
            None => {
                abort_accepting(&s, &job, &tmp_directory);
                return error(StatusCode::BAD_REQUEST, "payload_json is required");
            }
        };
        let payload_file = match std::fs::File::open(&payload) {
            Ok(f) => f,
            Err(e) => {
                abort_accepting(&s, &job, &tmp_directory);
                return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
        };
        ingest_payload(&s, id, &job, BufReader::new(payload_file), true)
    } else {
        let spool_file = match std::fs::File::open(&spool) {
            Ok(f) => f,
            Err(e) => {
                abort_accepting(&s, &job, &tmp_directory);
                return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
            }
        };
        ingest_payload(&s, id, &job, BufReader::new(spool_file), known_large)
    };
    let (parameters, total, _unknown) = match result {
        Ok(v) => v,
        Err(e) => {
            let _ = s.store.writer.execute(WriterCmd::AbortAcceptJob {
                job_id: job.clone(),
            });
            let _ = std::fs::remove_dir_all(&tmp_directory);
            let _ = std::fs::remove_dir_all(s.data_root.join("files").join(&job));
            return e;
        }
    };
    let accepted = now();
    if let Err(e) = s.store.writer.execute(WriterCmd::PromoteJob {
        job_id: job.clone(),
        total,
        shared_params_json: Value::Object(parameters).to_string(),
        accepted_at_unix: accepted,
        file_rows,
        nonterminal_cap: Some(s.nonterminal_cap),
    }) {
        let _ = s.store.writer.execute(WriterCmd::AbortAcceptJob {
            job_id: job.clone(),
        });
        // AbortAcceptJob cascades the accepting row's staged recipients; the
        // spooled body and any finalized multipart blobs live under tmp/<job>
        // and files/<job> and must be removed here too (the boot sweep would
        // collect them, but leaving up to multipart_body_bytes per refusal is
        // exactly what the storage ceilings are meant to prevent).
        let _ = std::fs::remove_dir_all(&tmp_directory);
        let _ = std::fs::remove_dir_all(s.data_root.join("files").join(&job));
        if matches!(e, StoreError::CapacityExceeded(_)) {
            return overloaded();
        }
        return error(StatusCode::SERVICE_UNAVAILABLE, e.to_string());
    }
    s.store.metrics.record_ingest_accepted(u64::from(total));
    let _ = std::fs::remove_file(spool);
    let _ = std::fs::remove_dir(tmp_directory);
    axum::Json(json!({"ok":true,"result":{"job_id":job,"state":"queued","total":total,"accepted_at_unix":accepted,"status_url":format!("/bot{token}/bulk/jobs/{job}")}})).into_response()
}
/// Best-effort cleanup for an ingest that began accepting but must now be
/// abandoned: drop the accepting job row and remove its spooled/blobs dirs so
/// no orphaned accepting job or unreferenced file survives (the boot sweep
/// would collect them, but cleaning eagerly keeps the tree tidy).
fn abort_accepting(s: &ApiState, job: &str, tmp_directory: &std::path::Path) {
    let _ = s.store.writer.execute(WriterCmd::AbortAcceptJob {
        job_id: job.to_owned(),
    });
    let _ = std::fs::remove_dir_all(tmp_directory);
    let _ = std::fs::remove_dir_all(s.data_root.join("files").join(job));
}

fn overloaded() -> Response {
    let mut r = error(StatusCode::SERVICE_UNAVAILABLE, "overloaded");
    r.headers_mut()
        .insert(header::RETRY_AFTER, "30".parse().unwrap());
    r
}

/// Operator storage admission (US-006 AC3): true when a submit may proceed.
/// Two independent ceilings from `config/operator.defaults.toml` are honored.
/// The free-disk reserve (`free_disk_reserve_bytes`) keeps SQLite FULL-sync
/// headroom and is checked via statvfs on the data directory's filesystem; the
/// storage high-watermark (`storage_high_watermark_bytes`) is the operator
/// budget for this store (SQLite main DB + WAL + durable multipart blobs). Any
/// stat/query failure is treated as admissible so an odd sandbox never
/// hard-blocks legitimate submits: the free-reserve stat is the anti-ENOSPC
/// backstop and the high-watermark is the growth tripwire.
fn storage_admission(s: &ApiState) -> bool {
    #[cfg(unix)]
    {
        let Ok(cpath) = std::ffi::CString::new(s.data_root.to_string_lossy().as_bytes()) else {
            return true;
        };
        let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(cpath.as_ptr(), &mut st) } != 0 {
            return true;
        }
        let frsize = st.f_frsize;
        let avail = st.f_bavail * frsize;
        if avail < s.free_disk_reserve_bytes {
            return false;
        }
    }
    let db = std::fs::metadata(&s.database_path)
        .map(|m| m.len())
        .unwrap_or(0);
    let wal = std::fs::metadata(format!("{}-wal", s.database_path.display()))
        .map(|m| m.len())
        .unwrap_or(0);
    let blobs: i64 = s.store.readers.blob_bytes().unwrap_or(0);
    let used = db + wal + u64::try_from(blobs.max(0)).unwrap_or(0);
    used < s.storage_high_watermark_bytes
}
#[derive(Deserialize)]
struct Page {
    cursor: Option<u32>,
    limit: Option<u16>,
}
async fn status(State(s): State<ApiState>, Path((token, job)): Path<(String, String)>) -> Response {
    let id = match authenticate(&s, &token).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    match s.store.readers.job(id, job.clone()) {
        Ok(Some(j)) => {
            let terminal = j.succeeded + j.failed + j.ambiguous;
            let percent = if j.total == 0 {
                0.0
            } else {
                100.0 * terminal as f64 / j.total as f64
            };
            axum::Json(json!({"ok":true,"result":{"job_id":j.job_id,"method":j.method,"state":j.state,"accepted_at_unix":j.accepted_at_unix,"started_at_unix":j.started_at_unix,"completed_at_unix":j.completed_at_unix,"expire_at_unix":j.expire_at_unix,"deadline_unix":j.deadline_unix,"total":j.total,"queued":j.queued,"in_flight":j.inflight,"delayed":j.delayed,"succeeded":j.succeeded,"failed":j.failed,"ambiguous":j.ambiguous,"percent_complete":percent,"rate":{"instant_per_sec":j.rate_5s,"window_per_sec":j.rate_30s,"window_secs":30},"eta":if j.rate_30s>=0.05 { json!({"seconds":((j.total-terminal) as f64/j.rate_30s).ceil() as u64,"reason":null}) } else { json!({"seconds":null,"reason":if j.started_at_unix.is_none(){"not_started"}else{"insufficient_samples"}}) },"webhook_state":j.webhook_state,"results_url":format!("/bot{token}/bulk/jobs/{job}/results")}})).into_response()
        }
        Ok(None) => error(StatusCode::NOT_FOUND, "job not found"),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
}
async fn results(
    State(s): State<ApiState>,
    Path((token, job)): Path<(String, String)>,
    Query(q): Query<Page>,
) -> Response {
    let id = match authenticate(&s, &token).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let cursor = q.cursor.unwrap_or(0);
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    match s.store.readers.results(id, job, cursor, limit) {
        Ok(items) => {
            let next = items.last().map(|x| x.idx + 1);
            axum::Json(json!({"ok":true,"result":{"items":items,"next_cursor":next}}))
                .into_response()
        }
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
}
