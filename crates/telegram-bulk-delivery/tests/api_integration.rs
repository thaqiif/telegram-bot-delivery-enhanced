use axum::{extract::Path, response::IntoResponse, routing::get, Json, Router};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use telegram_bulk_delivery::{
    auth::KeyRing,
    http::{api_router, ApiState},
    store::{Store, WriterCmd},
};
use tokio::sync::Semaphore;
use tower::ServiceExt;

struct Fixture {
    app: Router,
    store: Arc<Store>,
    _temp: tempfile::TempDir,
    calls: Arc<Mutex<HashMap<String, usize>>>,
}
async fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(temp.path().join("queue.db"), 128, 64).unwrap());
    let calls = Arc::new(Mutex::new(HashMap::new()));
    let c = calls.clone();
    let upstream = Router::new().route(
        "/bot{token}/getMe",
        get(move |Path(token): Path<String>| {
            let c = c.clone();
            async move {
                *c.lock().unwrap().entry(token.clone()).or_default() += 1;
                if token == "bad" {
                    (
                        axum::http::StatusCode::UNAUTHORIZED,
                        Json(json!({"ok":false})),
                    )
                        .into_response()
                } else {
                    Json(json!({"ok":true,"result":{"id":if token=="one"{1}else{2}}}))
                        .into_response()
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let keys = Arc::new(KeyRing::derive(&[7; 32], "test").unwrap());
    let state = ApiState {
        store: store.clone(),
        keys,
        telegram_base: format!("http://{address}"),
        client: reqwest::Client::new(),
        telegram: Arc::new(Semaphore::new(1)),
        large_body: Arc::new(Semaphore::new(2)),
        large_ingest: Arc::new(Semaphore::new(1)),
        body_budget: Arc::new(Semaphore::new(64 * 1024 * 1024)),
        max_body: 32 * 1024 * 1024,
        max_multipart_body: 64 * 1024 * 1024,
        data_root: temp.path().to_path_buf(),
        max_recipients: 100_000,
        shared_limit: 65536,
        patch_limit: 8192,
        negative_401: Arc::new(Mutex::new(HashMap::new())),
        database_path: temp.path().join("queue.db"),
        http_concurrency: Arc::new(Semaphore::new(64)),
        free_disk_reserve_bytes: 0,
        storage_high_watermark_bytes: u64::MAX,
        nonterminal_cap: 1_000_000,
    };
    Fixture {
        app: api_router(state),
        store,
        _temp: temp,
        calls,
    }
}
async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: impl Into<axum::body::Body>,
) -> (u16, Value) {
    let mut b = axum::http::Request::builder().method(method).uri(uri);
    if let Some(c) = content_type {
        b = b.header("content-type", c)
    }
    let response = app
        .clone()
        .oneshot(b.body(body.into()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
async fn auth_negative_cache_config_submit_poll_results_deadline_and_isolation() {
    let f = fixture().await;
    for _ in 0..2 {
        assert_eq!(
            call(&f.app, "GET", "/botbad/getBulkDeliveryConfig", None, "")
                .await
                .0,
            401
        )
    }
    assert_eq!(f.calls.lock().unwrap()["bad"], 1);
    let (_,set)=call(&f.app,"POST","/botone/setBulkDeliveryConfig",Some("application/json"),r#"{"target_msgs_per_sec":999,"fairness_weight":99,"completion_webhook_url":"https://example.test/hook"}"#).await;
    assert_eq!(set["result"]["target_msgs_per_sec"], 25.0);
    assert_eq!(set["result"]["fairness_weight"], 16);
    assert!(set["result"]["webhook_secret"].is_string());
    let (_, get) = call(&f.app, "GET", "/botone/getBulkDeliveryConfig", None, "").await;
    assert!(get["result"].get("webhook_secret").is_none());
    assert_eq!(get["result"]["webhook_secret_configured"], true);
    let body = r#"{"recipients":[{"chat_id":1},{"chat_id":2,"text":"other"}],"parameters":{"text":"shared"}}"#;
    let (status, submit) = call(
        &f.app,
        "POST",
        "/botone/sendMessage",
        Some("application/json"),
        body,
    )
    .await;
    assert_eq!(status, 200);
    let id = submit["result"]["job_id"].as_str().unwrap();
    let (status, job) = call(&f.app, "GET", &format!("/botone/bulk/jobs/{id}"), None, "").await;
    assert_eq!(status, 200);
    assert_eq!(job["result"]["total"], 2);
    assert_eq!(job["result"]["queued"], 2);
    assert_eq!(job["result"]["rate"]["window_secs"], 30);
    let (status, results) = call(
        &f.app,
        "GET",
        &format!("/botone/bulk/jobs/{id}/results?limit=1"),
        None,
        "",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(results["result"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(results["result"]["next_cursor"], 1);
    assert_eq!(
        call(&f.app, "GET", &format!("/bottwo/bulk/jobs/{id}"), None, "")
            .await
            .0,
        404
    );
    f.store
        .writer
        .execute(WriterCmd::ExpireDeadline {
            now_unix: i64::MAX / 2,
        })
        .unwrap();
    let (_, expired) = call(&f.app, "GET", &format!("/botone/bulk/jobs/{id}"), None, "").await;
    assert_eq!(expired["result"]["failed"], 2);
    assert_eq!(expired["result"]["percent_complete"], 100.0);
    assert!(expired["result"]["expire_at_unix"].is_number());
    let (_, reset) = call(
        &f.app,
        "POST",
        "/botone/resetBulkDeliveryConfig",
        Some("application/json"),
        "{}",
    )
    .await;
    assert_eq!(reset["result"]["target_msgs_per_sec"], 20.0);
    assert_eq!(reset["result"]["webhook_secret_configured"], false);
    assert_eq!(f.calls.lock().unwrap()["one"], 1);
}
#[tokio::test]
async fn multipart_files_are_durable_before_promote() {
    let f = fixture().await;
    let boundary = "BOUNDARY42";
    let body=format!("--{boundary}\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"x.txt\"\r\nContent-Type: text/plain\r\n\r\nfile contents\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"payload_json\"\r\n\r\n{{\"recipients\":[{{\"chat_id\":1}}],\"parameters\":{{\"photo\":\"attach://photo\"}}}}\r\n--{boundary}--\r\n");
    let (status, v) = call(
        &f.app,
        "POST",
        "/botone/sendPhoto",
        Some(&format!("multipart/form-data; boundary={boundary}")),
        body,
    )
    .await;
    assert_eq!(status, 200, "{v}");
    let id = v["result"]["job_id"].as_str().unwrap();
    let dir = f._temp.path().join("files").join(id);
    let files = std::fs::read_dir(dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(std::fs::read(files[0].path()).unwrap(), b"file contents");
    assert_eq!(
        call(&f.app, "GET", &format!("/botone/bulk/jobs/{id}"), None, "")
            .await
            .0,
        200
    );
}
