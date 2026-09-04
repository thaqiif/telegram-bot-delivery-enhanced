use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::{collections::HashSet, sync::Arc};
use telegram_bulk_delivery::{
    auth::{KeyRing, Purpose},
    domain::BotId,
    http::{redact_path, router, HealthState, Readiness},
    store::{CheckpointMode, ReadResult, Store, WriterCmd, WriterResult, MIGRATION},
};
use tempfile::TempDir;
use tower::ServiceExt;

fn store() -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("db.sqlite"), 128, 8).unwrap();
    (dir, store)
}

#[test]
fn sqlite_runtime_and_pragmas_are_enforced() {
    let (_dir, store) = store();
    assert!(matches!(
        store.readers.sqlite_version().unwrap(),
        ReadResult::SQLiteVersion(_)
    ));
    assert_eq!(
        store.readers.pragma_text("journal_mode").unwrap(),
        ReadResult::Text("wal".into())
    );
    assert_eq!(
        store.readers.pragma_int("synchronous").unwrap(),
        ReadResult::Integer(2)
    );
    assert_eq!(
        store.readers.pragma_int("foreign_keys").unwrap(),
        ReadResult::Integer(1)
    );
    assert_eq!(
        store.readers.pragma_int("temp_store").unwrap(),
        ReadResult::Integer(1)
    );
    assert_eq!(
        store.readers.pragma_int("mmap_size").unwrap(),
        ReadResult::Integer(0)
    );
    assert_eq!(
        store.readers.pragma_int("busy_timeout").unwrap(),
        ReadResult::Integer(5000)
    );
    assert_eq!(
        store.readers.pragma_int("query_only").unwrap(),
        ReadResult::Integer(1)
    );
}

#[test]
fn schema_contains_every_foundation_table_and_index() {
    let (_dir, store) = store();
    let ReadResult::TextList(names) = store.readers.table_names().unwrap() else {
        panic!("unexpected result")
    };
    let actual: HashSet<_> = names.iter().map(String::as_str).collect();
    for name in [
        "_migrations",
        "bots",
        "bot_configs",
        "jobs",
        "job_files",
        "recipients",
        "limiter_pauses",
        "webhook_events",
        "webhook_pages",
        "webhook_deliveries",
        "fairness_deficits",
        "rate_samples",
    ] {
        assert!(actual.contains(name), "missing {name}");
    }
    for required in [
        "wire_started",
        "lease_owner",
        "recipients_ready",
        "webhook_deliveries",
    ] {
        assert!(MIGRATION.contains(required));
    }
}

#[test]
fn exactly_four_query_workers_and_one_writer_are_named() {
    let (_dir, store) = store();
    assert_eq!(store.readers.thread_count(), 4);
    let mut names = HashSet::new();
    for _ in 0..8 {
        let ReadResult::Pong { thread_name } = store.readers.ping().unwrap() else {
            panic!()
        };
        names.insert(thread_name);
    }
    assert_eq!(
        names,
        HashSet::from([
            "sqlite-reader-0".into(),
            "sqlite-reader-1".into(),
            "sqlite-reader-2".into(),
            "sqlite-reader-3".into()
        ])
    );
    assert_eq!(store.writer.thread_name(), "sqlite-writer");
    assert!(matches!(
        store
            .writer
            .execute(WriterCmd::Checkpoint {
                mode: CheckpointMode::Passive
            })
            .unwrap(),
        WriterResult::Checkpoint { .. }
    ));
}

#[test]
fn keyed_bot_id_and_aead_are_bound_to_identity_and_purpose() {
    let master = [7u8; 32];
    let ring = KeyRing::derive(&master, "kid-1").unwrap();
    let token = b"123456789:AAExampleToken_abcdefghijklmnopqrstuvwxyz";
    let bot = ring.bot_id(token);
    assert_eq!(bot, ring.bot_id(token));
    assert_ne!(bot, ring.bot_id(b"other"));
    let encrypted = ring.encrypt(bot, Purpose::BotToken, token).unwrap();
    assert_ne!(encrypted.ciphertext, token);
    assert_eq!(
        &*ring.decrypt(bot, Purpose::BotToken, &encrypted).unwrap(),
        token
    );
    assert!(ring
        .decrypt(BotId([1; 32]), Purpose::BotToken, &encrypted)
        .is_err());
    assert!(ring
        .decrypt(bot, Purpose::WebhookSecret, &encrypted)
        .is_err());
    let secret = b"webhook-secret-material-32-bytes!!";
    let encrypted = ring.encrypt(bot, Purpose::WebhookSecret, secret).unwrap();
    assert_eq!(
        &*ring
            .decrypt(bot, Purpose::WebhookSecret, &encrypted)
            .unwrap(),
        secret
    );
}

#[test]
fn token_paths_are_redacted_everywhere() {
    let token = "123456789:AAExampleToken_abcdefghijklmnopqrstuvwxyz";
    let input = format!("POST /bot{token}/sendMessage failed; status=/bot{token}/bulk/jobs/j1");
    let output = redact_path(&input);
    assert!(!output.contains(token));
    assert_eq!(
        output,
        "POST /bot<redacted>/sendMessage failed; status=/bot<redacted>/bulk/jobs/j1"
    );
}

#[tokio::test]
async fn health_and_readiness_are_distinct() {
    let (_dir, store) = store();
    let readiness = Arc::new(Readiness::ready());
    let app = router(HealthState {
        readiness: readiness.clone(),
        readers: store.readers.clone(),
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    readiness.set_writer_alive(false);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
