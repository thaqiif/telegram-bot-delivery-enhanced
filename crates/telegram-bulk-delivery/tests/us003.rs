use serde_json::json;
use std::sync::Arc;
use telegram_api_meta::method;
use telegram_api_meta::{can_auto_migrate, lease_ttl_secs, method_timeout_secs};
use telegram_bulk_delivery::scheduler::{
    limiters::{Limiters, Scope},
    Wdrr,
};
use telegram_bulk_delivery::{
    domain::{AmbiguityPolicy, BotId},
    scheduler::dispatcher::{AttemptPolicy, Dispatcher, LeasedCall},
    store::{RecipientInsert, Store, WriterCmd},
    telegram::{
        classify::{classify, Classification},
        simulator::{Behavior, Simulator},
    },
};
fn setup(policy: &str) -> (tempfile::TempDir, Arc<Store>, BotId, String) {
    let d = tempfile::tempdir().unwrap();
    let s = Arc::new(Store::open(d.path().join("db"), 128, 8).unwrap());
    let b = BotId([7; 32]);
    s.writer
        .execute(WriterCmd::UpsertBot {
            bot_id: b,
            token_nonce: [0; 12],
            token_ciphertext: vec![1],
            token_kid: "k".into(),
            telegram_user_id: Some(1),
            now_unix: 1,
        })
        .unwrap();
    let job = "job".to_string();
    s.writer
        .execute(WriterCmd::BeginAcceptJob {
            job_id: job.clone(),
            bot_id: b,
            method: "sendMessage".into(),
            policy_snapshot_json: policy.into(),
            config_version: 1,
            deadline_unix: Some(1000),
        })
        .unwrap();
    s.writer
        .execute(WriterCmd::InsertRecipientBatch {
            job_id: job.clone(),
            rows: vec![RecipientInsert {
                idx: 0,
                bot_id: b,
                patch_json: "{}".into(),
                chat_id: Some("1".into()),
                chat_kind: Some("private".into()),
            }],
        })
        .unwrap();
    s.writer
        .execute(WriterCmd::PromoteJob {
            job_id: job.clone(),
            total: 1,
            shared_params_json: "{}".into(),
            accepted_at_unix: 1,
            file_rows: vec![],
            nonterminal_cap: None,
        })
        .unwrap();
    (d, s, b, job)
}
fn policy() -> AttemptPolicy {
    AttemptPolicy {
        retry_max: 8,
        retry_consumed: 0,
        retry_base_ms: 500,
        retry_max_ms: 60000,
        ambiguity: AmbiguityPolicy::AtLeastOnce,
    }
}
#[tokio::test]
async fn simulator_behaviors_and_capture() {
    let sim = Simulator::default();
    for b in [
        Behavior::Success { message_id: 9 },
        Behavior::Latency {
            millis: 1,
            message_id: 10,
        },
        Behavior::Flood { retry_after: 3 },
        Behavior::ServerError,
        Behavior::Unauthorized,
        Behavior::Migrate { chat_id: -1001 },
        Behavior::BlackholeAfterWire,
    ] {
        sim.push(b)
    }
    for _ in 0..7 {
        let _ = sim
            .send(BotId([1; 32]), "sendMessage", Some("5"), true)
            .await;
    }
    assert_eq!(sim.captured().len(), 7);
    assert_eq!(sim.scope_count("sendmessage:5"), 7)
}
#[test]
fn ttl_dominates_timeout_and_migration_is_scoped() {
    for n in ["sendMessage", "sendPhoto"] {
        let m = method(n).unwrap();
        assert!(lease_ttl_secs(m) >= method_timeout_secs(m) + 10)
    }
    assert!(can_auto_migrate(method("sendMessage").unwrap()));
    assert!(!can_auto_migrate(method("answerInlineQuery").unwrap()))
}
#[test]
fn flood_one_hundred_times_consumes_no_retry() {
    let (_d, s, b, job) = setup(
        r#"{"retry_max_attempts":8,"ambiguity_policy":"at_least_once","fairness_weight":1,"job_deadline_secs":100}"#,
    );
    let dispatcher = Dispatcher::new(s.clone(), "epoch");
    let spec = method("sendMessage").unwrap();
    for i in 0..100 {
        let (j, idx, lease_token) = dispatcher.lease(b, spec, 0, 2 + i * 2).unwrap().unwrap();
        let call = LeasedCall {
            job_id: j,
            idx,
            lease_token,
            bot_id: b,
            method: spec,
            chat_id: Some("1".into()),
            policy: policy(),
            migrated: false,
        };
        dispatcher.mark_wire_started(&call).unwrap();
        dispatcher
            .finish(
                &call,
                429,
                Some(&json!({"ok":false,"parameters":{"retry_after":1}})),
                false,
                2 + i * 2,
            )
            .unwrap();
    }
    let item = s.readers.results(b, job, 0, 1).unwrap().pop().unwrap();
    assert_eq!(item.attempt_count, 100);
    assert_eq!(item.status, "delayed");
    let raw = s.readers.pragma_int("query_only").unwrap();
    assert!(matches!(
        raw,
        telegram_bulk_delivery::store::ReadResult::Integer(1)
    ))
}
#[test]
fn previous_epoch_never_sent_is_requeued_and_stale_completion_is_safe() {
    let (_d, s, b, job) = setup(
        r#"{"retry_max_attempts":1,"ambiguity_policy":"at_most_once","fairness_weight":1,"job_deadline_secs":100}"#,
    );
    let spec = method("sendMessage").unwrap();
    let d = Dispatcher::new(s.clone(), "old");
    let (j, idx, lease_token) = d.lease(b, spec, 1, 1).unwrap().unwrap();
    s.writer
        .execute(WriterCmd::RecoverLeases {
            process_epoch: "new".into(),
            now_unix: 100,
        })
        .unwrap();
    let item = s
        .readers
        .results(b, job.clone(), 0, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(item.status, "queued");
    let call = LeasedCall {
        job_id: j,
        idx,
        lease_token,
        bot_id: b,
        method: spec,
        chat_id: Some("1".into()),
        policy: policy(),
        migrated: false,
    };
    d.finish(
        &call,
        200,
        Some(&json!({"ok":true,"result":{"message_id":99}})),
        false,
        101,
    )
    .unwrap();
    assert_eq!(s.readers.results(b, job, 0, 1).unwrap()[0].status, "queued")
}
#[test]
fn stale_sender_cannot_overwrite_a_released_attempt() {
    let (_d, s, b, job) = setup(
        r#"{"retry_max_attempts":8,"ambiguity_policy":"at_least_once","fairness_weight":1,"job_deadline_secs":100}"#,
    );
    let spec = method("sendMessage").unwrap();
    let d = Dispatcher::new(s.clone(), "epoch");
    // Lease #1 and put it on the wire, but never complete it: simulates a
    // sender that keeps running past its lease TTL.
    let (j, idx, tok1) = d.lease(b, spec, 0, 1).unwrap().unwrap();
    assert_eq!(tok1, 1);
    let stale = LeasedCall {
        job_id: j.clone(),
        idx,
        lease_token: tok1,
        bot_id: b,
        method: spec,
        chat_id: Some("1".into()),
        policy: policy(),
        migrated: false,
    };
    d.mark_wire_started(&stale).unwrap();
    // The lease expires without a completion; recovery requeues it
    // (wire_started=1, at-least-once retries left -> delayed).
    s.writer
        .execute(WriterCmd::RecoverLeases {
            process_epoch: "epoch".into(),
            now_unix: 100,
        })
        .unwrap();
    // Lease #2 issues a NEW token for the same recipient.
    let (j2, idx2, tok2) = d.lease(b, spec, 0, 200).unwrap().unwrap();
    assert_eq!((j2.as_str(), idx2), (j.as_str(), idx));
    assert_ne!(tok2, tok1);
    // The stale sender finishes late carrying its OLD token. Without the
    // per-attempt guard this would terminalize the re-leased row as message
    // 999 and the real attempt's result would be dropped.
    d.finish(
        &stale,
        200,
        Some(&json!({"ok":true,"result":{"message_id":999}})),
        false,
        201,
    )
    .unwrap();
    // The actual lease holder completes with its own token and wins.
    let fresh = LeasedCall {
        job_id: j2,
        idx: idx2,
        lease_token: tok2,
        bot_id: b,
        method: spec,
        chat_id: Some("1".into()),
        policy: policy(),
        migrated: false,
    };
    d.finish(
        &fresh,
        200,
        Some(&json!({"ok":true,"result":{"message_id":42}})),
        false,
        202,
    )
    .unwrap();
    let row = s.readers.results(b, job, 0, 1).unwrap().pop().unwrap();
    assert_eq!(row.status, "succeeded");
    assert_eq!(row.telegram_message_id, Some(42));
    assert_eq!(row.attempt_count, 1);
}

#[test]
fn projection_and_error_taxonomy() {
    let m = method("sendMediaGroup").unwrap();
    let c = classify(
        200,
        Some(&json!({"ok":true,"result":[{"message_id":4,"date":8}]})),
        false,
        9,
        m,
        Some("1"),
    );
    assert!(matches!(c,Classification::Success(ref x) if x.message_id==Some(4)&&x.sent_at_unix==8));
    assert!(matches!(
        classify(403, Some(&json!({"ok":false})), false, 0, m, None),
        Classification::Permanent { norm: "forbidden" }
    ))
}

#[test]
fn chat_limiter_map_respects_capacity_and_future_pauses() {
    let d = tempfile::tempdir().unwrap();
    let s = Arc::new(Store::open(d.path().join("db"), 128, 8).unwrap());
    let b = BotId([9; 32]);
    s.writer
        .execute(WriterCmd::UpsertBot {
            bot_id: b,
            token_nonce: [0; 12],
            token_ciphertext: vec![1],
            token_kid: "k".into(),
            telegram_user_id: Some(1),
            now_unix: 1,
        })
        .unwrap();
    let mut limiters = Limiters::new(0);
    // Insert 131072 chats with future not_before
    for i in 0..131072 {
        let chat = format!("chat{}", i);
        limiters.pause(&Scope::Chat(b, chat.clone()), 5000, 0);
    }
    assert_eq!(limiters.chat_entries(), 131072);
    // The 131073rd paused chat should not drop existing paused entries
    let _scope = Scope::Chat(b, "overflow".into());
    let _result =
        limiters.check_and_acquire(b, Some("overflow"), false, "sendMessage", false, 1000);
    assert_eq!(limiters.chat_entries(), 131072);
}

#[tokio::test]
async fn wdrr_fairness_1000_bots_fake_clock_grants_all() {
    let mut q = Wdrr::new();
    for n in 0..1000 {
        q.upsert_ready(id(n), 1)
    }
    let mut counts = vec![0u16; 1000];
    let mut granted = 0;
    while granted < 1000 {
        if let Some(b) = q.grant() {
            q.complete(b);
            let n = u32::from_be_bytes(b.0[..4].try_into().unwrap()) as usize;
            if counts[n] == 0 {
                granted += 1;
            }
            counts[n] += 1;
        } else {
            break;
        }
    }
    assert!(
        counts.iter().all(|n| *n >= 1),
        "every bot got at least 1 grant"
    );
    let max_w = *counts.iter().max().unwrap();
    let min_w = *counts.iter().min().unwrap();
    assert!(
        max_w - min_w <= 34,
        "fairness bound violated: max={} min={}",
        max_w,
        min_w
    );
}

#[test]
fn wdrr_leave_and_reenter_does_not_starve() {
    let mut q = Wdrr::new();
    for n in 0..100 {
        q.upsert_ready(id(n), 1)
    }
    let b = id(50);
    q.sleep_until(b, 1000);
    for _ in 0..200 {
        if let Some(x) = q.grant() {
            q.complete(x);
        }
    }
    let woken = q.wake_due(1000);
    for bot in woken {
        q.upsert_ready(bot, 1);
    }
    assert!(q.contains(b), "re-entered bot in ring");
    let mut got = false;
    for _ in 0..50 {
        if let Some(x) = q.grant() {
            q.complete(x);
            if x == b {
                got = true;
            }
        }
    }
    assert!(got, "re-entered bot got grant");
}

fn id(n: u32) -> BotId {
    let mut b = [0; 32];
    b[..4].copy_from_slice(&n.to_be_bytes());
    BotId(b)
}
