use serde_json::{json, Map, Value};
use std::io::Cursor;
use telegram_bulk_delivery::api_model::{parse_envelope, shallow_merge};
#[test]
fn order_independent_and_shallow_merge() {
    for body in [
        r#"{"parameters":{"text":"shared","nested":{"a":1}},"recipients":[{"chat_id":1},{"chat_id":2,"text":"custom","nested":{"b":2}}]}"#,
        r#"{"recipients":[{"chat_id":1},{"chat_id":2,"text":"custom","nested":{"b":2}}],"parameters":{"text":"shared","nested":{"a":1}}}"#,
    ] {
        let mut patches = vec![];
        let p = parse_envelope(Cursor::new(body), 65536, 8192, 100000, |_, v| {
            patches.push(v);
            Ok(())
        })
        .unwrap();
        assert_eq!(patches.len(), 2);
        let merged = shallow_merge(&p, &patches[1]);
        assert_eq!(merged["text"], "custom");
        assert_eq!(merged["nested"], json!({"b":2}));
    }
}
#[test]
fn rejects_duplicate_keys_and_empty() {
    let mut sink = |_: u32, _: Map<String, Value>| Ok(());
    assert!(parse_envelope(
        Cursor::new(r#"{"parameters":{},"parameters":{},"recipients":[{}]}"#),
        100,
        100,
        10,
        &mut sink
    )
    .is_err());
    assert!(parse_envelope(
        Cursor::new(r#"{"recipients":[],"x":1}"#),
        100,
        100,
        10,
        &mut sink
    )
    .is_err());
}
#[test]
fn streams_one_hundred_thousand() {
    let mut body = String::from("{\"recipients\":[");
    for i in 0..100000 {
        if i > 0 {
            body.push(',')
        }
        body.push_str(&format!("{{\"chat_id\":{i}}}"));
    }
    body.push_str("],\"parameters\":{\"text\":\"one shared payload\"}}");
    let mut n = 0;
    let p = parse_envelope(Cursor::new(body), 65536, 8192, 100000, |idx, _| {
        assert_eq!(idx, n);
        n += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(n, 100000);
    assert_eq!(p["text"], "one shared payload");
}
#[test]
fn durable_file_protocol() {
    let t = tempfile::tempdir().unwrap();
    let f =
        telegram_bulk_delivery::store::files::persist(t.path(), "job", "file", b"hello").unwrap();
    assert_eq!(std::fs::read(f.path).unwrap(), b"hello");
    assert_eq!(f.size, 5);
    assert!(!t.path().join("tmp/job/file.part").exists());
}
