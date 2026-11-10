//! The class test of D-213 over every path that sends a request with a `sequence`: the Outbox mutation, the upload and its final mutation, the Calls mutation, the session call.

use std::sync::Arc;

use serde_json::{json, Value};

use super::rig_tests::bound;
use super::upload_tests::{executed, opened_with_row, request, Puts};
use crate::ffi::tests::session_guards_tests::Rig as SessionRig;
use crate::ffi::{CommandRequest, Database};

#[derive(Clone, Copy, Debug)]
enum Path {
    OutboxMutation,
    UploadFinalMutation,
    CallsMutation,
    SessionCall,
}

struct Row {
    path: Path,
    builder: &'static str,
}

const ROWS: [Row; 4] = [
    Row { path: Path::OutboxMutation, builder: "ffi/write.rs" },
    Row { path: Path::UploadFinalMutation, builder: "ffi/upload.rs" },
    Row { path: Path::CallsMutation, builder: "owners/life/calls_runner.rs" },
    Row { path: Path::SessionCall, builder: "transport/encrypted/graphql.rs" },
];

async fn drive(path: Path, database: &Arc<Database>, rig: &SessionRig, known: bool) {
    let epoch = database.session_epoch();
    match path {
        Path::OutboxMutation => {
            let request = CommandRequest { epoch, model: "Moment".into(), values_json: json!({"text": "hi"}).to_string(), args_json: "{}".into(), upload: None };
            let receipt = bound(database.send(request)).await.unwrap();
            bound(database.confirmed(receipt)).await.unwrap();
        }
        Path::UploadFinalMutation => {
            let directory = tempfile::tempdir().unwrap();
            let source = directory.path().join("clip.mp4");
            std::fs::write(&source, b"abc").unwrap();
            let receipt = bound(database.upload(epoch, "Moment".into(), "42".into(), "upload".into(), request(&source))).await.unwrap();
            bound(database.confirmed(receipt)).await.unwrap();
        }
        Path::CallsMutation => {
            let document = "mutation Ping { ping { id } }";
            if known { rig.server.register(document); }
            database.register_operations(json!([{"name": "Ping", "document": document, "rootPath": "ping"}]).to_string()).await.unwrap();
            rig.server.reply_with(json!({"ping": {"id": "p"}}));
            let answer = bound(database.run(epoch, "Ping".into(), "{}".into())).await.unwrap();
            assert!(!answer.contains("SEQUENCE_CONFLICT"), "{answer}");
        }
        Path::SessionCall => {}
    }
}

fn graphql_pairs(rig: &SessionRig) -> Vec<(i64, String, Value)> {
    rig.server.seen().into_iter().filter(|request| request["body"]["operationName"].is_string() && request["body"]["operationName"] != "Feed").filter_map(|request| Some((request["sequence"].as_i64()?, request["requestId"].as_str()?.to_string(), request["body"].clone()))).collect()
}

fn has_document(body: &Value) -> bool { body.get("query").is_some_and(Value::is_string) }

/// One pair per number when the registry knows the document, else the hash-only pair and then the full pair under a new request id.
fn assert_pairs(path: Path, sent: &[(i64, String, Value)], known: bool) {
    assert!(!sent.is_empty(), "{path:?}: the server saw no sequenced request");
    let numbers: std::collections::BTreeSet<i64> = sent.iter().map(|(sequence, ..)| *sequence).collect();
    assert_eq!(numbers.iter().zip(numbers.iter().skip(1)).filter(|(a, b)| **b != **a + 1).count(), 0, "{path:?}: a number was skipped or repeated: {numbers:?}");
    for number in numbers {
        let pairs: Vec<_> = sent.iter().filter(|(sequence, ..)| *sequence == number).collect();
        if known {
            assert_eq!(pairs.len(), 1, "{path:?} number {number}: a registered document goes in one pair: {pairs:?}");
            assert!(!has_document(&pairs[0].2), "{path:?} number {number}: the first pair is hash-only");
        } else {
            assert_eq!(pairs.len(), 2, "{path:?} number {number}: {pairs:?}");
            assert!(!has_document(&pairs[0].2) && has_document(&pairs[1].2), "{path:?} number {number}: hash-only, then full");
            assert_ne!(pairs[0].1, pairs[1].1, "{path:?} number {number}: the full pair is a new request id");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_sequenced_body_one_per_pair_paths() {
    for known in [true, false] {
        for row in &ROWS {
            let rig = SessionRig::new();
            let database = opened_with_row(&rig, Arc::new(Puts::default())).await;
            let before = graphql_pairs(&rig).len();
            if !known { rig.server.empty_registry(); }
            drive(row.path, &database, &rig, known).await;
            let sent: Vec<_> = graphql_pairs(&rig).into_iter().skip(before).collect();
            match row.path {
                Path::SessionCall => assert!(rig.server.seen().iter().filter(|request| request["sequence"] == 1).all(|request| request["body"].get("extensions").is_none()), "a session call goes verbatim, no document pair"),
                path => {
                    assert_pairs(path, &sent, known);
                    let operation = if matches!(path, Path::CallsMutation) { "Ping" } else { "MakeMoment" };
                    assert!(!executed(&rig, operation).is_empty(), "{path:?}: the mutation was executed");
                }
            }
            database.close_now();
        }
    }
}

#[test]
fn every_builder_of_a_sequenced_body_has_a_row() {
    let root = std::path::PathBuf::from("src");
    let mut found = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            let text = path.to_string_lossy().replace('\\', "/");
            if path.is_dir() {
                if !text.ends_with("/tests") && !text.ends_with("/spec") && !text.ends_with("/selftest") { stack.push(path); }
            } else if text.ends_with(".rs") && !text.ends_with("_tests.rs") && std::fs::read_to_string(&path).unwrap().contains("graphql_body(") {
                found.push(text.trim_start_matches(&format!("{}/", root.to_string_lossy().replace('\\', "/"))).to_string());
            }
        }
    }
    found.sort();
    let mut rows: Vec<&str> = ROWS.iter().map(|row| row.builder).collect();
    rows.sort_unstable();
    assert_eq!(found, rows, "a builder of a sequenced body without a row of the class test (or a row without a builder)");
}
