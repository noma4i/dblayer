use std::sync::Arc;
use serde_json::json;
use super::stand::Stand;
use crate::ffi::command::CommandRequest;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;

#[tokio::test(flavor = "multi_thread")]
async fn consumer_keep_retry_consistent_at_every_commit() {
    use rusqlite::OptionalExtension;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer_next("MessageSend", json!({"errors":[{"message":"denied","extensions":{"code":"FORBIDDEN"}}]}));
    let receipt = bound(stand.database.send(CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","body":"first","kind":"TEXT","createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(), upload: None })).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let op = receipt.operation.unwrap();
    let result = bound(stand.database.confirmed(receipt, false)).await;
    assert!(matches!(&result, Err(crate::ffi::DbError::Rejected { kept: true, failure }) if failure.kind == "rejected" && failure.message == "denied"), "{result:?}");
    stand.database.set_online(false).unwrap();
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let before: (String, i64, i64, Option<String>, Option<String>) = db.query_row("SELECT status, attempts, cycle_id, error_kind, error_message FROM op WHERE seq = ?1", [op], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))).unwrap();
    assert_eq!((before.0.as_str(), before.1, before.2), ("failed", 1, 1));
    assert_eq!((before.3.as_deref(), before.4.as_deref()), (Some("rejected"), Some("denied")), "{before:?}");
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    let retry = bound(stand.database.retry(epoch, "Message".into(), id.clone(), None, None)).await.unwrap();
    stand.answer("MessageSend", json!({"messageSend":{"message":super::stand::message("server-message", "c1", "first")}}));
    stand.database.set_online(true).unwrap();
    let confirmed = bound(stand.database.confirmed(retry, false)).await.unwrap();
    assert_eq!(confirmed.value.unwrap().id, "server-message");
    stand.settle().await;
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 3];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("retry-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        let saved: Option<(String, i64, i64, Option<String>, Option<String>)> = db.query_row("SELECT status, attempts, cycle_id, error_kind, error_message FROM op WHERE seq = ?1", [op], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))).optional().unwrap();
        let old: Option<String> = db.query_row("SELECT body FROM row_Message WHERE id = ?1", [&id], |r| r.get(0)).optional().unwrap();
        let new: Option<String> = db.query_row("SELECT body FROM row_Message WHERE id = 'server-message'", [], |r| r.get(0)).optional().unwrap();
        match saved {
            Some((ref status, attempts, cycle, ref kind, ref message)) if status == "failed" => {
                assert_eq!((attempts, cycle, kind, message), (1, 1, &before.3, &before.4), "WAL {k}");
                assert_eq!((old.as_deref(), new.as_deref()), (Some("first"), None), "WAL {k}");
                seen[0] = true;
            }
            Some((ref status, attempts, cycle, ref kind, ref message)) if status == "pending" || status == "sending" => {
                assert_eq!((attempts, cycle, kind, message), (1, 2, &None, &None), "WAL {k}");
                assert_eq!((old.as_deref(), new.as_deref()), (Some("first"), None), "WAL {k}");
                seen[1] = true;
            }
            None => {
                assert_eq!((old.as_deref(), new.as_deref()), (None, Some("first")), "WAL {k}");
                seen[2] = true;
            }
            other => panic!("WAL {k}: unexpected operation {other:?}"),
        }
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 3]);
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_logout_consistent_at_every_commit() {
    use crate::ffi::{CredentialStore, SessionStanding};
    use crate::ffi::command::CommandUpload;
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", super::thread_pages_tests::page(1, 1, false, 1));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.database.set_online(false).unwrap();
    let upload = bound(stand.database.send(CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","kind":"VIDEO","clientKey":"pending","media":{"kind":"VIDEO"}}).to_string(), args_json: "{}".into(), upload: Some(CommandUpload { action: "send_file".into(), request: WireUploadRequest { source: WireSource::Pending, variables_json: r#"{"chatId":"c1","kind":"VIDEO"}"#.into() } }) })).await.unwrap();
    let id = upload.optimistic_value.unwrap().id;
    stand.settle().await;
    let secret = credentials.get().unwrap().unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    stand.database.set_online(true).unwrap();
    let answer = bound(stand.database.session_call("logout".into(), "{}".into())).await.unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&answer).unwrap(), json!({"revoked":true}));
    stand.settle().await;
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 2];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("logout-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let restored_credentials = Arc::new(WatchedHeld::default());
        restored_credentials.set(secret.clone()).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), restored_credentials.clone(), None, false).await;
        reopened.settle().await;
        let status = reopened.database.session_status().unwrap().phase;
        let db = rusqlite::Connection::open(&image).unwrap();
        let counts: Vec<i64> = ["row_Message", "query_window", "op", "sync_upload", "transfer_outcome"].iter().map(|table| db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap()).collect();
        if status == SessionStanding::Active {
            assert!(counts[0] >= 2 && counts[1] > 0 && counts[2] > 0 && counts[3] > 0, "WAL {k}: active counts={counts:?}");
            assert!(reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), id.clone()).unwrap().is_some());
            assert_eq!(restored_credentials.get().unwrap().as_deref(), Some(secret.as_str()));
            seen[0] = true;
        } else {
            assert_eq!(status, SessionStanding::None, "WAL {k}");
            assert!(counts.iter().all(|count| *count == 0), "WAL {k}: cleared counts={counts:?}");
            assert!(restored_credentials.get().unwrap().is_none(), "WAL {k}");
            seen[1] = true;
        }
        drop(db);
        reopened.database.set_online(true).unwrap();
        reopened.settle().await;
        assert_eq!(bound(reopened.database.interrupted_session_answer()).await.unwrap(), None, "WAL {k}: a live logout has no saved answer");
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 2]);
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_ephemeral_memory_consistent_at_every_commit() {
    use crate::ffi::wire::Cell;
    use crate::schema::{EphemeralSpec, FieldSpec, ModelSpec, Storage};
    use crate::value::ScalarKind;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let mut model = ModelSpec::new("MemoryNote", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Memory;
    let mut spec: serde_json::Value = serde_json::from_str(super::stand::SPEC).unwrap();
    spec["models"].as_array_mut().unwrap().push(serde_json::to_value(model).unwrap());
    let chat = spec["models"].as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Chat").unwrap();
    let field = EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))).resetting_after("5.seconds");
    chat["ephemeral"].as_array_mut().unwrap().push(serde_json::to_value(field).unwrap());
    let spec = spec.to_string();
    let stand = Stand::prepare_at_with_spec(Some(path.to_string_lossy().into_owned()), credentials.clone(), None, true, 1_000_000, spec.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("ChatNode", json!({"node":super::stand::chat("c1", "before")}));
    bound(stand.database.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    let note = bound(stand.database.create(epoch, "MemoryNote".into(), json!({"text":"transient"}).to_string(), "{}".into())).await.unwrap().optimistic_value.unwrap().id;
    bound(stand.database.set_ephemeral(epoch, "Chat".into(), "c1".into(), json!({"typing":true}).to_string())).await.unwrap();
    stand.settle().await;
    let row = stand.database.peek_row(epoch, "Chat".into(), "c1".into()).unwrap().unwrap();
    assert_eq!(stand.cell("Chat", &row, "typing"), Cell::Bool { value: true });
    assert!(stand.database.peek_row(epoch, "MemoryNote".into(), note.clone()).unwrap().is_some());
    let db = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    stand.answer("ChatNode", json!({"node":super::stand::chat("c1", "after")}));
    bound(stand.database.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    stand.settle().await;
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("memory-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_at_with_spec(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false, 1_000_000, spec.clone()).await;
        reopened.settle().await;
        let epoch = reopened.database.session_epoch();
        assert!(reopened.database.peek_row(epoch, "MemoryNote".into(), note.clone()).unwrap().is_none(), "WAL {k}: memory row survived");
        let row = reopened.database.peek_row(epoch, "Chat".into(), "c1".into()).unwrap().unwrap();
        assert_eq!(reopened.cell("Chat", &row, "typing"), Cell::Bool { value: false }, "WAL {k}");
        let db = rusqlite::Connection::open(&image).unwrap();
        let tables: i64 = db.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='row_MemoryNote'", [], |r| r.get(0)).unwrap();
        let operations: i64 = db.query_row("SELECT count(*) FROM op WHERE action_key LIKE 'MemoryNote:%'", [], |r| r.get(0)).unwrap();
        assert_eq!((tables, operations), (0, 0), "WAL {k}");
        assert!(!std::path::Path::new(&format!("{}.files/transfers", image.display())).exists(), "WAL {k}");
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_cascade_destroy_consistent_at_every_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("ChatNode", json!({"node":super::stand::chat("c1", "chat")}));
    bound(stand.database.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    stand.answer("chatThread", super::thread_pages_tests::page(1, 2, false, 2));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.database.set_online(false).unwrap();
    let receipt = bound(stand.database.destroy(epoch, "Chat".into(), "c1".into())).await.unwrap();
    let op = receipt.operation.unwrap();
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let saved: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [op], |r| r.get(0)).unwrap();
    assert_eq!(saved, 1, "the offline destroy must be durable before the crash");
    assert_eq!(stand.requests("ChatGroupDelete"), 0);
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    stand.answer("ChatGroupDelete", json!({"groupChatDelete":{"deletedId":"c1"}}));
    stand.database.set_online(true).unwrap();
    let answer = bound(stand.database.confirmed(receipt, false)).await;
    assert!(answer.is_ok(), "destroy: {answer:?}");
    stand.settle().await;
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 2];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("cascade-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        let parent: i64 = db.query_row("SELECT count(*) FROM row_Chat WHERE id = 'c1'", [], |r| r.get(0)).unwrap();
        let children: i64 = db.query_row("SELECT count(*) FROM row_Message WHERE id IN ('m1','m2') AND chat_id = 'c1'", [], |r| r.get(0)).unwrap();
        let members: i64 = db.query_row("SELECT count(*) FROM scope_member WHERE model = 'Message' AND row_id IN ('m1','m2')", [], |r| r.get(0)).unwrap();
        let queued: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [op], |r| r.get(0)).unwrap();
        if k == 0 { assert_eq!(queued, 1, "offline destroy vanished on open, parent={parent}, children={children}, members={members}"); }
        seen[usize::from(queued == 0)] = true;
        let mut query = db.prepare("SELECT seq FROM tombstone WHERE (model = 'Chat' AND id = 'c1') OR (model = 'Message' AND id IN ('m1','m2')) ORDER BY model,id").unwrap();
        let tombs: Vec<i64> = query.query_map([], |r| r.get(0)).unwrap().collect::<Result<_,_>>().unwrap();
        drop(query);
        if parent == 1 {
            assert_eq!((children, members, queued), (2, 2, 1), "WAL {k}");
            assert!(tombs.is_empty(), "WAL {k}: tombstones={tombs:?}");
        } else {
            assert_eq!((children, members), (0, 0), "WAL {k}");
            assert!(tombs.is_empty() || (tombs.len() == 3 && tombs.iter().all(|seq| *seq == tombs[0])), "WAL {k}: tombstones={tombs:?}");
            if queued == 0 { assert_eq!(tombs.len(), 3, "WAL {k}: confirmed destroy tombstones={tombs:?}"); }
            let other: i64 = db.query_row("SELECT count(*) FROM op WHERE seq != ?1 AND (action_key LIKE '%m1%' OR action_key LIKE '%m2%')", [op], |r| r.get(0)).unwrap();
            assert_eq!(other, 0, "WAL {k}");
        }
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 2], "images must include queued and confirmed destroy");
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_nested_node_consistent_at_every_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    let value = |number: i64| {
        let mut row = super::stand::moment("m1", number);
        row["media"] = json!({"kind":"IMAGE","fileUrl":format!("url-{number}")});
        row["visitorIds"] = json!([format!("user-{number}")]);
        row["widgets"] = json!([{"id":format!("widget-{number}"),"kind":"LABEL","settings":{"offset":{"x":number,"y":number}}}]);
        row
    };
    stand.answer("MomentNode", json!({"node":value(1)}));
    bound(stand.database.node(epoch, "Moment".into(), "node".into(), "m1".into(), true)).await.unwrap().unwrap();
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    stand.answer("MomentNode", json!({"node":value(2)}));
    bound(stand.database.node(epoch, "Moment".into(), "node".into(), "m1".into(), true)).await.unwrap().unwrap();
    stand.settle().await;
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 2];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("nested-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let row = bound(reopened.database.read_row(reopened.database.session_epoch(), "Moment".into(), "m1".into())).await.unwrap().unwrap();
        let visits = reopened.cell("Moment", &row, "visitsCount");
        let number = match visits { crate::ffi::wire::Cell::Int { value: n @ 1..=2 } => n, other => panic!("WAL {k}: visits {other:?}") };
        let stored = reopened.database.db.read(&["Moment"], |reader| reader.point("Moment", "m1")).unwrap().unwrap();
        assert_eq!(stored["media"]["fileUrl"], json!(format!("url-{number}")), "WAL {k}");
        assert_eq!(stored["visitorIds"], json!([format!("user-{number}")]), "WAL {k}");
        assert_eq!(stored["widgets"][0]["id"], json!(format!("widget-{number}")), "WAL {k}");
        assert_eq!(stored["widgets"][0]["settings"]["offset"]["x"].as_f64(), Some(number as f64), "WAL {k}: widgets={:?}", stored["widgets"]);
        let db = rusqlite::Connection::open(&image).unwrap();
        let mut tables = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'row_Moment__%'").unwrap();
        let names: Vec<String> = tables.query_map([], |r| r.get(0)).unwrap().collect::<Result<_,_>>().unwrap();
        drop(tables);
        assert!(!names.is_empty());
        for name in names {
            let mut columns = db.prepare(&format!("PRAGMA table_info(\"{name}\")")).unwrap();
            let columns: Vec<String> = columns.query_map([], |r| r.get(1)).unwrap().collect::<Result<_,_>>().unwrap();
            if columns.iter().any(|column| column == "row_id") {
                let orphan: i64 = db.query_row(&format!("SELECT count(*) FROM \"{name}\" WHERE row_id NOT IN (SELECT id FROM row_Moment)"), [], |r| r.get(0)).unwrap();
                assert_eq!(orphan, 0, "WAL {k}: orphan in {name}");
            }
        }
        seen[(number - 1) as usize] = true;
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 2]);
    bound(stand.database.close_database()).await.unwrap();
}
