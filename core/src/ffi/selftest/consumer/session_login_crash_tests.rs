use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;

fn copy_image(from: &Path, to: &Path) {
    for suffix in ["", "-wal"] {
        let source = format!("{}{suffix}", from.display());
        if Path::new(&source).exists() { std::fs::copy(source, format!("{}{suffix}", to.display())).unwrap(); }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_restored_login_answer_at_every_commit() {
    for refused in [false, true] {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let pending = directory.path().join("pending.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(live.to_string_lossy().into_owned()), credentials.clone()).await;
    first.hold(&["login_link"]);
    let mark = first.request_mark();
    let database = first.database.clone();
    let call = tokio::spawn(async move { database.session_call("login_link".into(), json!({"account":"A"}).to_string()).await });
    first.receive_after(mark, "login_link", None, 1).await;
    let secret = crate::ffi::CredentialStore::get(&*credentials).unwrap().unwrap();
    copy_image(&live, &pending);
    first.release(&["login_link"]);
    bound(call).await.unwrap().unwrap();
    bound(first.database.close_database()).await.unwrap();
    drop(first);

    let restored = Stand::prepare_online(Some(pending.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
    let connection = rusqlite::Connection::open(&pending).unwrap();
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0, "checkpoint must complete: {checkpoint:?}");
    drop(connection);
    if refused { restored.fail_next("login_link", crate::ffi::DbError::Http { status: 403, detail: "refused".into() }); }
    restored.database.set_online(true).unwrap();
    restored.answered(1).await;
    restored.settle().await;
    copy_image(&pending, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let page_size = if page_size == 1 { 65536 } else { page_size };
    let frame_size = 24 + page_size;
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!((refused || commits.len() >= 2) && (wal.len() - 32) / frame_size < 1000, "the WAL must retain each commit: refused={refused}, commits={}, frames={}, requests={:?}", commits.len(), (wal.len() - 32) / frame_size, restored.sequenced());
    bound(restored.database.close_database()).await.unwrap();
    drop(restored);

    for (index, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("image-{index}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        crate::ffi::CredentialStore::set(&*credentials, secret.clone()).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        if refused { reopened.fail_next("login_link", crate::ffi::DbError::Http { status: 403, detail: "refused".into() }); }
        reopened.database.set_online(true).unwrap();
        let answer = bound(reopened.database.interrupted_session_answer()).await;
        if refused {
            assert!(matches!(&answer, Err(crate::ffi::DbError::Http { status: 403, .. })), "WAL commit {index} lost the refusal: {answer:?}, requests: {:?}", reopened.sequenced());
        } else {
            assert!(answer.as_ref().ok().and_then(|value| value.as_deref()).is_some_and(|body| serde_json::from_str::<serde_json::Value>(body).is_ok_and(|value| value["sessionId"] == "A")), "WAL commit {index} lost the login answer: {answer:?}, requests: {:?}", reopened.sequenced());
        }
        bound(reopened.database.close_database()).await.unwrap();
    }
    }
}
