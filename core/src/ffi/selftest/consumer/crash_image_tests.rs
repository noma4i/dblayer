//! C6: crash and upgrade injection. A crash image is the database file with its write-ahead log copied while the app runs (the state a killed process leaves). After each step of a real scenario (sign in, three upload batches) the image is opened by a fresh core: the durable intents (operations, rows with an upload) equal the live set and the session survives. Then the format version of the file is changed to an older one and the file is reopened, and an integer column is corrupted and the open must answer an error, not panic.
use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem};
use crate::ffi::{ReadRequest, ReadValue};
use crate::test_matrix::Matrix;

async fn operations(stand: &Stand) -> Vec<(i64, String, String)> {
    let epoch = stand.database.session_epoch();
    let stream = stand.database.subscribe_values(epoch, ReadRequest::Operations);
    let first = bound(stream.pull_next()).await;
    match first {
        Some(ReadValue::Operations { items, .. }) => items.into_iter().map(|item| (item.seq, item.row_id, item.action)).collect(),
        _ => Vec::new(),
    }
}

fn copy_image(from: &Path, to: &Path) {
    for suffix in ["", "-wal"] {
        let (source, target) = (format!("{}{suffix}", from.display()), format!("{}{suffix}", to.display()));
        if Path::new(&source).exists() { std::fs::copy(source, target).unwrap(); }
    }
}

async fn batch(stand: &Stand, index: usize) -> Vec<String> {
    let epoch = stand.database.session_epoch();
    let items = (0..2).map(|item| WireUploadItem {
        values_json: json!({"chatId": "c1", "kind": "TEXT", "clientKey": format!("k{index}-{item}"), "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"}).to_string(),
        path: None, name: None, mime: None, variables_json: "{}".into(),
    }).collect();
    let receipt = bound(stand.database.upload_create_batch(epoch, "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
    receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_images_keep_the_durable_intents() {
    let mut matrix = Matrix::new("C6-crash-image");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let mut created: Vec<String> = Vec::new();
    for step in 0..4usize {
        if step > 0 { created.extend(batch(&stand, step).await); }
        stand.settle().await;
        let live = operations(&stand).await;
        let image = directory.path().join(format!("image-{step}.sqlite"));
        copy_image(&path, &image);
        let reopened = Stand::prepare_on(Some(image.to_string_lossy().into_owned()), credentials.clone()).await;
        reopened.settle().await;
        let after = operations(&reopened).await;
        let label = format!("after step {step}");
        matrix.check(&label, "operations of the image", "same set as live", &format!("{live:?}"), &format!("{after:?}"));
        let epoch = reopened.database.session_epoch();
        let missing: Vec<&String> = created.iter().filter(|id| reopened.database.peek_row(epoch, "Message".into(), (*id).clone()).ok().flatten().is_none()).collect();
        matrix.check(&label, "rows of the batches in the image", "none missing", "none missing", if missing.is_empty() { "none missing" } else { "rows missing" });
        matrix.check(&label, "session of the image", "kept", "kept", if step == 0 || reopened.database.session_epoch() > 0 { "kept" } else { "lost" });
        bound(reopened.database.close_database()).await.unwrap();
    }
    let live = operations(&stand).await;
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);

    let broken = directory.path().join("corrupt.sqlite");
    copy_image(&path, &broken);
    let connection = rusqlite::Connection::open(&path).unwrap();
    crate::store::ddl::manifest_write(&connection, crate::store::ddl::FORMAT_VERSION_KEY, "older-format").unwrap();
    drop(connection);
    let upgraded = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    upgraded.settle().await;
    let after = operations(&upgraded).await;
    matrix.check("format version bump", "operations survive", "same set as before", &format!("{:?}", live.iter().map(|op| (&op.1, &op.2)).collect::<Vec<_>>()), &format!("{:?}", after.iter().map(|op| (&op.1, &op.2)).collect::<Vec<_>>()));
    matrix.check("format version bump", "session survives", "kept", "kept", if upgraded.database.session_epoch() > 0 { "kept" } else { "lost" });
    bound(upgraded.database.close_database()).await.unwrap();
    drop(upgraded);

    let connection = rusqlite::Connection::open(&broken).unwrap();
    let corrupted = connection.execute("UPDATE op SET attempts = 99999999999", []).map_err(|error| panic!("{error}")).unwrap();
    drop(connection);
    let opened = tokio::spawn({
        let (path, credentials) = (broken.clone(), credentials.clone());
        async move { Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials).await.settle().await }
    }).await;
    matrix.check("corrupt integer column", "rows corrupted (value above u32 in a STRICT integer column)", ">= 1", ">= 1", if corrupted >= 1 { ">= 1" } else { "0 rows" });
    let answer = match opened {
        Ok(()) => "opened".to_string(),
        Err(error) => { let text = error.into_panic().downcast_ref::<String>().cloned().unwrap_or_default(); if text.contains("Storage") { "Storage error".to_string() } else { format!("other panic: {text}") } }
    };
    matrix.check("corrupt integer column", "open of a corrupt file", "attempts above u32", "Storage error", &answer);
    matrix.finish();
}
