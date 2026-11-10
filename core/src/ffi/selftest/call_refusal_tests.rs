use serde_json::json;
use super::rig_tests::{bound, Fault, Rig};
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::{plain, Stand, CREATE_MOMENT};
use crate::ffi::command::CommandUpload;
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadRequest};
use crate::ffi::{CommandRequest, Database, DbError};

async fn run_unregister(rig: &Rig) -> Result<(), DbError> {
    let epoch = rig.database.session_epoch();
    bound(rig.database.run(epoch, "UnregisterDevice".into(), "{}".into())).await.map(|_| ())
}

async fn ends(database: &Database) { bound(crate::ffi::settled(database)).await.unwrap(); }

#[tokio::test(flavor = "multi_thread")]
async fn call_response_limit_unknown_pair() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let limit = crate::transport::MAX_HTTP_RESPONSE_BYTES;
    rig.world.fail_next("UnregisterDevice", Fault::AppliedResponseTooLarge(limit));
    let database = rig.database.clone();
    let epoch = database.session_epoch();
    let mut call = rig.spawn(async move { database.run(epoch, "UnregisterDevice".into(), "{}".into()).await });
    let mut armed = rig.clock.armed();
    let result = loop {
        tokio::select! {
            result = &mut call => break result.unwrap(),
            () = rig.clock.await_armed(armed) => { armed = rig.clock.armed(); rig.clock.advance(5_000); }
        }
    };
    assert!(result.is_ok(), "the replay returns the stored server answer: {result:?}");
    let sends: Vec<_> = rig.world.log().into_iter().filter(|entry| entry.operation == "UnregisterDevice").collect();
    assert!(sends.len() >= 2);
    assert!(sends[0].request_id.is_some() && sends[0].sequence.is_some());
    assert!(sends.iter().all(|send| send.request_id == sends[0].request_id && send.sequence == sends[0].sequence));
    assert_eq!(rig.world.count("UnregisterDevice"), 1);
    rig.database.close_now();
}

/// D-220: a mutation call retries the held pair on a transient transport failure and answers the server once the retry lands; `wire.md` «transport failure without an answer» sequenced = `Ok|2`.
#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_call_with_a_transient_transport_failure_retries_the_held_pair() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    rig.world.fail_next("UnregisterDevice", Fault::Down);
    let database = rig.database.clone();
    let epoch = database.session_epoch();
    let mut call = rig.spawn(async move { database.run(epoch, "UnregisterDevice".into(), "{}".into()).await });
    let mut armed = rig.clock.armed();
    let answer = loop {
        tokio::select! {
            result = &mut call => break result.unwrap(),
            () = rig.clock.await_armed(armed) => { armed = rig.clock.armed(); rig.clock.advance(5_000); }
        }
    };
    assert!(answer.is_ok(), "{answer:?}");
    ends(&rig.database).await;
    let sends: Vec<_> = rig.world.log().into_iter().filter(|entry| entry.operation == "UnregisterDevice").collect();
    assert_eq!(sends.len(), 2, "the transient failure is retried once");
    assert_eq!(sends[0].sequence, sends[1].sequence, "the retry reuses the held pair");
    rig.database.close_now();
}

/// D-153: a mutation call whose answer carries a temporary GraphQL code answers that error and is sent once.
#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_call_refused_with_a_temporary_code_answers_the_graphql_error_and_is_sent_once() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let hits = rig.world.log().len();
    rig.world.fail_next("UnregisterDevice", Fault::Answer(json!({"errors": [{"message": "Busy", "extensions": {"code": "SERVICE_UNAVAILABLE"}}]})));
    let DbError::Graphql { messages, extensions } = run_unregister(&rig).await.unwrap_err() else { panic!("expected a Graphql error") };
    assert_eq!(messages, vec!["Busy".to_string()]);
    assert_eq!(extensions.and_then(|extensions| extensions.code), Some("SERVICE_UNAVAILABLE".to_string()));
    ends(&rig.database).await;
    assert_eq!(rig.world.log().len(), hits + 1);
    rig.database.close_now();
}

fn kind(result: Result<(), DbError>) -> &'static str {
    match result {
        Ok(()) => "admitted",
        Err(DbError::Closed { .. }) => "Closed",
        Err(DbError::NoSession { .. }) => "NoSession",
        Err(DbError::Invalid { .. }) => "Invalid",
        Err(DbError::NotFound { .. }) => "NotFound",
        Err(DbError::InFlight { .. }) => "InFlight",
        Err(_) => "other",
    }
}

fn batch_items(directory: &std::path::Path, empty_key_at: Option<usize>, pending: bool) -> Vec<WireUploadItem> {
    (0..3).map(|index| {
        let path = directory.join(format!("{index}.jpg"));
        std::fs::write(&path, b"image").unwrap();
        let key = if empty_key_at == Some(index) { String::new() } else { format!("key-{index}") };
        WireUploadItem { values_json: format!(r#"{{"clientKey":"{key}","size":3,"position":{index}}}"#), source: (!pending).then(|| path.to_str().unwrap().into()).map(|path| crate::ffi::upload_wire::WireSource::File { path, name: None, mime: None }).unwrap_or(crate::ffi::upload_wire::WireSource::Pending), variables_json: "{}".into() }
    }).collect()
}

/// D-230 (P-BR-11): `upload`, every element of `uploadBatch`, a pending `send` and `cancel_upload` answer the first refusal of steps 1-5.
#[tokio::test(flavor = "multi_thread")]
async fn call_refusal_order_table() {
    let stand = Stand::new();
    let operations = [OPERATIONS[0], OPERATIONS[1], OPERATIONS[2], CREATE_MOMENT];
    let database = stand.open(None, &operations).await;
    database.set_online(false).unwrap();
    let epoch = database.session_epoch();
    let row = bound(database.send(plain("Moment", r#"{"text":"intro"}"#, epoch))).await.unwrap().optimistic_value.unwrap().id;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"video").unwrap();
    let request = |path: Option<&std::path::Path>| WireUploadRequest { source: path.map(|path| path.to_string_lossy().into_owned()).map(|path| crate::ffi::upload_wire::WireSource::File { path, name: None, mime: None }).unwrap_or(crate::ffi::upload_wire::WireSource::Pending), variables_json: "{}".into() };
    let batch = |operation: &str| WireUploadBatch { operation: operation.into(), variables_json: "{}".into() };
    let mut table: Vec<(String, &str, &str)> = Vec::new();
    for (stale, model, row_id, action, expected) in [
        (true, "NoSuchModel", "absent-row", "no_such_action", "NoSession"),
        (false, "NoSuchModel", "absent-row", "no_such_action", "Invalid"),
        (false, "Moment", "absent-row", "no_such_action", "Invalid"),
        (false, "Moment", "absent-row", "upload_clip", "admitted"),
    ] {
        let at = if stale { epoch + 1000 } else { epoch };
        let result = database.upload(at, model.into(), row_id.into(), action.into(), request(Some(&source))).await.map(drop);
        table.push((format!("upload stale={stale} model={model} row={row_id} action={action}"), expected, kind(result)));
        let result = database.cancel_upload(at, model.into(), row_id.into(), action.into()).await.map(drop);
        table.push((format!("cancel_upload stale={stale} model={model} row={row_id} action={action}"), expected, kind(result)));
    }
    for (pending, bad_element) in [(false, None), (true, None), (false, Some(2)), (true, Some(2))] {
        for (stale, model, operation, expected) in [
            (true, "NoSuchModel", "no_such_operation", "NoSession"),
            (false, "NoSuchModel", "no_such_operation", "Invalid"),
            (false, "Asset", "no_such_operation", "Invalid"),
            (false, "Asset", "create_files", if bad_element.is_some() { "Invalid" } else { "admitted" }),
        ] {
            if expected == "admitted" { continue; }
            let items = batch_items(directory.path(), bad_element, pending);
            let at = if stale { epoch + 1000 } else { epoch };
            let result = database.upload_create_batch(at, model.into(), items, batch(operation)).await.map(drop);
            table.push((format!("uploadBatch pending={pending} bad_element={bad_element:?} stale={stale} model={model} operation={operation}"), expected, kind(result)));
        }
    }
    for (stale, model, expected) in [(true, "NoSuchModel", "NoSession"), (false, "NoSuchModel", "Invalid")] {
        let at = if stale { epoch + 1000 } else { epoch };
        let command = CommandRequest { epoch: at, model: model.into(), values_json: r#"{"text":"clip"}"#.into(), args_json: "{}".into(), upload: Some(CommandUpload { action: "no_such_action".into(), request: request(None) }) };
        table.push((format!("send without a file stale={stale} model={model}"), expected, kind(database.send(command).await.map(drop))));
    }
    let wrong: Vec<String> = table.iter().filter(|(_, expected, observed)| expected != observed).map(|(name, expected, observed)| format!("{name}: expected {expected}, got {observed}")).collect();
    let _ = database.close_database().await;
    let closed = database.upload(epoch, "Moment".into(), row.clone(), "upload_clip".into(), request(Some(&source))).await.map(drop);
    assert_eq!(kind(closed), "Closed");
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
