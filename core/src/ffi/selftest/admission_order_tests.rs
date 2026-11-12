//! Every exported call of `Database` that names a model and a row, against every subset of the violated admission conditions (stale epoch, unknown model, missing row) and against a closed database. Oracle: the public call admission section of `bridge.md` (D-207): closed -> `Closed`, stale epoch -> `NoSession`, arguments -> `Invalid`, row -> `NotFound`; the first violated step answers.
use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::bridge_support_tests::message;
use super::cases::Cases;
use super::rig_tests::{bound, Fault, Rig};
use super::upload_stand::{plain, Stand, CREATE_MOMENT};
use crate::ffi::upload_wire::{WireSource, WireUploadRequest, WireUploadSource};
use crate::ffi::{Database, DbError};

struct Args {
    epoch: u64,
    model: String,
    row: String,
    field: String,
    local_path: String,
    local_value: String,
    values: String,
    action: String,
    source_path: String,
}

/// A database with a local model that has a file field and a local field: the calls of files and local values run on it.
async fn files_database(path: &Path) -> (Arc<Database>, u64, String) {
    let mut local = crate::schema::FieldSpec::object("draft").optional();
    local.fields = vec![crate::schema::FieldSpec::scalar("text", crate::value::ScalarKind::Str).optional()];
    local.merge = Some(crate::schema::MergeRule::Local);
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable(), local]);
    spec.storage = crate::schema::Storage::Local;
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = crate::ffi::Config { path: Some(path.to_string_lossy().into()), data_version: None };
    let database = Database::open(config, crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = database.session_epoch();
    let row = database.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    (database, epoch, row)
}

const FILE_CALLS: [&str; 5] = ["file", "url", "attach_file", "detach", "set_local"];
const UPLOAD_CALLS: [&str; 3] = ["upload", "supply_source", "fail_source"];
const RELATION_CALLS: [&str; 4] = ["node", "nodes", "refresh", "edge_values"];

/// The calls that name a model and a row, except the upload calls (`UPLOAD_CALLS`, run on the stand of uploads). `destroy` is last: it removes the shared row.
const ROW_CALLS: [&str; 19] = [
    "peek_row", "read_row", "file", "url", "retry", "discard", "update", "act", "set_local", "set_ephemeral", "attach_file", "detach", "create", "cancel_upload", "node", "nodes", "refresh", "edge_values", "destroy",
];

async fn invoke(name: &str, database: &Arc<Database>, args: &Args) -> Option<Result<(), DbError>> {
    let (epoch, model, row) = (args.epoch, args.model.clone(), args.row.clone());
    let field = args.field.clone();
    Some(match name {
        "peek_row" => database.peek_row(epoch, model, row).map(|_| ()),
        "read_row" => database.read_row(epoch, model, row).await.map(|_| ()),
        "file" => database.file(epoch, model, row, field).await.map(|_| ()),
        "url" => database.url(epoch, model, row, field).await.map(|_| ()),
        "retry" => database.retry(epoch, model, row, None, None).await.map(|_| ()),
        "discard" => database.discard(epoch, model, row, None).await,
        "destroy" => database.destroy(epoch, model, row).await.map(|_| ()),
        "update" => database.update(epoch, model, row, "{\"text\":\"x\"}".into(), "{}".into()).await.map(|_| ()),
        "act" => database.act(epoch, model, row, "edit".into(), "{}".into(), "{}".into()).await.map(|_| ()),
        "set_local" => database.set_local(epoch, model, row, args.local_path.clone(), args.local_value.clone()).await,
        "set_ephemeral" => database.set_ephemeral(epoch, model, row, args.values.clone()).await,
        "attach_file" => database.attach_file(epoch, model, row, field, args.source_path.clone(), "a".into(), "image/png".into()).await.map(|_| ()),
        "detach" => database.detach(epoch, model, row, field).await.map(|_| ()),
        "create" => database.create(epoch, model, args.values.clone(), "{}".into()).await.map(|_| ()),
        "cancel_upload" => database.cancel_upload(epoch, model, row, "upload".into()).await.map(|_| ()),
        "node" => database.node(epoch, model, "node".into(), row, false).await.map(|_| ()),
        "nodes" => database.nodes(epoch, model, "nodes".into(), vec![row]).await.map(|_| ()),
        "refresh" => database.refresh(epoch, model, Some("profile".into()), "{}".into()).await,
        "edge_values" => database.edge_values(epoch, model, Some("profile".into()), "{}".into(), vec![row]).await.map(|_| ()),
        "upload" => database.upload(epoch, model, row, args.action.clone(), WireUploadRequest { source: crate::ffi::upload_wire::WireSource::File { path: args.source_path.clone(), name: None, mime: None, prepare: None }, variables_json: "{}".into() }).await.map(|_| ()),
        "supply_source" => database.supply_source(epoch, model, row, args.action.clone(), WireUploadSource { path: args.source_path.clone(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None }).await.map(|_| ()),
        "fail_source" => database.fail_source(epoch, model, row, args.action.clone(), "preparation".into()).await,
        _ => return None,
    })
}

async fn invoke_bounded(name: &str, database: &Arc<Database>, args: &Args) -> Option<Result<(), DbError>> {
    match tokio::time::timeout(std::time::Duration::from_millis(400), invoke(name, database, args)).await {
        Ok(result) => result,
        Err(_) => Some(Err(DbError::Interrupted { detail: "matrix timeout".into() })),
    }
}

fn observed(result: &Result<(), DbError>) -> &'static str {
    match result {
        Err(DbError::Interrupted { detail }) if detail == "matrix timeout" => "hangs",
        Err(DbError::Closed { .. }) => "Closed",
        Err(DbError::NoSession { .. }) => "NoSession",
        Err(DbError::Invalid { .. }) => "Invalid",
        Err(DbError::NotFound { .. }) => "NotFound",
        _ => "admitted",
    }
}

/// A read answers `None` for a missing row (store.md), a local or ephemeral value waits for its row (outbox.md, P-DATA-14): only a write that needs the row answers `NotFound`.
const MISSING_ROW_ADMITTED: [&str; 10] = ["peek_row", "read_row", "file", "url", "edge_values", "node", "nodes", "refresh", "set_local", "set_ephemeral"];

fn expected(closed: bool, stale: bool, invalid: bool, missing: bool) -> &'static str {
    if closed { "Closed" } else if stale { "NoSession" } else if invalid { "Invalid" } else if missing { "NotFound" } else { "admitted" }
}

/// An upload on an absent row is admitted (D-287) and opens a transfer under that key: `fail_source` of the key then finds a transfer that waits for no source.
fn upload_expected(name: &str, stale: bool, invalid: bool, missing: bool) -> &'static str {
    match (name, stale || invalid, missing) {
        ("upload", false, true) => "admitted",
        ("fail_source", false, true) => "Invalid",
        _ => expected(false, stale, invalid, missing),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_row_call_answers_the_first_violated_admission_step() {
    let mut matrix = Cases::new("call admission");
    let calls = ROW_CALLS;
    let rig = Rig::encrypted_with_options(false, true).await;
    rig.login_ios("A").await;
    rig.world.answer_with("UsersByIds", json!({"data":{"nodes":[]}}));
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.send(message(epoch, "present", 1))).await.unwrap();
    let present = receipt.optimistic_value.as_ref().map(|row| row.id.clone()).unwrap_or_default();
    let closed_rig = Rig::encrypted().await;
    closed_rig.login_ios("A").await;
    let closed_epoch = closed_rig.database.session_epoch();
    let closed_present = bound(closed_rig.database.send(message(closed_epoch, "present", 1))).await.unwrap().optimistic_value.map(|row| row.id).unwrap_or_default();
    let _ = bound(closed_rig.database.close_database()).await;
    let directory = tempfile::tempdir().unwrap();
    let attach_source = directory.path().join("attachment.bin");
    std::fs::write(&attach_source, [1]).unwrap();
    let (files, files_epoch, files_row) = files_database(&directory.path().join("open.sqlite")).await;
    let (closed_files, closed_files_epoch, closed_files_row) = files_database(&directory.path().join("closed.sqlite")).await;
    let _ = closed_files.close_database().await;
    let _ = bound(rig.database.confirmed(receipt, false)).await;
    rig.world.fail_next("SendMessage", Fault::Http(422));
    let retry_receipt = bound(rig.database.send(message(epoch, "retryrow", 2))).await.unwrap();
    let retry_row = retry_receipt.optimistic_value.as_ref().unwrap().id.clone();
    let _ = bound(rig.database.confirmed(retry_receipt, false)).await;
    rig.world.fail_next("SendMessage", Fault::Http(422));
    let discard_receipt = bound(rig.database.send(message(epoch, "discardrow", 3))).await.unwrap();
    let discard_row = discard_receipt.optimistic_value.as_ref().unwrap().id.clone();
    let _ = bound(rig.database.confirmed(discard_receipt, false)).await;
    let cancel_receipt = bound(rig.database.send(message(epoch, "cancelrow", 4))).await.unwrap();
    let cancel_row = cancel_receipt.optimistic_value.as_ref().unwrap().id.clone();
    let _ = bound(rig.database.confirmed(cancel_receipt, false)).await;
    let cancel_source = directory.path().join("cancel.bin");
    std::fs::write(&cancel_source, b"clip").unwrap();
    let _blob_gate = rig.world.hold_next("CreateBlob");
    let _ = bound(rig.database.upload(epoch, "Message".into(), cancel_row.clone(), "upload".into(), WireUploadRequest { source: WireSource::File { path: cancel_source.to_string_lossy().into_owned(), name: None, mime: None, prepare: None }, variables_json: "{}".into() })).await;
    let target = |name: &str, closed: bool| -> (Arc<Database>, Args) {
        let (database, epoch, row) = match (FILE_CALLS.contains(&name), closed) {
            (true, false) => (files.clone(), files_epoch, files_row.clone()),
            (true, true) => (closed_files.clone(), closed_files_epoch, closed_files_row.clone()),
            (false, false) => (rig.database.clone(), epoch, present.clone()),
            (false, true) => (closed_rig.database.clone(), closed_epoch, closed_present.clone()),
        };
        let mut args = Args { epoch, model: if FILE_CALLS.contains(&name) { "Files" } else { "Message" }.into(), row, field: "photo".into(), local_path: "draft".into(), local_value: "{\"text\":\"x\"}".into(), values: "{\"chatId\":\"c1\",\"text\":\"t\",\"at\":1}".into(), action: "upload".into(), source_path: attach_source.to_string_lossy().into_owned() };
        if RELATION_CALLS.contains(&name) { args.model = "User".into(); args.row = "user".into(); }
        if name == "set_ephemeral" { args.model = "User".into(); args.row = "user".into(); args.values = "{\"typing\":true}".into(); }
        if !closed {
            match name {
                "retry" => args.row = retry_row.clone(),
                "discard" => args.row = discard_row.clone(),
                "cancel_upload" => args.row = cancel_row.clone(),
                _ => {}
            }
        }
        (database, args)
    };
    for &name in &calls {
        for mask in 0u8..8 {
            if name == "create" && mask & 4 != 0 { continue; }
            let (stale, invalid, missing) = (mask & 1 != 0, mask & 2 != 0, mask & 4 != 0 && name != "create");
            let (database, mut args) = target(name, false);
            if stale { args.epoch += 1000; }
            if invalid { args.model = "NoSuchModel".into(); }
            if missing { args.row = "absent-row".into(); }
            let input = format!("stale={stale} invalid_model={invalid} missing_row={missing}");
            match invoke_bounded(name, &database, &args).await {
                Some(result) => matrix.cell(name, "open", &input, expected(false, stale, invalid, missing && !MISSING_ROW_ADMITTED.contains(&name)), observed(&result)),
                None => panic!("{name}: no way to call it"),
            }
        }
        let (database, args) = target(name, true);
        match invoke_bounded(name, &database, &args).await {
            Some(result) => matrix.cell(name, "closed", "valid arguments", "Closed", observed(&result)),
            None => panic!("{name}: no way to call it"),
        }
    }
    matrix.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receipt_is_refused_by_another_database_a_closed_one_an_unknown_operation_and_an_ended_session() {
    use crate::ffi::receipt::{Ticket, WireReceiptTicket};
    let rig = Rig::encrypted_with_options(false, true).await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.send(message(epoch, "present", 1))).await.unwrap();
    let other = Rig::encrypted().await;
    other.login_ios("B").await;
    assert!(matches!(bound(other.database.confirmed(receipt.clone(), false)).await, Err(DbError::Invalid { .. })));
    bound(other.database.close_database()).await.unwrap();
    assert!(matches!(bound(other.database.confirmed(receipt.clone(), false)).await, Err(DbError::Closed { .. })));

    let held = &receipt.ticket;
    let Some(Ticket::Outbox(mut unknown)) = held.ticket.clone() else { panic!("expected an Outbox receipt") };
    unknown.op_id = i64::MAX as u64;
    let ticket = WireReceiptTicket { ticket: Some(Ticket::Outbox(unknown)), owner: held.owner.clone(), epoch: held.epoch, model: held.model.clone(), operation: Some(i64::MAX), revision: held.revision, optimistic_value: held.optimistic_value.clone() };
    let missing = crate::ffi::WireOperationReceipt { operation: Some(i64::MAX), ticket: Arc::new(ticket), ..receipt.clone() };
    assert!(matches!(bound(rig.database.confirmed(missing, false)).await, Err(DbError::NotFound { .. })));
    bound(rig.database.session_reset()).await.unwrap();
    assert!(matches!(bound(rig.database.confirmed(receipt, false)).await, Err(DbError::NoSession { .. })));
}

/// The upload calls run on the stand of uploads: a process keeps one open database, so the stand is closed for the last row.
#[tokio::test(flavor = "multi_thread")]
async fn the_upload_calls_answer_the_first_violated_admission_step() {
    let mut matrix = Cases::new("E-call-admission-uploads");
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    database.set_online(false).unwrap();
    let row = bound(database.send(plain("Moment", r#"{"text":"intro"}"#, epoch))).await.unwrap().optimistic_value.unwrap().id;
    let supply_row = bound(database.send(plain("Moment", r#"{"text":"supply"}"#, epoch))).await.unwrap().optimistic_value.unwrap().id;
    let fail_row = bound(database.send(plain("Moment", r#"{"text":"fail"}"#, epoch))).await.unwrap().optimistic_value.unwrap().id;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"video").unwrap();
    let pending = || WireUploadRequest { source: WireSource::Pending, variables_json: "{}".into() };
    let _ = bound(database.upload(epoch, "Moment".into(), supply_row.clone(), "upload_clip".into(), pending())).await;
    let _ = bound(database.upload(epoch, "Moment".into(), fail_row.clone(), "upload_clip".into(), pending())).await;
    let deferred = |name: &str| match name { "supply_source" => supply_row.clone(), "fail_source" => fail_row.clone(), _ => row.clone() };
    let args = |epoch: u64| Args { epoch, model: "Moment".into(), row: row.clone(), field: String::new(), local_path: String::new(), local_value: String::new(), values: String::new(), action: "upload_clip".into(), source_path: source.to_string_lossy().into_owned() };
    for name in UPLOAD_CALLS {
        for mask in 0u8..8 {
            let (stale, invalid, missing) = (mask & 1 != 0, mask & 2 != 0, mask & 4 != 0);
            let mut call = args(epoch);
            call.row = deferred(name);
            if stale { call.epoch += 1000; }
            if invalid { call.model = "NoSuchModel".into(); }
            if missing { call.row = "absent-row".into(); }
            let input = format!("stale={stale} invalid_model={invalid} missing_row={missing}");
            let result = invoke_bounded(name, &database, &call).await.expect("an upload call");
            matrix.cell(name, "open", &input, upload_expected(name, stale, invalid, missing), observed(&result));
        }
    }
    let _ = database.close_database().await;
    for name in UPLOAD_CALLS {
        let result = invoke_bounded(name, &database, &args(epoch)).await.expect("an upload call");
        matrix.cell(name, "closed", "valid arguments", "Closed", observed(&result));
    }
    matrix.finish();
}

/// E isolation: an upload on a missing row is admitted (D-287) and leaves the database open for the next call.
#[tokio::test(flavor = "multi_thread")]
async fn an_upload_on_a_missing_row_leaves_the_database_open() {
    let mut matrix = Cases::new("E-upload-missing-row");
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    database.set_online(false).unwrap();
    let row = bound(database.send(plain("Moment", r#"{"text":"intro"}"#, epoch))).await.unwrap().optimistic_value.unwrap().id;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"video").unwrap();
    let request = || WireUploadRequest { source: crate::ffi::upload_wire::WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None, prepare: None }, variables_json: "{}".into() };
    let first = database.upload(epoch, "Moment".into(), "absent-row".into(), "upload_clip".into(), request()).await;
    matrix.cell("upload(missing row)", "first call", "missing_row", "admitted", observed(&first.map(|_| ())));
    let next = database.peek_row(epoch, "Moment".into(), row.clone());
    matrix.cell("peek_row(present row)", "next call on the same database", "after the refused upload", "admitted", observed(&next.map(|_| ())));
    let again = database.upload(epoch + 1000, "Moment".into(), "absent-row".into(), "upload_clip".into(), request()).await;
    matrix.cell("upload(stale epoch, missing row)", "second upload", "after the refused upload", "NoSession", observed(&again.map(|_| ())));
    let valid = database.upload(epoch, "Moment".into(), row.clone(), "upload_clip".into(), request()).await;
    matrix.cell("upload(valid row)", "first call", "valid arguments", "admitted", observed(&valid.map(|_| ())));
    let _ = bound(crate::ffi::settled(&database)).await;
    let after = database.peek_row(epoch, "Moment".into(), row);
    matrix.cell("peek_row(present row)", "next call after a valid upload", "the server has no scripted answer", "admitted", observed(&after.map(|_| ())));
    matrix.finish();
}
