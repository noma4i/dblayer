//! Matrix E: every exported call of `Database` that names a model and a row, against every subset of the violated admission conditions (stale epoch, unknown model, missing row) and against a closed database. Oracle: the public call admission section of `bridge.md` (D-207): closed -> `Closed`, stale epoch -> `NoSession`, arguments -> `Invalid`, row -> `NotFound`; the first violated step answers.
use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::bridge_support_tests::message;
use super::rig_tests::{bound, Fault, Rig};
use super::upload_stand::{plain, Stand, CREATE_MOMENT};
use crate::ffi::upload_wire::{WireSource, WireUploadRequest, WireUploadSource};
use crate::ffi::{Database, DbError, FileBytes};
use crate::test_matrix::{crate_dir, Matrix};

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

const FILE_CALLS: [&str; 6] = ["file", "read", "url", "attach", "detach", "set_local"];
const UPLOAD_CALLS: [&str; 3] = ["upload", "supply_source", "fail_source"];
const RELATION_CALLS: [&str; 4] = ["node", "nodes", "refresh", "edge_values"];

fn exported_calls() -> Vec<String> {
    fn files(directory: &Path, found: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(directory) else { return };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "selftest" || name == "tests") { continue; }
                files(&path, found);
            } else if path.extension().is_some_and(|extension| extension == "rs") && !path.to_string_lossy().ends_with("_tests.rs") {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    files(&crate_dir().join("src/ffi"), &mut found);
    let mut names = Vec::new();
    for file in found {
        let Ok(text) = std::fs::read_to_string(&file) else { continue };
        let mut from = 0;
        while let Some(at) = text[from..].find("#[uniffi::export") {
            let start = from + at;
            from = start + 10;
            let rest = &text[start..];
            let Some(line_end) = rest.find('\n') else { break };
            let after = rest[line_end..].trim_start();
            if !after.starts_with("impl Database {") { continue; }
            let open = start + line_end + rest[line_end..].find('{').unwrap_or(0);
            let (mut depth, mut end) = (0usize, open);
            for (offset, character) in text[open..].char_indices() {
                match character {
                    '{' => depth += 1,
                    '}' => { depth -= 1; if depth == 0 { end = open + offset; break; } }
                    _ => {}
                }
            }
            for line in text[open..end].lines() {
                let line = line.strip_prefix("    pub async fn ").or_else(|| line.strip_prefix("    pub fn "));
                if let Some(name) = line.and_then(|line| line.split('(').next()) { names.push(name.to_string()); }
            }
        }
    }
    names.sort();
    names.dedup();
    names
}

/// Calls that carry no model and no row: they have no admission steps 3 to 5.
const WITHOUT_ROW: [&str; 26] = [
    "retry_progress",
    "open_report", "admit", "subscription_census", "register_operations", "set_online", "online_sink", "close_database", "session_epoch",
    "interrupted_session_answer", "session_reset", "session_call", "set_round_jobs", "round_started", "round_refused", "round_ended", "outcome",
    "call", "cable_closes", "subscribe_values", "read_query", "begin_load", "begin_load_all", "load_more", "send", "upload_create_batch",
];

async fn invoke(name: &str, database: &Arc<Database>, args: &Args) -> Option<Result<(), DbError>> {
    let (epoch, model, row) = (args.epoch, args.model.clone(), args.row.clone());
    let field = args.field.clone();
    Some(match name {
        "peek_row" => database.peek_row(epoch, model, row).map(|_| ()),
        "read_row" => database.read_row(epoch, model, row).await.map(|_| ()),
        "file" => database.file(epoch, model, row, field).await.map(|_| ()),
        "read" => database.read(epoch, model, row, field).await.map(|_| ()),
        "url" => database.url(epoch, model, row, field).await.map(|_| ()),
        "retry" => database.retry(epoch, model, row, None, None).await.map(|_| ()),
        "discard" => database.discard(epoch, model, row, None).await,
        "destroy" => database.destroy(epoch, model, row).await.map(|_| ()),
        "update" => database.update(epoch, model, row, "{\"text\":\"x\"}".into(), "{}".into()).await.map(|_| ()),
        "act" => database.act(epoch, model, row, "edit".into(), "{}".into(), "{}".into()).await.map(|_| ()),
        "set_local" => database.set_local(epoch, model, row, args.local_path.clone(), args.local_value.clone()).await,
        "set_ephemeral" => database.set_ephemeral(epoch, model, row, args.values.clone()).await,
        "attach" => database.attach(epoch, model, row, field, FileBytes { bytes: vec![1], name: "a".into(), mime: "image/png".into() }).await.map(|_| ()),
        "detach" => database.detach(epoch, model, row, field).await.map(|_| ()),
        "create" => database.create(epoch, model, args.values.clone(), "{}".into()).await.map(|_| ()),
        "cancel_upload" => database.cancel_upload(epoch, model, row, "upload".into()).await.map(|_| ()),
        "node" => database.node(epoch, model, "node".into(), row, false).await.map(|_| ()),
        "nodes" => database.nodes(epoch, model, "nodes".into(), vec![row]).await.map(|_| ()),
        "refresh" => database.refresh(epoch, model, Some("profile".into()), "{}".into()).await,
        "edge_values" => database.edge_values(epoch, model, Some("profile".into()), "{}".into(), vec![row]).await.map(|_| ()),
        "upload" => database.upload(epoch, model, row, args.action.clone(), WireUploadRequest { source: crate::ffi::upload_wire::WireSource::File { path: args.source_path.clone(), name: None, mime: None }, variables_json: "{}".into() }).await.map(|_| ()),
        "supply_source" => database.supply_source(epoch, model, row, args.action.clone(), WireUploadSource { path: args.source_path.clone(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into() }).await.map(|_| ()),
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
const MISSING_ROW_ADMITTED: [&str; 11] = ["peek_row", "read_row", "file", "read", "url", "edge_values", "node", "nodes", "refresh", "set_local", "set_ephemeral"];

fn expected(closed: bool, stale: bool, invalid: bool, missing: bool) -> &'static str {
    if closed { "Closed" } else if stale { "NoSession" } else if invalid { "Invalid" } else if missing { "NotFound" } else { "admitted" }
}

async fn admission_group(group: usize) {
    let name = ["E-call-admission-0", "E-call-admission-1", "E-call-admission-2", "E-call-admission-3"][group];
    let mut matrix = Matrix::new(name);
    let all = exported_calls();
    let calls: Vec<String> = all.iter().enumerate().filter(|(index, _)| index % 4 == group).map(|(_, call)| call.clone()).collect();
    if group == 0 && all.len() < 30 { matrix.unclassified("Database", "-", "-", &format!("the source scan found {} exported calls", all.len())); }
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
    let (files, files_epoch, files_row) = files_database(&directory.path().join("open.sqlite")).await;
    let (closed_files, closed_files_epoch, closed_files_row) = files_database(&directory.path().join("closed.sqlite")).await;
    let _ = closed_files.close_database().await;
    let _ = bound(rig.database.outcome(receipt)).await;
    rig.world.fail_next("SendMessage", Fault::Http(422));
    let retry_receipt = bound(rig.database.send(message(epoch, "retryrow", 2))).await.unwrap();
    let retry_row = retry_receipt.optimistic_value.as_ref().unwrap().id.clone();
    let _ = bound(rig.database.outcome(retry_receipt)).await;
    rig.world.fail_next("SendMessage", Fault::Http(422));
    let discard_receipt = bound(rig.database.send(message(epoch, "discardrow", 3))).await.unwrap();
    let discard_row = discard_receipt.optimistic_value.as_ref().unwrap().id.clone();
    let _ = bound(rig.database.outcome(discard_receipt)).await;
    let cancel_receipt = bound(rig.database.send(message(epoch, "cancelrow", 4))).await.unwrap();
    let cancel_row = cancel_receipt.optimistic_value.as_ref().unwrap().id.clone();
    let _ = bound(rig.database.outcome(cancel_receipt)).await;
    let cancel_source = directory.path().join("cancel.bin");
    std::fs::write(&cancel_source, b"clip").unwrap();
    let _blob_gate = rig.world.hold_next("CreateBlob");
    let _ = bound(rig.database.upload(epoch, "Message".into(), cancel_row.clone(), "upload".into(), WireUploadRequest { source: WireSource::File { path: cancel_source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() })).await;
    let target = |name: &str, closed: bool| -> (Arc<Database>, Args) {
        let (database, epoch, row) = match (FILE_CALLS.contains(&name), closed) {
            (true, false) => (files.clone(), files_epoch, files_row.clone()),
            (true, true) => (closed_files.clone(), closed_files_epoch, closed_files_row.clone()),
            (false, false) => (rig.database.clone(), epoch, present.clone()),
            (false, true) => (closed_rig.database.clone(), closed_epoch, closed_present.clone()),
        };
        let mut args = Args { epoch, model: if FILE_CALLS.contains(&name) { "Files" } else { "Message" }.into(), row, field: "photo".into(), local_path: "draft".into(), local_value: "{\"text\":\"x\"}".into(), values: "{\"chatId\":\"c1\",\"text\":\"t\",\"at\":1}".into(), action: "upload".into(), source_path: String::new() };
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
    for name in &calls {
        if WITHOUT_ROW.contains(&name.as_str()) || UPLOAD_CALLS.contains(&name.as_str()) { continue; }
        for mask in 0u8..8 {
            if name == "create" && mask & 4 != 0 { continue; }
            let (stale, invalid, missing) = (mask & 1 != 0, mask & 2 != 0, mask & 4 != 0 && name != "create");
            let (database, mut args) = target(name, false);
            if stale { args.epoch += 1000; }
            if invalid { args.model = "NoSuchModel".into(); }
            if missing { args.row = "absent-row".into(); }
            let input = format!("stale={stale} invalid_model={invalid} missing_row={missing}");
            match invoke_bounded(name, &database, &args).await {
                Some(result) => matrix.check(name, "open", &input, expected(false, stale, invalid, missing && !MISSING_ROW_ADMITTED.contains(&name.as_str())), observed(&result)),
                None => matrix.unclassified(name, "open", &input, "the matrix has no way to call this export"),
            }
        }
        let (database, args) = target(name, true);
        match invoke_bounded(name, &database, &args).await {
            Some(result) => matrix.check(name, "closed", "valid arguments", "Closed", observed(&result)),
            None => matrix.unclassified(name, "closed", "valid arguments", "the matrix has no way to call this export"),
        }
    }
    matrix.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_call_admission_0() { admission_group(0).await; }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_call_admission_1() { admission_group(1).await; }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_call_admission_2() { admission_group(2).await; }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_call_admission_3() { admission_group(3).await; }

/// The upload calls run on the stand of uploads: a process keeps one open database, so the stand is closed for the last row.
#[tokio::test(flavor = "multi_thread")]
async fn matrix_call_admission_uploads() {
    let mut matrix = Matrix::new("E-call-admission-uploads");
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
            matrix.check(name, "open", &input, expected(false, stale, invalid, missing), observed(&result));
        }
    }
    let _ = database.close_database().await;
    for name in UPLOAD_CALLS {
        let result = invoke_bounded(name, &database, &args(epoch)).await.expect("an upload call");
        matrix.check(name, "closed", "valid arguments", "Closed", observed(&result));
    }
    matrix.finish();
}

/// E isolation: an upload on a missing row answers `NotFound` and leaves the database open for the next call.
#[tokio::test(flavor = "multi_thread")]
async fn matrix_upload_missing_row_leaves_the_database_open() {
    let mut matrix = Matrix::new("E-upload-missing-row");
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    database.set_online(false).unwrap();
    let row = bound(database.send(plain("Moment", r#"{"text":"intro"}"#, epoch))).await.unwrap().optimistic_value.unwrap().id;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"video").unwrap();
    let request = || WireUploadRequest { source: crate::ffi::upload_wire::WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() };
    let first = database.upload(epoch, "Moment".into(), "absent-row".into(), "upload_clip".into(), request()).await;
    matrix.check("upload(missing row)", "first call", "missing_row", "NotFound", observed(&first.map(|_| ())));
    let next = database.peek_row(epoch, "Moment".into(), row.clone());
    matrix.check("peek_row(present row)", "next call on the same database", "after the refused upload", "admitted", observed(&next.map(|_| ())));
    let again = database.upload(epoch + 1000, "Moment".into(), "absent-row".into(), "upload_clip".into(), request()).await;
    matrix.check("upload(stale epoch, missing row)", "second upload", "after the refused upload", "NoSession", observed(&again.map(|_| ())));
    let valid = database.upload(epoch, "Moment".into(), row.clone(), "upload_clip".into(), request()).await;
    matrix.check("upload(valid row)", "first call", "valid arguments", "admitted", observed(&valid.map(|_| ())));
    let _ = bound(crate::ffi::settled(&database)).await;
    let after = database.peek_row(epoch, "Moment".into(), row);
    matrix.check("peek_row(present row)", "next call after a valid upload", "the server has no scripted answer", "admitted", observed(&after.map(|_| ())));
    matrix.finish();
}
