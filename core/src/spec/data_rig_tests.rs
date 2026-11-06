//! Cells of `data.md` through the public API of `Database` on the rig with a mock server.

use super::rig::{bounded, within, Rig};
use crate::ffi::{DbError, WireOperationOutcome};
use crate::schema::OnFailure;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_scope_enum_default() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("defaults.sqlite").to_str().unwrap().to_string();
    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&super::rig::moment_models(OnFailure::Rollback)).unwrap();
    let mut relation = crate::schema::RelationSpec::new("feed", &[]);
    relation.text_params.push("kind".into());
    relation.text_types.insert("kind".into(), "FeedKind".into());
    relation.text_defaults.insert("kind".into(), "PRIMARY".into());
    relation.remote = Some(crate::schema::RemoteSpec {
        operation: "Feed".into(), shape: crate::schema::RemoteShape::List { coverage: crate::schema::Coverage::Complete },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: false,
        page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    models[0].relations.push(relation);
    let rig = Rig::open_models(serde_json::to_string(&models).unwrap(), Some(path), super::rig::Server::new()).await;
    rig.db.register_operations(r#"[{"name":"Feed","document":"query Feed($kind: FeedKind!) { moments(kind: $kind) { id text } }","rootPath":"moments"}]"#.into()).unwrap();
    for params in ["{}", r#"{"kind":"PRIMARY"}"#, "{}"] {
        let request = rig.db.begin_load(rig.epoch(), "Moment".into(), "feed".into(), params.into()).unwrap();
        crate::test_wait::within(request.result()).await.unwrap();
        let query = json!({"model":"Moment","relation":"feed","params":serde_json::from_str::<serde_json::Value>(params).unwrap()});
        assert!(crate::test_wait::within(rig.db.read_query(rig.epoch(), query.to_string())).await.unwrap().is_empty());
    }
    let requests = rig.server.seen();
    let requests: Vec<_> = requests.iter().filter(|request| request.operation == "Feed").collect();
    assert_eq!(requests.len(), 1, "omitted and explicit defaults share a fresh window");
    assert_eq!(requests[0].variables, json!({"kind":"PRIMARY"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_operation_parameter_not_stored() {
    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&super::rig::moment_models(OnFailure::Rollback)).unwrap();
    let create = models[0].actions.iter_mut().find(|action| action.name == "create").unwrap();
    create.variables.insert("clientFlag".into(), "clientFlag".into());
    let server = super::rig::Server::new();
    let rig = Rig::open_models(serde_json::to_string(&models).unwrap(), None, server.clone()).await;
    server.hold(super::rig::Kind::Mutate, true);
    let receipt = within(rig.db.send(crate::ffi::CommandRequest {
        epoch: rig.epoch(), model: "Moment".into(), values_json: json!({"text":"hello"}).to_string(),
        args_json: json!({"clientFlag":"chosen"}).to_string(), upload: None,
    })).await.unwrap();
    let optimistic = receipt.optimistic_value.as_ref().unwrap();
    assert_eq!(optimistic.cells, vec![crate::ffi::Cell::Text { value: "hello".into() }]);
    crate::test_wait::yield_until("mutation is held", || server.held(super::rig::Kind::Mutate) == 1).await;
    assert_eq!(server.seen().last().unwrap().variables["clientFlag"], json!("chosen"));
    server.answer(super::rig::Kind::Mutate, super::rig::Reply::Ok);
    assert!(matches!(rig.outcome(&receipt).await, WireOperationOutcome::Confirmed { .. }));
    assert_eq!(rig.row("srv1").await.unwrap().cells, vec![crate::ffi::Cell::Text { value: "hello".into() }]);
}

#[test]
fn inv_op_19_create_with_an_existing_id_is_an_error_and_writes_nothing() { bounded("inv_op_19", inv_op_19_run) }

async fn inv_op_19_run() {
    let rig = Rig::open(OnFailure::Rollback).await;
    let created = rig.create("hello").await;
    assert!(matches!(rig.outcome(&created).await, WireOperationOutcome::Confirmed { .. }));
    let (revision, mutations) = (rig.row("srv1").await.unwrap().revision, rig.server.mutations());

    let again = within(rig.db.send(crate::ffi::CommandRequest { epoch: rig.epoch(), model: "Moment".into(), values_json: json!({"id": "srv1", "text": "other"}).to_string(), args_json: "{}".into(), upload: None })).await;

    assert!(matches!(&again, Err(DbError::AlreadyExists { model, id }) if model == "Moment" && id == "srv1"), "{again:?}");
    rig.quiet().await;
    assert_eq!((rig.row("srv1").await.unwrap().revision, rig.server.mutations()), (revision, mutations), "nothing is written and nothing is sent");
    assert_eq!(rig.row("srv1").await.unwrap().id, "srv1");
}

#[test]
fn inv_row_05_update_with_unchanged_values_writes_nothing() { bounded("inv_row_05", inv_row_05_run) }

async fn inv_row_05_run() {
    let rig = Rig::open(OnFailure::Rollback).await;
    let created = rig.create("hello").await;
    assert!(matches!(rig.outcome(&created).await, WireOperationOutcome::Confirmed { .. }));
    let (revision, mutations) = (rig.row("srv1").await.unwrap().revision, rig.server.mutations());

    let receipt = rig.update("srv1", "hello").await;

    rig.quiet().await;
    assert!(receipt.is_ok(), "{receipt:?}");
    assert_eq!((rig.row("srv1").await.unwrap().revision, rig.server.mutations()), (revision, mutations), "no commit, no operation, no request");
    let changed = rig.update("srv1", "changed").await.unwrap();
    assert!(matches!(rig.outcome(&changed).await, WireOperationOutcome::Confirmed { .. }));
    assert!(rig.row("srv1").await.unwrap().revision > revision, "a real change commits");
}
