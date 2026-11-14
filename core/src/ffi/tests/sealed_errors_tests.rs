use std::sync::Arc;

use super::encrypted_tests::{SubscriptionsOnly, ThroughServer};
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use super::{Config, Database, DbError, EncryptedConfig, SessionCode};
use crate::schema::{ActionDeclaration, Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec, RootShape};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
use crate::value::ScalarKind;

const TABLE: [(&str, SessionCode); 11] = [
    ("SEQUENCE_CONFLICT", SessionCode::SequenceConflict),
    ("REPLAYED_REQUEST", SessionCode::ReplayedRequest),
    ("RATE_LIMITED", SessionCode::RateLimited),
    ("ACCOUNT_UNAVAILABLE", SessionCode::AccountUnavailable),
    ("UNAUTHENTICATED", SessionCode::Unauthenticated),
    ("SESSION_ENDED", SessionCode::SessionEnded),
    ("INVALID_CODE", SessionCode::InvalidCode),
    ("INVALID_PHONE", SessionCode::InvalidPhone),
    ("INVALID_PROVIDER", SessionCode::InvalidProvider),
    ("PROVIDER_REJECTED", SessionCode::ProviderRejected),
    ("ALREADY_AUTHENTICATED", SessionCode::AlreadyAuthenticated),
];

pub(in super::super) const OPERATIONS: &str = r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"},{"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text } }","rootPath":"makeMoment"}]"#;

pub(in super::super) fn moment_spec() -> ModelSpec {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.upload_steps = Some(crate::owners::write::upload::UploadOperations::test_steps());
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(),
        empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("MakeMoment".to_string());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.on_failure = crate::schema::OnFailure::Keep;
    model.actions.push(create);
    let mut upload = crate::schema_fixtures::upload_declared("upload");
    upload.mutation = Some("MakeMoment".to_string());
    model.actions.push(upload);
    model
}

pub(in super::super) async fn signed_in(rig: &Rig) -> Arc<Database> {
    let database = Database::open(Config { path: None }, crate::ffi::specs(serde_json::to_string(&vec![moment_spec()]).unwrap()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await
    .unwrap();
    database.register_operations(OPERATIONS.into()).await.unwrap();
    database.set_online(true).await.unwrap();
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.register("query Feed { moments { id text } }");
    rig.server.register("mutation MakeMoment { makeMoment { id text } }");
    database
}

pub(in super::super) fn session(error: DbError) -> (SessionCode, String) {
    let DbError::Session { code, detail } = error else { panic!("expected a session error, got {error:?}") };
    (code, detail)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_code_in_a_sealed_answer_reaches_the_bridge_typed_for_every_entry_point_and_keeps_the_credential() {
    let mut cases = Vec::new();
    for (wire, code) in TABLE {
        for confirmed in [false, true] {
            cases.push(tokio::spawn(async move {
                for entry in ["load", "refresh", "run query", "run mutation"] {
                    if wire == "REPLAYED_REQUEST" && entry != "run mutation" {
                        continue;
                    }
                    let rig = Rig::new();
                    let database = signed_in(&rig).await;
                    let credential = rig.credentials.inner.value();
                    rig.server.refuse_next(wire, confirmed);
                    let error = match entry {
                        "load" => database.load(database.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap_err(),
                        "refresh" => database.refresh(database.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).await.unwrap_err(),
                        "run query" => database.run(database.session_epoch(), "Feed".into(), "{}".into()).await.unwrap_err(),
                        _ => database.run(database.session_epoch(), "MakeMoment".into(), "{}".into()).await.unwrap_err(),
                    };
                    let case = format!("{wire} {entry} confirmed={confirmed}");
                    if code == SessionCode::Unauthenticated {
                        assert!(matches!(error, DbError::Unauthorized { .. }), "{case}: {error:?}");
                    } else if code == SessionCode::SessionEnded {
                        let expected_end = matches!(error, DbError::NoSession { .. });
                        assert!(expected_end, "{case}: a revoked answer ends the epoch of the author and every wait of it ends with NoSession (D-278, D-279, bridge.md:65): {error:?}");
                        let life = database.life_handle();
                        crate::test_wait::within(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
                        assert_eq!(rig.credentials.inner.value(), None, "{case}: the published server end erases the session");
                        continue;
                    } else {
                        let (got, detail) = session(error);
                        let refusal = serde_json::from_str::<serde_json::Value>(&detail).ok().and_then(|body| body["error"].as_str().map(str::to_string));
                        assert_eq!((got, refusal.as_deref()), (code, Some(wire)), "{case}: the error carries the refusal body unchanged, got {detail:?}");
                    }
                    assert_eq!((rig.credentials.inner.value(), rig.touched().1), (credential.clone(), 0), "{case}");
                }
            }));
        }
    }
    for case in cases {
        case.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_code_on_a_queued_mutation_reaches_settled_typed_and_keeps_the_credential() {
    for (wire, code) in TABLE {
        for confirmed in [false, true] {
            let rig = Rig::new();
            let database = signed_in(&rig).await;
            let credential = rig.credentials.inner.value();
            rig.server.refuse_next(wire, confirmed);
            let receipt = database.send_create("Moment", r#"{"text":"hello"}"#).await;

            let outcome = database.confirmed(receipt, false).await;
            let case = format!("{wire} confirmed={confirmed}");
            if code == SessionCode::SessionEnded {
                assert!(matches!(outcome, Err(super::DbError::NoSession { .. })), "{case}: a revoked answer ends the operation with the session, got {outcome:?}");
                let life = database.life_handle();
                crate::test_wait::within(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
                assert_eq!(rig.credentials.inner.value(), None, "{case}: the published server end erases the session");
                continue;
            }
            let Err(super::DbError::Rejected { failure, kept: true }) = outcome else { panic!("expected rejection: {outcome:?}") };

            let refusal = serde_json::from_str::<serde_json::Value>(&failure.message).ok().and_then(|body| body["error"].as_str().map(str::to_string));
            assert_eq!((failure.session_code, refusal.as_deref()), (Some(code), Some(wire)), "{case}: the failure carries the refusal body unchanged, got {:?}", failure.message);
            assert_eq!((rig.credentials.inner.value(), rig.touched().1), (credential, 0), "{case}");
        }
    }
}

/// D-455, R16: a mutation the Outbox accepted answers the caller with its receipt even when its own revoked answer ends the session before the caller reads the answer; the end shows in the outcome, not in `send`.
#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_accepted_before_the_session_ends_returns_its_receipt() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    rig.server.refuse_next("SESSION_ENDED", true);
    let gate = Arc::new((crate::test_wait::Flag::default(), crate::test_wait::Flag::default()));
    *database.db.answer_gate.lock().unwrap() = Some(gate.clone());
    let epoch = database.session_epoch();
    let sending = { let database = database.clone(); tokio::spawn(async move { database.send(crate::ffi::CommandRequest { epoch, model: "Moment".into(), values_json: r#"{"text":"hello"}"#.into(), args_json: "{}".into(), upload: None }).await }) };
    let entered = gate.clone();
    crate::test_wait::within(tokio::task::spawn_blocking(move || entered.0.wait_blocking("the send reached the answer of the owner"))).await.unwrap();
    let life = database.life_handle();
    crate::test_wait::within(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
    assert_eq!(rig.credentials.inner.value(), None, "the revoked answer of the mutation ended the session before the caller read its answer");
    gate.1.set();
    let receipt = crate::test_wait::within(sending).await.unwrap();
    let receipt = receipt.unwrap_or_else(|error| panic!("the accepted mutation answers with its receipt, got {error:?}"));
    assert!(matches!(database.confirmed(receipt, false).await, Err(DbError::NoSession { .. })), "the end of the session shows in the outcome");
}

pub(in super::super) fn graphql_posts(rig: &Rig) -> usize {
    rig.server.seen().iter().filter(|request| request["path"] == "/api/graphql").count()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unauthenticated_or_unavailable_refusal_keeps_the_queued_mutation_failed_and_retry_sends_it_once() {
    for (wire, code) in [("UNAUTHENTICATED", SessionCode::Unauthenticated), ("ACCOUNT_UNAVAILABLE", SessionCode::AccountUnavailable)] {
        let rig = Rig::new();
        let database = signed_in(&rig).await;
        rig.server.refuse_next(wire, false);
        let receipt = database.send_create("Moment", r#"{"text":"hello"}"#).await;
        let row = receipt.optimistic_value.clone().unwrap();

        let outcome = database.confirmed(receipt, false).await;
        let Err(super::DbError::Rejected { failure, kept: true }) = outcome else { panic!("expected rejection: {outcome:?}") };

        assert_eq!(failure.session_code, Some(code), "{wire}");
        let stored = database.db.find("Moment", &crate::identity::RowId::new(&row.id).unwrap()).unwrap();
        assert_eq!(stored.and_then(|stored| stored.text("text").map(str::to_string)), Some("hello".to_string()), "{wire}");
        let states: Vec<_> = crate::ffi::tests::queue_view_tests::queued(&database).iter().map(|operation| operation.status).collect();
        assert_eq!(states, [crate::owners::write::operation_status::OperationStatus::Failed], "{wire}");
        let before = graphql_posts(&rig);

        let retried = database.retry(database.session_epoch(), "Moment".into(), row.id.clone(), None, None).await.unwrap();
        let _ = database.confirmed(retried, false).await;

        assert_eq!(graphql_posts(&rig) - before, 1, "{wire}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn any_other_session_refusal_follows_the_same_keep_declaration() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    rig.server.refuse_next("RATE_LIMITED", true);
    let receipt = database.send_create("Moment", r#"{"text":"hello"}"#).await;
    let row = receipt.optimistic_value.clone().unwrap();

    assert!(matches!(database.confirmed(receipt, false).await, Err(super::DbError::Rejected { kept: true, .. })));

    assert!(database.db.find("Moment", &crate::identity::RowId::new(&row.id).unwrap()).unwrap().is_some());
}
