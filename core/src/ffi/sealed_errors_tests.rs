use std::sync::Arc;

use super::encrypted_tests::{SubscriptionsOnly, ThroughServer};
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use super::{Config, Database, DbError, EncryptedConfig, SessionCode, Transport};
use crate::schema::{ActionDeclaration, Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec, RootShape};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
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

pub(super) const OPERATIONS: &str = r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"},{"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text } }","rootPath":"makeMoment"}]"#;

pub(super) fn moment_spec() -> ModelSpec {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
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
    model.actions.push(create);
    model
}

pub(super) async fn signed_in(rig: &Rig) -> Arc<Database> {
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec()]).unwrap(), Some(Arc::new(SubscriptionsOnly)), Some(EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() }), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))))
    .unwrap();
    database.register_operations(OPERATIONS.into()).unwrap();
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.register("query Feed { moments { id text } }");
    rig.server.register("mutation MakeMoment { makeMoment { id text } }");
    database
}

pub(super) fn session(error: DbError) -> (SessionCode, String) {
    let DbError::Session { code, detail } = error else { panic!("expected a session error, got {error:?}") };
    (code, detail)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_code_in_a_sealed_answer_reaches_the_bridge_typed_for_every_entry_point_and_keeps_the_credential() {
    for (wire, code) in TABLE {
        for entry in ["load", "refresh", "run query", "run mutation"] {
            for confirmed in [false, true] {
                if wire == "REPLAYED_REQUEST" && entry != "run mutation" {
                    continue;
                }
                let rig = Rig::new();
                let database = signed_in(&rig).await;
                let credential = rig.credentials.inner.value();
                rig.server.refuse_next(wire, confirmed);

                let error = match entry {
                    "load" => database.load("Moment".into(), "feed".into(), "{}".into()).await.unwrap_err(),
                    "refresh" => database.refresh("Moment".into(), "feed".into(), "{}".into()).await.unwrap_err(),
                    "run query" => database.run("Feed".into(), "{}".into()).await.unwrap_err(),
                    _ => database.run("MakeMoment".into(), "{}".into()).await.unwrap_err(),
                };

                let case = format!("{wire} {entry} confirmed={confirmed}");
                assert_eq!(session(error), (code, wire.to_string()), "{case}");
                assert_eq!((rig.credentials.inner.value(), rig.touched().1), (credential, 0), "{case}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_code_on_a_queued_mutation_reaches_settled_typed_and_keeps_the_credential() {
    for (wire, code) in TABLE {
        for confirmed in [false, true] {
            let rig = Rig::new();
            let database = signed_in(&rig).await;
            let credential = rig.credentials.inner.value();
            let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
            rig.server.refuse_next(wire, confirmed);

            let error = database.settled("Moment".into(), row.id.clone()).await.unwrap_err();

            let case = format!("{wire} confirmed={confirmed}");
            assert_eq!(session(error), (code, wire.to_string()), "{case}");
            assert_eq!((rig.credentials.inner.value(), rig.touched().1), (credential, 0), "{case}");
        }
    }
}

struct Refusing(SessionCode);

#[async_trait::async_trait]
impl Transport for Refusing {
    async fn open_cable(&self) -> Result<String, DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> { Err(DbError::Session { code: self.0, detail: "wire".into() }) }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { Err(DbError::Session { code: self.0, detail: "wire".into() }) }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<String, DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_error_thrown_by_a_platform_transport_keeps_its_code_through_load_run_and_settled() {
    for (_, code) in TABLE {
        let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec()]).unwrap(), Some(Arc::new(Refusing(code))), None, None, None).unwrap();
        database.register_operations(OPERATIONS.into()).unwrap();

        let load = database.load("Moment".into(), "feed".into(), "{}".into()).await.unwrap_err();
        let run = database.run("MakeMoment".into(), "{}".into()).await.unwrap_err();
        let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
        let settled = database.settled("Moment".into(), row.id).await.unwrap_err();

        for error in [load, run, settled] {
            assert_eq!(session(error).0, code, "{code:?}");
        }
    }
}

pub(super) fn graphql_posts(rig: &Rig) -> usize {
    rig.server.seen().iter().filter(|request| request["path"] == "/api/graphql").count()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unauthenticated_or_unavailable_refusal_keeps_the_queued_mutation_failed_and_retry_sends_it_once() {
    for (wire, code) in [("UNAUTHENTICATED", SessionCode::Unauthenticated), ("ACCOUNT_UNAVAILABLE", SessionCode::AccountUnavailable)] {
        let rig = Rig::new();
        let database = signed_in(&rig).await;
        let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
        rig.server.refuse_next(wire, false);

        let error = database.settled("Moment".into(), row.id.clone()).await.unwrap_err();

        assert_eq!(session(error).0, code, "{wire}");
        let stored = database.db.find("Moment", &crate::identity::RowId::new(&row.id).unwrap()).unwrap();
        assert_eq!(stored.and_then(|stored| stored.text("text").map(str::to_string)), Some("hello".to_string()), "{wire}");
        let states: Vec<_> = database.db.outbox().unwrap().iter().map(|entry| entry.operation.status).collect();
        assert_eq!(states, [crate::commit::OperationStatus::Failed], "{wire}");
        database.session_resume().await.unwrap();
        let before = graphql_posts(&rig);

        database.retry("Moment".into(), row.id.clone()).await.unwrap();
        let _ = database.settled("Moment".into(), row.id.clone()).await;

        assert_eq!(graphql_posts(&rig) - before, 1, "{wire}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn any_other_session_refusal_rolls_the_queued_mutation_back() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
    rig.server.refuse_next("RATE_LIMITED", true);

    database.settled("Moment".into(), row.id.clone()).await.unwrap_err();

    assert!(database.db.find("Moment", &crate::identity::RowId::new(&row.id).unwrap()).unwrap().is_none());
}
