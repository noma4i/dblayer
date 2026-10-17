use std::sync::Arc;

use super::sealed_errors_tests::{moment_spec, session, OPERATIONS};
use super::{Config, Database, DbError, SessionCode, Transport};

const THROWN: [(SessionCode, &str); 4] = [
    (SessionCode::Unauthenticated, "token expired"),
    (SessionCode::AccountUnavailable, "blocked until Monday"),
    (SessionCode::Other, "I_AM_A_TEAPOT"),
    (SessionCode::RateLimited, "slow down"),
];

struct Refusing(SessionCode, String);

#[async_trait::async_trait]
impl Transport for Refusing {
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> { Err(DbError::Session { code: self.0, detail: self.1.clone() }) }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { Err(DbError::Session { code: self.0, detail: self.1.clone() }) }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<String, DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_error_thrown_by_a_platform_transport_keeps_code_and_detail_through_run_and_settled() {
    for (code, detail) in THROWN {
        let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec()]).unwrap(), Some(Arc::new(Refusing(code, detail.to_string())))).unwrap();
        database.register_operations(OPERATIONS.into()).unwrap();

        let run = database.run("MakeMoment".into(), "{}".into()).await.unwrap_err();
        let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
        let settled = database.settled("Moment".into(), row.id).await.unwrap_err();

        let expected = (code, detail.to_string());
        assert_eq!(session(run), expected, "run {code:?}");
        assert_eq!(session(settled), expected, "settled {code:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_queued_mutation_keeps_the_session_code_and_detail_in_its_stored_failure() {
    for (code, detail) in [(SessionCode::Unauthenticated, "token expired"), (SessionCode::AccountUnavailable, "blocked until Monday")] {
        let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec()]).unwrap(), Some(Arc::new(Refusing(code, detail.to_string())))).unwrap();
        database.register_operations(OPERATIONS.into()).unwrap();
        let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
        database.settled("Moment".into(), row.id).await.unwrap_err();

        let stored: Vec<_> = database.db.outbox().unwrap().iter().map(|entry| entry.operation.error.clone()).collect();

        assert_eq!(stored, [Some(crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Session(code), detail))], "{code:?}");
    }
}
